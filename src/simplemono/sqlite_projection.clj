(ns simplemono.sqlite-projection
  "Project batches from a source function into SQLite read models.

   :source receives the last committed cursor (nil initially) and returns
   reducible {:cursor non-negative-Long :events [...]} batches strictly after
   it. Cursors increase but need not be consecutive. The source owns reading,
   ordering and upcasting; projection handlers still receive individual events.

   All batches and their final cursor commit in one SQLite transaction. This
   library does not write source events, run commands or manage tenants. SQLite
   is derived state: build a new UUID-named DB when projection semantics change."
  (:require [clojure.java.io :as io]
            [honey.sql :as sql]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import (java.nio.file FileAlreadyExistsException Files)))

(def ^:private state-table-statement
  {:create-table :projection_cursor
   :with-columns [[:cursor :integer [:primary-key]]]})

(def ^:private missing-value
  (Object.))

(defn- require-key
  [m k message]
  (let [value (get m k missing-value)]
    (if (or (identical? missing-value value)
            (nil? value))
      (throw (ex-info message {:required-key k}))
      value)))

(defn db-file
  "The projection DB file: `{uuid}.db` under :db/dir, as a java.io.File.

   :projection/version must be a UUID committed with the projection definition.
   The filename is the sole version identity; no version is stored in SQLite.
   Use a separate directory per event stream and projection family. Derives the
   path without touching the filesystem. Arbitrary :db/ds and :db/path options
   are no longer supported."
  ^java.io.File [opts]
  (doseq [[k replacement] [[:db/ds ":db/dir and :projection/version"]
                           [:db/path ":db/dir and :projection/version"]
                           [:event-store ":source"]]]
    (when (contains? opts k)
      (throw (ex-info (str k " is no longer supported; use " replacement)
                      {:error :unsupported-option :option k}))))
  (let [dir (require-key opts :db/dir "Missing :db/dir")
        version (require-key opts :projection/version "Missing :projection/version")]
    (when-not (uuid? version)
      (throw (ex-info ":projection/version must be a UUID"
                      {:projection/version version})))
    (io/file (str dir) (str version ".db"))))

(defn- callback?
  [x]
  (or (fn? x) (and (var? x) (fn? @x))))

(defn- source
  "Validate the callback without invoking it or touching storage."
  [opts]
  (let [source (require-key opts :source "Missing :source")]
    (when-not (callback? source)
      (throw (ex-info ":source must be a function or a Var containing one"
                      {:error :invalid-source})))
    source))

(defn- register
  "Validate every entry before any schema function or SQLite operation runs."
  [opts]
  (let [entries (require-key opts :projection/register
                             "Missing :projection/register")]
    (when-not (sequential? entries)
      (throw (ex-info ":projection/register must be a sequence of maps"
                      {:error :invalid-projection-register
                       :projection/value entries})))
    (doseq [[idx entry] (map-indexed vector entries)]
      (let [context {:error :invalid-projection-register
                     :projection/index idx}]
        (when-not (map? entry)
          (throw (ex-info "Projection register entry must be a map"
                          (assoc context :projection/value entry))))
        (let [schema? (contains? entry :projection/create)
              handler? (or (contains? entry :projection/event-type)
                           (contains? entry :projection/fn))]
          (when-not (or schema? handler?)
            (throw (ex-info "Projection register entry must define a schema or event handler"
                            (assoc context :projection/value entry))))
          (when (and handler? (not (:projection/event-type entry)))
            (throw (ex-info "Projection handler requires :projection/event-type"
                            (assoc context :projection/key :projection/event-type))))
          (doseq [k (cond-> []
                      schema? (conj :projection/create)
                      handler? (conj :projection/fn))]
            (when-not (callback? (get entry k))
              (throw (ex-info (str "Projection " k " value must be a function")
                              (assoc context :projection/key k
                                             :projection/value (get entry k)))))))))
    (vec entries)))

(defn- projection-definitions
  "Return validated schema definitions in register order."
  [register]
  (filterv #(contains? % :projection/create) register))

(defn- projection-lookup
  "Return {event-type [handler-entry ...]} from register in register order."
  [register]
  (->> register
       (filter #(contains? % :projection/fn))
       (reduce (fn [lookup entry]
                 (update lookup (:projection/event-type entry) (fnil conj []) entry))
               {})))

(defn- normalize-statements
  [statements context]
  (cond
    (nil? statements)
    []

    (map? statements)
    [statements]

    (sequential? statements)
    (vec (remove nil? statements))

    :else
    (throw (ex-info "Expected HoneySQL map or sequence of HoneySQL maps"
                    (assoc context :value statements)))))

(defn- execute-honeysql!
  [connectable statement context]
  (when-not (map? statement)
    (throw (ex-info "Projection statements must be HoneySQL maps"
                    (assoc context :statement statement))))
  (jdbc/execute! connectable (sql/format statement)))

(defn- execute-statements!
  [connectable statements context]
  (doseq [statement (normalize-statements statements context)]
    (execute-honeysql! connectable statement context)))

(defn- create-projection-schemas!
  "Execute every registered :projection/create HoneySQL statement."
  [connectable definitions]
  (doseq [[idx definition] (map-indexed vector definitions)]
    (execute-statements! connectable
                         ((:projection/create definition))
                         {:projection/index idx
                          :projection/action :create})))

(defn- create-state-table!
  "Create the library-owned derived projection cursor table in a fresh build."
  [connectable]
  (execute-honeysql! connectable
                     state-table-statement
                     {:projection/action :create-state-table}))

(defn- last-projected-cursor
  "Return the last fully projected source cursor, or nil before any batch."
  [connectable]
  (:cursor
   (jdbc/execute-one! connectable
                      (sql/format {:select [:cursor]
                                   :from [:projection_cursor]
                                   :limit 1})
                      {:builder-fn rs/as-unqualified-maps})))

(defn- write-last-projected-cursor!
  [connectable cursor]
  (jdbc/execute! connectable
                 (sql/format {:delete-from :projection_cursor}))
  (when (some? cursor)
    (jdbc/execute! connectable
                   (sql/format {:insert-into :projection_cursor
                                :values [{:cursor cursor}]}))))

(defn- event-type
  [event context]
  (or (:event/type event)
      (throw (ex-info "Event is missing :event/type"
                      (assoc context :error :missing-event-type)))))

(defn- apply-event!
  "Apply every handler registered for this event's :event/type. An event whose
   type has no handler is ignored; an event with no type at all is a bug in the
   stream, not something to skip silently. Handler and statement exceptions
   carry event context, with the original exception preserved as their cause."
  [connectable lookup cursor event-index event]
  (let [context {:cursor cursor :event-index event-index}
        type (event-type event context)
        context (assoc context :projection/event-type type)]
    (doseq [handler (get lookup type)]
      (try
        (execute-statements! connectable ((:projection/fn handler) event) context)
        (catch Exception e
          (throw (ex-info (str "Failed to project event " event-index " at cursor " cursor
                              " (" (pr-str type) ")")
                          (assoc context :error :projection-failed)
                          e)))))))

(defn- check-batch!
  [batch after]
  (when-not (map? batch)
    (throw (ex-info "Source must return batch maps" {:error :invalid-batch})))
  (let [{:keys [cursor events]} batch]
    (when-not (and (instance? Long cursor) (not (neg? cursor))
                   (or (nil? after) (> cursor after)))
      (throw (ex-info "Batch cursor must be a non-negative Long strictly after the previous cursor"
                      {:error :invalid-batch :batch/key :cursor :after-cursor after})))
    (when-not (vector? events)
      (throw (ex-info "Batch :events must be a vector (empty is allowed)"
                      {:error :invalid-batch :batch/key :events :cursor cursor})))))

(defn- apply-batches!
  "Consume the source once. Return its final cursor, or `after` when idle.
   The caller's SQLite transaction owns all effects, including partial replay
   failures. No cursor arithmetic or event-store protocols are involved."
  [connectable source lookup after]
  (let [batches (source after)]
    (when-not (or (instance? clojure.lang.IReduceInit batches) (sequential? batches))
      (throw (ex-info "Source must return reducible batches or a sequential collection, not nil"
                      {:error :invalid-source-result})))
    (reduce (fn [previous batch]
              (check-batch! batch previous)
              (let [{:keys [cursor events]} batch]
                (doseq [[index event] (map-indexed vector events)]
                  (apply-event! connectable lookup cursor index event))
                cursor))
            after
            batches)))

(defn catch-up!
  "Apply events after the cursor in the existing UUID-named DB.

  Requires explicit initialization with ensure-db-file! first. A missing file
  throws {:error :db-not-found}; catch-up never creates a file or runs schema
  functions. The filename is trusted to identify the projection definition.
  Calls :source with the last committed cursor (nil initially). All returned
  batches and the final cursor share one connection and transaction, so any
  failure rolls back the entire run. Idle runs perform no SQLite writes;
  batches with no events or no applicable handlers still advance the cursor.
  A cursor at Long/MAX_VALUE is terminal and needs no further source reads.
  Returns nil."
  [opts]
  (let [file (db-file opts)
        source (source opts)
        lookup (projection-lookup (register opts))]
    (when-not (.exists file)
      (throw (ex-info "Projection DB does not exist; call ensure-db-file! before catch-up!"
                      {:error :db-not-found :db/path (str file)})))
    ;; mode=rw forbids creation even if the file disappears after the check.
    ;; A file URI also escapes directory names containing ?, #, or %.
    (with-open [conn (jdbc/get-connection
                     (str "jdbc:sqlite:" (.toASCIIString (.toURI file)) "?mode=rw"))]
      (jdbc/with-transaction [tx conn]
        (let [previous (last-projected-cursor tx)]
          (when-not (= Long/MAX_VALUE previous)
            (let [cursor (apply-batches! tx source lookup previous)]
              (when (not= previous cursor)
                (write-last-projected-cursor! tx cursor)))))))
    nil))

(defn- build-fresh!
  [ds source register]
  (let [definitions (projection-definitions register)
        lookup (projection-lookup register)]
    (jdbc/with-transaction [tx ds]
      (create-state-table! tx)
      (create-projection-schemas! tx definitions)
      (let [cursor (apply-batches! tx source lookup nil)]
        (write-last-projected-cursor! tx cursor)))))

(defn- tmp-base-dir
  [opts]
  (io/file (or (:db/tmp-dir opts)
               (System/getProperty "java.io.tmpdir"))))

(defn- create-build-dir!
  [opts]
  (let [base (tmp-base-dir opts)]
    (Files/createDirectories (.toPath base)
                             (make-array java.nio.file.attribute.FileAttribute 0))
    (Files/createTempDirectory (.toPath base)
                               "sqlite-projection-"
                               (make-array java.nio.file.attribute.FileAttribute 0))))

(defn- parent-file
  [file]
  (or (.getParentFile file)
      (io/file ".")))

(defn- staging-file
  [final-file]
  (io/file (parent-file final-file)
           (str "." (.getName final-file) ".staging-" (random-uuid))))

(defn- delete-tree!
  [file]
  (when (.exists file)
    (doseq [f (reverse (file-seq file))]
      (io/delete-file f true))))

(defn- finalize-sqlite-build!
  [connectable]
  (jdbc/execute! connectable ["PRAGMA wal_checkpoint(TRUNCATE)"])
  (jdbc/execute! connectable ["PRAGMA journal_mode=DELETE"])
  (jdbc/execute! connectable ["VACUUM"]))

(defn build-db-file!
  "Build the UUID-named DB under :db/dir from :source.

  Schema creation, replay and cursor writing share one transaction in a fresh
  temporary DB. The DB is built in :db/tmp-dir, or java.io.tmpdir when omitted,
  then compacted and copied to a sibling staging file beside (db-file opts).
  Only after a successful build is the final filename atomically hard-linked
  to staging, then staging is removed. The destination filesystem must support
  hard links. An existing destination is never replaced: throws
  {:error :db-already-exists} and discards the completed build. The caller owns
  other failed temp-build cleanup, active DB switching, and old-version cleanup."
  [opts]
  (let [final-file (db-file opts)
        path (str final-file)
        source (source opts)
        register (register opts)
        final-parent (parent-file final-file)]
    (Files/createDirectories (.toPath final-parent)
                             (make-array java.nio.file.attribute.FileAttribute 0))
    (let [build-dir (create-build-dir! opts)
          build-file (.resolve build-dir "projection.db")
          stage-file (staging-file final-file)
          published? (try
                       (with-open [conn (jdbc/get-connection
                                        (str "jdbc:sqlite:" (.toASCIIString (.toUri build-file))))]
                         (build-fresh! conn source register)
                         (finalize-sqlite-build! conn))
                       (Files/copy build-file
                                   (.toPath stage-file)
                                   (make-array java.nio.file.CopyOption 0))
                       ;; ATOMIC_MOVE may replace an existing target. A hard
                       ;; link publishes the completed inode without clobbering
                       ;; a database another caller may already be using.
                       (let [published? (try
                                          (Files/createLink (.toPath final-file)
                                                            (.toPath stage-file))
                                          true
                                          (catch FileAlreadyExistsException _
                                            false))]
                         (io/delete-file stage-file true)
                         (delete-tree! (.toFile build-dir))
                         published?)
                       (catch Throwable t
                         (io/delete-file stage-file true)
                         (throw (ex-info "Failed to build SQLite DB file"
                                         {:error :db-build-failed
                                          :db/path path
                                          :db/tmp-dir (str build-dir)}
                                         t))))]
      (when-not published?
        (throw (ex-info "SQLite DB destination already exists"
                        {:error :db-already-exists
                         :db/path path})))
      nil)))

(defn ensure-db-file!
  "Build the versioned DB file for `opts` unless it already exists. This is
   the startup call: run it before serving, then open a datasource on the
   returned file and `catch-up!` as usual.

   An existing filename is trusted to identify a completed build of that
   projection definition. There is no internal version stamp or schema repair:
   callers must not rename other databases into the UUID namespace. Only
   completed builds are published; schema functions run only during building.

   Concurrent callers for the same stream, register and version need no
   coordination: the first completed build published wins. Later builders
   discard their builds and return the existing file, never replacing a DB
   that may already be open or have caught up further.

   Returns the DB file."
  ^java.io.File [opts]
  (let [file (db-file opts)]
    (when-not (.exists file)
      (try
        (build-db-file! opts)
        (catch clojure.lang.ExceptionInfo e
          (when-not (= :db-already-exists (:error (ex-data e)))
            (throw e)))))
    file))

(comment

  (def batches
    [{:cursor 1000
      :events [{:event/type :todo/created :todo/id "1" :todo/text "Ship it"}]}])

  (defn read-batches [after]
    (eduction (filter #(or (nil? after) (> (:cursor %) after))) batches))

  (defn create-todos
    []
    [{:create-table :todos
      :with-columns [[:id :text [:primary-key]]
                     [:text :text [:not nil]]
                     [:completed :integer [:not nil] [:default 0]]]}])

  (defn todo-created
    [event]
    [{:insert-into :todos
      :values [{:id (:todo/id event)
                :text (:todo/text event)
                :completed 0}]}])

  (def todo-register
    [{:projection/create #'create-todos}
     {:projection/event-type :todo/created
      :projection/fn #'todo-created}])

  (def opts {:source read-batches
             :db/dir "data/todos"
             :projection/version #uuid "4bfa586d-3429-4af7-b38b-5e85f03d611d"
             :projection/register todo-register})

  (def file (ensure-db-file! opts))
  (def ds (jdbc/get-datasource (str "jdbc:sqlite:" file)))

  (catch-up! opts)

  (jdbc/execute! ds ["select * from todos"])

  )
