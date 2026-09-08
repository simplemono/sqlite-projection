(ns simplemono.sqlite-projection
  "Project events from a `simplemono.event-store/EventSource` into SQLite read
   models.

   It does one thing: read events that were already appended to an event store
   and apply registered HoneySQL projections to SQLite.

   It does not write events, run commands, enrich events or manage tenants. The
   event stream is essential state and lives in the event store; SQLite tables
   are derived state and are disposable. When a projection changes, build a new
   DB file from the stream rather than mutating the old one."
  (:require [clojure.java.io :as io]
            [honey.sql :as sql]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [simplemono.event-store :as event-store])
  (:import (java.nio.file FileAlreadyExistsException Files)))

(def ^:private state-table-statement
  {:create-table :event_projection_last_event_number
   :with-columns [[:event_number :integer [:primary-key]]]})

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
  (doseq [k [:db/ds :db/path]]
    (when (contains? opts k)
      (throw (ex-info (str k " is no longer supported; use :db/dir and :projection/version")
                      {:error :unsupported-option :option k}))))
  (let [dir (require-key opts :db/dir "Missing :db/dir")
        version (require-key opts :projection/version "Missing :projection/version")]
    (when-not (uuid? version)
      (throw (ex-info ":projection/version must be a UUID"
                      {:projection/version version})))
    (io/file (str dir) (str version ".db"))))

(defn- event-store
  "The store to read from. Only `simplemono.event-store/EventSource` is used,
   so a projection can be handed something it cannot append to."
  [opts]
  (require-key opts :event-store "Missing :event-store"))

(defn- projection-function?
  [x]
  (or (fn? x) (and (var? x) (fn? @x))))

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
            (when-not (projection-function? (get entry k))
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

(defn- last-projected-event-number
  "Return the last fully projected event number, or nil when none was projected."
  [connectable]
  (:event_number
   (jdbc/execute-one! connectable
                      (sql/format {:select [:event_number]
                                   :from [:event_projection_last_event_number]
                                   :limit 1})
                      {:builder-fn rs/as-unqualified-maps})))

(defn- write-last-projected-event-number!
  [connectable last-event-number]
  (jdbc/execute! connectable
                 (sql/format {:delete-from :event_projection_last_event_number}))
  (when (some? last-event-number)
    (jdbc/execute! connectable
                   (sql/format {:insert-into :event_projection_last_event_number
                                :values [{:event_number last-event-number}]}))))

(defn- event-type
  [event event-number]
  (or (:event/type event)
      (throw (ex-info "Event is missing :event/type"
                      {:error :missing-event-type
                       :event-number event-number
                       :event event}))))

(defn- apply-event!
  "Apply every handler registered for this event's :event/type. An event whose
   type has no handler is ignored; an event with no type at all is a bug in the
   stream, not something to skip silently. Handler and statement exceptions
   carry event context, with the original exception preserved as their cause."
  [connectable lookup event-number event]
  (let [type (event-type event event-number)]
    (doseq [handler (get lookup type)]
      (let [context {:projection/event-type type :event-number event-number}]
        (try
          (execute-statements! connectable ((:projection/fn handler) event) context)
          (catch Exception e
            (throw (ex-info (str "Failed to project event " event-number " (" (pr-str type) ")")
                            (assoc context :error :projection-failed)
                            e))))))))

(defn- apply-events!
  "Apply events from `from` upwards until the first one that does not exist.
   Returns the last applied event number, or nil when the first one was already
   missing.

   How to read them is the store's business: it is the only thing that knows
   what a request costs. `events` returns something `reduce` walks, and what it
   does inside is up to the store — on an object store that is one request per
   batch rather than per event, and an idle catch-up is a single request, which
   matters because that is most of them.

   The accumulator is the last applied event number, so the next one is always
   its successor: the stream is gap-free and the replay starts at `from`.
   Starting one below `from` means \"nothing applied yet\" needs no separate
   flag."
  [connectable store lookup from]
  (let [from (long from)
        applied (reduce (fn [last-event-number event]
                          (let [event-number (inc (long last-event-number))]
                            (apply-event! connectable lookup event-number event)
                            event-number))
                        (dec from)
                        (event-store/events store from))]
    (when (>= (long applied) from)
      applied)))

(defn catch-up!
  "Apply events after the cursor in the existing UUID-named DB.

  Requires explicit initialization with ensure-db-file! first. A missing file
  throws {:error :db-not-found}; catch-up never creates a file or runs schema
  functions. The filename is trusted to identify the projection definition.
  Reads until the first missing event, ignoring unhandled event types. Event
  projection and cursor updates share one connection and transaction, so any
  failure rolls back the entire run. Idle runs perform no SQLite writes;
  consumed events still advance the cursor even if their handlers do no work.
  Returns nil."
  [opts]
  (let [file (db-file opts)
        store (event-store opts)
        lookup (projection-lookup (register opts))]
    (when-not (.exists file)
      (throw (ex-info "Projection DB does not exist; call ensure-db-file! before catch-up!"
                      {:error :db-not-found :db/path (str file)})))
    ;; mode=rw forbids creation even if the file disappears after the check.
    ;; A file URI also escapes directory names containing ?, #, or %.
    (with-open [conn (jdbc/get-connection
                     (str "jdbc:sqlite:" (.toASCIIString (.toURI file)) "?mode=rw"))]
      (jdbc/with-transaction [tx conn]
        (let [previous-last (last-projected-event-number tx)
              from (if previous-last (inc (long previous-last)) 0)]
          (when-some [last-event-number (apply-events! tx store lookup from)]
            (write-last-projected-event-number! tx last-event-number)))))
    nil))

(defn- build-fresh!
  [ds store register]
  (let [definitions (projection-definitions register)
        lookup (projection-lookup register)]
    (jdbc/with-transaction [tx ds]
      (create-state-table! tx)
      (create-projection-schemas! tx definitions)
      (let [last-event-number (apply-events! tx store lookup 0)]
        (write-last-projected-event-number! tx last-event-number)))))

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
  "Build the UUID-named DB under :db/dir from the event store.

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
        store (event-store opts)
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
                         (build-fresh! conn store register)
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

  (require '[simplemono.event-store :as event-store]
           '[simplemono.event-store.memory :as memory])

  (def store (memory/store))

  (event-store/try-append! store 0 {:event/type :todo/created
                                    :todo/id "1"
                                    :todo/text "Ship it"})

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

  (def register
    [{:projection/create #'create-todos}
     {:projection/event-type :todo/created
      :projection/fn #'todo-created}])

  (def opts {:event-store store
             :db/dir "data/todos"
             :projection/version #uuid "4bfa586d-3429-4af7-b38b-5e85f03d611d"
             :projection/register register})

  (def file (ensure-db-file! opts))
  (def ds (jdbc/get-datasource (str "jdbc:sqlite:" file)))

  (catch-up! opts)

  (jdbc/execute! ds ["select * from todos"])

  )
