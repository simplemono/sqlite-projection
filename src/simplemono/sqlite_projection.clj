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
  (:import (java.nio.file FileAlreadyExistsException Files)
           (java.sql DriverManager)))

(def ^:private state-table-statement
  {:create-table [:event_projection_last_event_number :if-not-exists]
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

(defn- projection-version
  [opts]
  (let [version (require-key opts :projection/version
                             "Missing :projection/version")]
    (when-not (and (integer? version) (not (neg? version)))
      (throw (ex-info ":projection/version must be a non-negative integer"
                      {:projection/version version})))
    version))

(defn- connectable
  [opts]
  (require-key opts :db/ds "Missing :db/ds"))

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

(defn- ensure-projection-schemas!
  "Execute every registered :projection/create HoneySQL statement."
  [connectable definitions]
  (doseq [[idx definition] (map-indexed vector definitions)]
    (execute-statements! connectable
                         ((:projection/create definition))
                         {:projection/index idx
                          :projection/action :create})))

(defn- ensure-state-table!
  "Create the library-owned derived projection cursor table if needed."
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

(defn- stored-projection-version
  "Read SQLite PRAGMA user_version."
  [connectable]
  (:user_version
   (jdbc/execute-one! connectable ["PRAGMA user_version"]
                      {:builder-fn rs/as-unqualified-maps})))

(defn- stamp-projection-version!
  [connectable version]
  (jdbc/execute! connectable [(str "PRAGMA user_version = " (long version))]))

(defn- projection-version-mismatch!
  [expected actual]
  (throw (ex-info "SQLite projection version mismatch; rebuild the projection DB"
                  {:error :projection-version-mismatch
                   :projection/expected-version expected
                   :projection/actual-version actual})))

(defn- state-table-exists?
  [connectable]
  (some? (jdbc/execute-one! connectable
                            [(str "select name from sqlite_master"
                                  " where type = 'table'"
                                  " and name = 'event_projection_last_event_number'")])))

(defn- unstamped-projection!
  [cursor]
  (throw (ex-info "SQLite projection DB has a cursor but no version stamp; rebuild the projection DB"
                  {:error :projection-unstamped
                   :event-number cursor})))

(defn- ensure-compatible-version!
  "A `user_version` of 0 means a fresh file, but only while nothing has been
   projected. The stamp and the cursor are written in the same transaction, so
   a cursor next to a zero stamp cannot come out of this library — that file
   was copied or corrupted, and adopting it would project new events onto
   state of an unknown version. The state table alone proves nothing: it is
   created before the first transaction, so a crash can leave it behind empty,
   and that file has provably projected nothing."
  [connectable expected]
  (let [actual (stored-projection-version connectable)]
    (if (zero? actual)
      (when (state-table-exists? connectable)
        (when-some [cursor (last-projected-event-number connectable)]
          (unstamped-projection! cursor)))
      (when (not= expected actual)
        (projection-version-mismatch! expected actual)))
    actual))

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
   stream, not something to skip silently."
  [connectable lookup event-number event]
  (doseq [handler (get lookup (event-type event event-number))]
    (execute-statements! connectable
                         ((:projection/fn handler) event)
                         {:projection/event-type (:projection/event-type handler)
                          :event-number event-number})))

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
  "Apply event-store events after the SQLite projection cursor.

  Reads events from the cursor until the first missing one. Events with no
  registered handler are ignored. The cursor advances only after every event in
  this catch-up run has been applied successfully in one SQLite transaction."
  [opts]
  (let [ds (connectable opts)
        store (event-store opts)
        version (projection-version opts)
        register (register opts)
        definitions (projection-definitions register)
        lookup (projection-lookup register)]
    (ensure-compatible-version! ds version)
    (ensure-state-table! ds)
    (ensure-projection-schemas! ds definitions)
    (jdbc/with-transaction [tx ds]
      (let [previous-last (last-projected-event-number tx)
            from (if previous-last (inc (long previous-last)) 0)
            last-event-number (or (apply-events! tx store lookup from)
                                  previous-last)]
        (stamp-projection-version! tx version)
        (write-last-projected-event-number! tx last-event-number)))
    nil))

(defn- build-fresh!
  [ds store version register]
  (let [definitions (projection-definitions register)
        lookup (projection-lookup register)]
    (jdbc/with-transaction [tx ds]
      (ensure-state-table! tx)
      (ensure-projection-schemas! tx definitions)
      (let [last-event-number (apply-events! tx store lookup 0)]
        (stamp-projection-version! tx version)
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
  "Build a caller-supplied SQLite DB file path from the event store.

  Requires :db/path. The DB is built in :db/tmp-dir, or java.io.tmpdir when
  omitted, then compacted and copied to a sibling staging file beside :db/path.
  Only after a successful build is :db/path atomically hard-linked to the
  staging file, then staging is removed. The destination filesystem must support
  hard links. An existing destination is never replaced: throws
  {:error :db-already-exists} and discards the completed build. The caller owns
  other failed temp-build cleanup, active DB switching, and old-version cleanup."
  [opts]
  (let [path (str (require-key opts :db/path "Missing :db/path"))
        store (event-store opts)
        version (projection-version opts)
        register (register opts)
        final-file (io/file path)
        final-parent (parent-file final-file)]
    (Files/createDirectories (.toPath final-parent)
                             (make-array java.nio.file.attribute.FileAttribute 0))
    (let [build-dir (create-build-dir! opts)
          build-file (.resolve build-dir "projection.db")
          stage-file (staging-file final-file)
          published? (try
                       (with-open [conn (DriverManager/getConnection
                                         (str "jdbc:sqlite:" build-file))]
                         (build-fresh! conn store version register)
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

(defn db-file
  "The versioned DB file for `opts`: `v{version}.db` under `:db/dir`.

   The version is in the file name so that bumping :projection/version points
   the code at a file that does not exist yet instead of invalidating one in
   place. The old version's file stays untouched and servable while the new
   one builds, and no pointer has to be kept current: the running code knows
   its own version, so the path is derivable."
  ^java.io.File [opts]
  (io/file (str (require-key opts :db/dir "Missing :db/dir"))
           (str "v" (projection-version opts) ".db")))

(defn ensure-db-file!
  "Build the versioned DB file for `opts` unless it already exists. This is
   the startup call: run it before serving, then open a datasource on the
   returned file and `catch-up!` as usual.

   An existing file is trusted to be a completed build of its version, because
   `build-db-file!` only publishes finished builds atomically — a final
   name can never hold a half-built file. `catch-up!`'s version check remains
   behind that as the safety net.

   Concurrent callers for the same stream, register and version need no
   coordination: the first completed build published wins. Later builders
   discard their builds and return the existing file, never replacing a DB
   that may already be open or have caught up further.

   Returns the DB file."
  ^java.io.File [opts]
  (let [file (db-file opts)]
    (when-not (.exists file)
      (try
        (build-db-file! (assoc opts :db/path (str file)))
        (catch clojure.lang.ExceptionInfo e
          (when-not (= :db-already-exists (:error (ex-data e)))
            (throw e)))))
    file))

(defn- version-below?
  [re version name]
  (when-some [[_ v] (re-matches re name)]
    (< (parse-long v) (long version))))

(defn delete-old-db-files!
  "Delete DB files of versions below `:projection/version - 1` under :db/dir,
   together with their SQLite sidecar files and leftover staging files.

   The current version's neighbours on both sides survive, for the same
   reason. Newer versions are kept because during a rollback this process
   must not destroy the file the version being rolled forward to has already
   built. The immediate predecessor is kept because in a blue/green rollout
   on a shared volume the old code may still be serving it — deleting a
   SQLite file out from under a process that opens a connection per query
   breaks it on its next one — and it doubles as the rollback target. That
   is what makes this safe to call at startup, right after `ensure-db-file!`:
   the new version being built says nothing about the old one being done.
   Everything further out is garbage and goes, staging leftovers included.

   Returns the deleted files."
  [opts]
  (let [dir (io/file (str (require-key opts :db/dir "Missing :db/dir")))
        version (dec (long (projection-version opts)))
        old? (fn [^java.io.File f]
               (let [name (.getName f)]
                 (or (version-below? #"v(\d+)\.db(?:-wal|-shm|-journal)?"
                                     version name)
                     (version-below? #"\.v(\d+)\.db\.staging-.*"
                                     version name))))
        old-files (vec (filter old? (.listFiles dir)))]
    (doseq [f old-files]
      (io/delete-file f true))
    old-files))

(comment

  (require '[simplemono.event-store :as event-store]
           '[simplemono.event-store.memory :as memory])

  (def store (memory/store))

  (event-store/try-append! store 0 {:event/type :todo/created
                                    :todo/id "1"
                                    :todo/text "Ship it"})

  (defn create-todos
    []
    [{:create-table [:todos :if-not-exists]
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

  (def ds (jdbc/get-datasource "jdbc:sqlite:todos-v1.db"))

  (catch-up! {:event-store store
              :db/ds ds
              :projection/version 1
              :projection/register register})

  (jdbc/execute! ds ["select * from todos"])

  )
