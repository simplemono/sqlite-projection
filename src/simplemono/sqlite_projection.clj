(ns simplemono.sqlite-projection
  "Project events from a `simplemono.event-store/EventStore` into SQLite read
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
  (:import (java.nio.file Files StandardCopyOption)
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
  [opts]
  (require-key opts :event-store "Missing :event-store"))

(defn- register
  [opts]
  (vec (require-key opts :projection/register
                    "Missing :projection/register")))

(defn- projection-definition?
  "Return true when register entry defines a projection schema data function."
  [entry]
  (contains? entry :projection/create))

(defn- projection-schema-fn?
  [x]
  (or (fn? x) (var? x)))

(defn- projection-handler?
  "Return true when register entry defines an event projection handler."
  [entry]
  (and (:projection/event-type entry)
       (:projection/fn entry)))

(defn- projection-definitions
  "Return projection schema definitions from register in register order.

  Each definition must contain :projection/create. Create functions are
  zero-arity functions that return HoneySQL maps or seqs of HoneySQL maps."
  [register]
  (let [definitions (->> register (filter projection-definition?) vec)]
    (doseq [[idx definition] (map-indexed vector definitions)]
      (when-not (projection-schema-fn? (:projection/create definition))
        (throw (ex-info "Projection :projection/create value must be a function"
                        {:projection/index idx
                         :projection/value (:projection/create definition)}))))
    definitions))

(defn- projection-lookup
  "Return {event-type [handler-entry ...]} from register in register order."
  [register]
  (->> register
       (filter projection-handler?)
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

(defn- call-schema-fn
  [definition key idx]
  (let [f (get definition key)]
    (when-not (projection-schema-fn? f)
      (throw (ex-info "Projection schema value must be a function"
                      {:projection/index idx
                       :projection/key key
                       :projection/value f})))
    (f)))

(defn- ensure-projection-schemas!
  "Execute every registered :projection/create HoneySQL statement."
  [connectable definitions]
  (doseq [[idx definition] (map-indexed vector definitions)]
    (execute-statements! connectable
                         (call-schema-fn definition :projection/create idx)
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

(defn- ensure-compatible-version!
  [connectable expected]
  (let [actual (stored-projection-version connectable)]
    (when (and (not (zero? actual))
               (not= expected actual))
      (projection-version-mismatch! expected actual))
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

(defn- apply-events-until-missing!
  "Apply events from `from` upwards until `get-event` returns nil. Returns the
   last applied event number, or nil when the first one was already missing.

   Reading until the first miss is what keeps `latest-event-number` out of the
   catch-up path: on an object store that is a LIST, which is a Class A
   operation, while `get-event` is a Class B GET."
  [connectable store lookup from]
  (loop [event-number (long from)
         last-event-number nil]
    (if-let [event (event-store/get-event store event-number)]
      (do
        (apply-event! connectable lookup event-number event)
        (recur (inc event-number) event-number))
      last-event-number)))

(defn catch-up!
  "Apply event-store events after the SQLite projection cursor.

  Reads sequential get-event results until the first missing event. Events with
  no registered handler are ignored. The cursor advances only after every event
  in this catch-up run has been applied successfully in one SQLite transaction."
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
            last-event-number (or (apply-events-until-missing! tx store lookup from)
                                  previous-last)]
        (stamp-projection-version! tx version)
        (write-last-projected-event-number! tx last-event-number)))
    nil))

(defn- build-fresh!
  [opts ds]
  (let [store (event-store opts)
        version (projection-version opts)
        register (register opts)
        definitions (projection-definitions register)
        lookup (projection-lookup register)]
    (jdbc/with-transaction [tx ds]
      (ensure-state-table! tx)
      (ensure-projection-schemas! tx definitions)
      (let [last-event-number (apply-events-until-missing! tx store lookup 0)]
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
  Only after a successful build does the staging file move atomically to
  :db/path. The caller owns failed temp-build cleanup, active DB switching, and
  old-version cleanup."
  [opts]
  (let [path (str (require-key opts :db/path "Missing :db/path"))
        final-file (io/file path)
        final-parent (parent-file final-file)]
    (Files/createDirectories (.toPath final-parent)
                             (make-array java.nio.file.attribute.FileAttribute 0))
    (let [build-dir (create-build-dir! opts)
          build-file (.resolve build-dir "projection.db")
          stage-file (staging-file final-file)]
      (try
        (with-open [conn (DriverManager/getConnection
                          (str "jdbc:sqlite:" build-file))]
          (build-fresh! opts conn)
          (finalize-sqlite-build! conn))
        (Files/copy build-file
                    (.toPath stage-file)
                    (make-array java.nio.file.CopyOption 0))
        (Files/move (.toPath stage-file)
                    (.toPath final-file)
                    (into-array java.nio.file.CopyOption
                                [StandardCopyOption/ATOMIC_MOVE]))
        (delete-tree! (.toFile build-dir))
        nil
        (catch Throwable t
          (io/delete-file stage-file true)
          (throw (ex-info "Failed to build SQLite DB file"
                          {:error :db-build-failed
                           :db/path path
                           :db/tmp-dir (str build-dir)}
                          t)))))))

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
