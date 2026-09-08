(ns simplemono.sqlite-projection-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing run-tests]]
            [honey.sql :as sql]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [simplemono.event-store :as event-store]
            [simplemono.event-store.memory :as memory]
            [simplemono.event-store.util :as util]
            [simplemono.sqlite-projection :as projection])
  (:import (java.nio.file Files)))

(def version-a #uuid "4bfa586d-3429-4af7-b38b-5e85f03d611d")
(def version-b #uuid "bf7eadc7-b8bb-45ad-b862-c0195e5dcb83")

(defn create-todos []
  ;; Deliberately not IF NOT EXISTS: schemas only run in fresh builds.
  [{:create-table :todos
    :with-columns [[:id :text [:primary-key]]
                   [:text :text [:not nil]]
                   [:completed :integer [:not nil] [:default 0]]]}])

(defn create-broken []
  (throw (ex-info "broken projection schema" {})))

(defn todo-created [event]
  [{:insert-into :todos
    :values [{:id (str (:todo/id event))
              :text (:todo/text event)
              :completed 0}]}])

(defn todo-completed [event]
  [{:update :todos
    :set {:completed 1}
    :where [:= :id (str (:todo/id event))]}])

(def register
  [{:projection/create #'create-todos}
   {:projection/event-type :todo/created :projection/fn #'todo-created}
   {:projection/event-type :todo/completed :projection/fn #'todo-completed}])

(defn temp-dir [prefix]
  (Files/createTempDirectory prefix (make-array java.nio.file.attribute.FileAttribute 0)))

(defn temp-opts []
  (let [root (temp-dir "sqlite-projection-test")]
    {:event-store (memory/store)
     :db/dir (str (.resolve root "db"))
     :db/tmp-dir (str (.resolve root "tmp"))
     :projection/version version-a
     :projection/register register}))

(defn file-ds [file]
  (jdbc/get-datasource (str "jdbc:sqlite:" (.toASCIIString (.toURI (io/file file))))))

(defn query-ds [opts]
  (file-ds (projection/db-file opts)))

(defn directory-names [dir]
  (set (.list (io/file dir))))

(defn query-one [ds statement]
  (jdbc/execute-one! ds (sql/format statement) {:builder-fn rs/as-unqualified-maps}))

(defn last-projected-event-number [ds]
  (:event_number
   (query-one ds {:select [:event_number]
                  :from [:event_projection_last_event_number]
                  :limit 1})))

(defn table-names [ds]
  (set (map :name (jdbc/execute! ds ["select name from sqlite_master where type = 'table'"]
                                {:builder-fn rs/as-unqualified-maps}))))

(defn append! [store n event]
  (is (true? (event-store/try-append! store n event))))

(defn seed-todos! [store n]
  (doseq [i (range n)]
    (append! store i {:event/type :todo/created :todo/id i :todo/text (str "todo " i)})))

(defn todo-row [ds id]
  (query-one ds {:select [:*] :from [:todos] :where [:= :id (str id)]}))

(defn todo-count [ds]
  (:count (query-one ds {:select [[[:count :*] :count]] :from [:todos]})))

(defn counting-store [store reads]
  (reify event-store/EventSource
    (events [_ from]
      (swap! reads conj from)
      (event-store/events store from))))

(deftest db-file-is-a-pure-uuid-based-path
  (let [opts (temp-opts)
        file (projection/db-file opts)]
    (is (= (str version-a ".db") (.getName file)))
    (is (= (io/file (:db/dir opts)) (.getParentFile file)))
    (is (= file (projection/db-file (assoc opts :db/dir (io/file (:db/dir opts))))))
    (is (not (.exists (io/file (:db/dir opts)))))))

(deftest invalid-versions-are-rejected-before-any-work
  (doseq [version [0 1 -1 Integer/MAX_VALUE Long/MAX_VALUE 1.5 true "invalid" (str version-a)]]
    (testing (pr-str version)
      (let [calls (atom [])
            opts (assoc (temp-opts)
                        :projection/version version
                        :event-store (reify event-store/EventSource
                                       (events [_ _] (swap! calls conj :events) []))
                        :projection/register [{:projection/create #(swap! calls conj :schema)}])]
        (doseq [operation [projection/db-file projection/ensure-db-file!
                           projection/build-db-file! projection/catch-up!]]
          (try
            (operation opts)
            (is false "expected invalid version")
            (catch clojure.lang.ExceptionInfo e
              (is (re-find #"must be a UUID" (.getMessage e)))
              (is (= version (:projection/version (ex-data e))))))
          (is (not (.exists (io/file (:db/dir opts)))))
          (is (not (.exists (io/file (:db/tmp-dir opts)))))
          (is (empty? @calls)))))))

(deftest arbitrary-datasource-and-path-options-are-rejected
  (doseq [k [:db/ds :db/path]
          operation [projection/db-file projection/ensure-db-file!
                     projection/build-db-file! projection/catch-up!]]
    (let [opts (assoc (temp-opts) k "must not be used")]
      (try
        (operation opts)
        (is false "expected unsupported option")
        (catch clojure.lang.ExceptionInfo e
          (is (= :unsupported-option (:error (ex-data e))))
          (is (= k (:option (ex-data e))))))
      (is (not (.exists (io/file (:db/dir opts)))))
      (is (not (.exists (io/file (:db/tmp-dir opts))))))))

(deftest required-options-cannot-be-missing-or-nil
  (doseq [operation [projection/ensure-db-file! projection/build-db-file! projection/catch-up!]
          k [:db/dir :projection/version :event-store :projection/register]
          remove-key? [true false]]
    (let [opts (temp-opts)]
      (try
        (operation (if remove-key? (dissoc opts k) (assoc opts k nil)))
        (is false "expected missing option")
        (catch clojure.lang.ExceptionInfo e
          (is (= k (:required-key (ex-data e))))))
      (is (not (.exists (io/file (:db/dir opts))))))))

(deftest catch-up-requires-explicit-initialization
  (let [calls (atom [])
        opts (assoc (temp-opts)
                    :event-store (reify event-store/EventSource
                                   (events [_ _] (swap! calls conj :events) []))
                    :projection/register [{:projection/create #(swap! calls conj :schema)}])]
    (try
      (projection/catch-up! opts)
      (is false "expected missing database")
      (catch clojure.lang.ExceptionInfo e
        (is (= :db-not-found (:error (ex-data e))))
        (is (= (str (projection/db-file opts)) (:db/path (ex-data e))))
        (is (re-find #"ensure-db-file!" (.getMessage e)))))
    (is (empty? @calls))
    (is (not (.exists (io/file (:db/dir opts)))))
    (is (not (.exists (io/file (:db/tmp-dir opts)))))))

(deftest catch-up-cannot-create-a-file-that-disappears-before-open
  (let [reads (atom [])
        opts (temp-opts)
        opts (assoc opts :event-store (counting-store (:event-store opts) reads))
        file (projection/ensure-db-file! opts)
        open jdbc/get-connection]
    (reset! reads [])
    ;; Delete after the existence check but before SQLite opens the file.
    (with-redefs [jdbc/get-connection (fn [& args]
                                       (io/delete-file file)
                                       (apply open args))]
      (is (thrown? java.sql.SQLException (projection/catch-up! opts))))
    (is (not (.exists file)) "SQLite mode=rw must not recreate an empty final file")
    (is (empty? @reads))))

(deftest schema-functions-run-only-during-building
  (let [calls (atom 0)
        opts (assoc (temp-opts) :projection/register
                    [{:projection/create #(do (swap! calls inc)
                                              (concat (create-todos)
                                                      (todo-created {:todo/id "seed" :todo/text "Seed"})))}
                     {:projection/event-type :todo/created :projection/fn #'todo-created}])
        store (:event-store opts)
        file (projection/ensure-db-file! opts)
        ds (query-ds opts)]
    (is (= 1 @calls))
    (is (= 1 (todo-count ds)))
    (is (nil? (last-projected-event-number ds)))
    (is (= file (projection/ensure-db-file! opts)))
    (dotimes [_ 2] (is (nil? (projection/catch-up! opts))))
    (append! store 0 {:event/type :todo/created :todo/id "event" :todo/text "Event"})
    (is (nil? (projection/catch-up! opts)))
    (is (= 1 @calls) "neither DDL nor non-idempotent seed inserts run again")
    (is (= 2 (todo-count ds)))
    (is (= 0 (last-projected-event-number ds)))))

(deftest sqlite-stores-only-the-cursor-not-the-version
  (let [opts (temp-opts)
        store (:event-store opts)]
    (projection/ensure-db-file! opts)
    (let [ds (query-ds opts)]
      (is (= #{"todos" "event_projection_last_event_number"} (table-names ds)))
      (is (= ["event_number"]
             (mapv :name (jdbc/execute! ds ["PRAGMA table_info(event_projection_last_event_number)"]
                                       {:builder-fn rs/as-unqualified-maps}))))
      (is (= 0 (:user_version (jdbc/execute-one! ds ["PRAGMA user_version"]
                                                {:builder-fn rs/as-unqualified-maps}))))
      (jdbc/execute! ds ["PRAGMA user_version = 42"])
      (append! store 0 {:event/type :todo/created :todo/id "1" :todo/text "No stamp"})
      (projection/catch-up! opts)
      (is (= 0 (last-projected-event-number ds)))
      (is (= 42 (:user_version (jdbc/execute-one! ds ["PRAGMA user_version"]
                                                 {:builder-fn rs/as-unqualified-maps})))
          "catch-up neither checks nor writes user_version"))))

(deftest catch-up-projects-events-and-advances-cursor
  (let [opts (temp-opts)
        store (:event-store opts)
        id (random-uuid)]
    (projection/ensure-db-file! opts)
    (append! store 0 {:event/type :todo/created :todo/id id :todo/text "Write tests"})
    (append! store 1 {:event/type :todo/completed :todo/id id})
    (append! store 2 {:event/type :something/ignored :x 1})
    (is (nil? (projection/catch-up! opts)))
    (let [ds (query-ds opts)]
      (is (= {:id (str id) :text "Write tests" :completed 1} (todo-row ds id)))
      (is (= 2 (last-projected-event-number ds)) "ignored events still advance the cursor")
      (is (nil? (projection/catch-up! opts)))
      (is (= 2 (last-projected-event-number ds))))))

(deftest catch-up-on-an-empty-stream-leaves-the-cursor-unset
  (let [opts (temp-opts)
        store (:event-store opts)]
    (projection/ensure-db-file! opts)
    (is (nil? (projection/catch-up! opts)))
    (let [ds (query-ds opts)]
      (is (nil? (last-projected-event-number ds)))
      (append! store 0 {:event/type :todo/created :todo/id "late" :todo/text "Late"})
      (projection/catch-up! opts)
      (is (= 0 (last-projected-event-number ds)))
      (is (= "Late" (:text (todo-row ds "late")))))))

(deftest idle-catch-up-does-not-contend-for-the-write-lock
  (doseq [event-count [0 1]]
    (testing (str "idle with " event-count " previously projected events")
      (let [base (temp-opts)
            store (:event-store base)
            reads (atom [])
            opts (assoc base :event-store (counting-store store reads))]
        (seed-todos! store event-count)
        (projection/ensure-db-file! opts)
        (reset! reads [])
        (with-open [writer (jdbc/get-connection (query-ds opts))]
          ;; A reserved write lock still permits reads. Even a DELETE against
          ;; an empty cursor table would require a conflicting write lock.
          (jdbc/execute! writer ["BEGIN IMMEDIATE"])
          (try
            (dotimes [_ 2]
              (is (nil? (projection/catch-up! opts))))
            (is (= [event-count event-count] @reads))
            (is (= (when (pos? event-count) (dec event-count))
                   (last-projected-event-number writer)))
            (is (= event-count (todo-count writer)))
            (finally
              (jdbc/execute! writer ["ROLLBACK"]))))))))

(deftest events-with-no-statements-still-advance-the-cursor
  (doseq [entries [[] [{:projection/event-type :noop :projection/fn (constantly nil)}]]]
    (let [base (assoc (temp-opts) :projection/register entries)
          store (:event-store base)
          reads (atom [])
          opts (assoc base :event-store (counting-store store reads))]
      (projection/ensure-db-file! opts)
      (reset! reads [])
      (doseq [n [0 1]]
        (append! store n {:event/type :noop})
        (is (nil? (projection/catch-up! opts)))
        (is (= n (last-projected-event-number (query-ds opts)))))
      (is (nil? (projection/catch-up! opts)))
      (is (= [0 1 2] @reads) "consumed no-op events are not read again")
      (is (= 1 (last-projected-event-number (query-ds opts)))))))

(deftest build-and-catch-up-each-read-the-stream-in-one-call
  (let [opts (temp-opts)
        inner (:event-store opts)
        reads (atom [])
        opts (assoc opts :event-store (counting-store inner reads))]
    (seed-todos! inner 10)
    (is (nil? (projection/build-db-file! opts)))
    (is (= [0] @reads))
    (let [ds (query-ds opts)]
      (is (= 10 (todo-count ds)))
      (is (= 9 (last-projected-event-number ds)))
      (append! inner 10 {:event/type :todo/created :todo/id 10 :todo/text "Next"})
      (projection/catch-up! opts)
      (is (= [0 10] @reads) "resume after the build cursor, not event zero")
      (is (= 11 (todo-count ds)))
      (is (= 10 (last-projected-event-number ds)))
      (projection/catch-up! opts)
      (is (= [0 10 11] @reads) "an idle catch-up also makes one events call")
      (is (= 11 (todo-count ds))))))

(deftest a-read-only-one-at-a-time-source-is-enough
  (let [events (atom {0 {:event/type :todo/created :todo/id "first" :todo/text "Read only"}})
        reads (atom [])
        store (reify event-store/EventSource
                (events [_ from]
                  (util/one-at-a-time #(do (swap! reads conj %) (get @events %)) from)))
        opts (assoc (temp-opts) :event-store store)]
    (projection/ensure-db-file! opts)
    (is (= [0 1] @reads))
    (swap! events assoc 1 {:event/type :todo/completed :todo/id "first"})
    (projection/catch-up! opts)
    (is (= [0 1 1 2] @reads))
    (let [ds (query-ds opts)]
      (is (= 1 (last-projected-event-number ds)))
      (is (= 1 (:completed (todo-row ds "first")))))))

(deftest an-event-without-a-type-is-a-bug-not-something-to-skip
  (let [opts (temp-opts)
        store (:event-store opts)]
    (projection/ensure-db-file! opts)
    (append! store 0 {:event/type :todo/created :todo/id "first" :todo/text "First"})
    (append! store 1 {:todo/text "No type"})
    (try
      (projection/catch-up! opts)
      (is false "expected missing event type")
      (catch clojure.lang.ExceptionInfo e
        (is (= :missing-event-type (:error (ex-data e))))
        (is (= 1 (:event-number (ex-data e))))))
    (let [ds (query-ds opts)]
      (is (nil? (last-projected-event-number ds)))
      (is (zero? (todo-count ds))))))

(deftest a-failed-catch-up-rolls-back-the-entire-run
  (let [fail? (atom true)
        opts (assoc (temp-opts) :projection/register
                    [{:projection/create #'create-todos}
                     {:projection/event-type :todo/created
                      :projection/fn (fn [event]
                                       (when (and @fail? (= "broken" (:todo/id event)))
                                         (throw (ex-info "projection blew up" {})))
                                       (todo-created event))}])
        store (:event-store opts)]
    (append! store 0 {:event/type :todo/created :todo/id "first" :todo/text "First"})
    (projection/ensure-db-file! opts)
    (append! store 1 {:event/type :todo/created :todo/id "second" :todo/text "Second"})
    (append! store 2 {:event/type :todo/created :todo/id "broken" :todo/text "Third"})
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"projection blew up"
                          (projection/catch-up! opts)))
    (let [ds (query-ds opts)]
      (is (= 0 (last-projected-event-number ds)))
      (is (nil? (todo-row ds "second")) "the successful event before the failure rolled back")
      (is (= 1 (todo-count ds)))
      (reset! fail? false)
      (is (nil? (projection/catch-up! opts)))
      (is (= 2 (last-projected-event-number ds)))
      (is (= 3 (todo-count ds))))))

(deftest an-event-source-failure-also-rolls-back-catch-up
  (let [opts (temp-opts)
        store (:event-store opts)
        broken-store (reify event-store/EventSource
                       (events [_ from]
                         (reify clojure.lang.IReduceInit
                           (reduce [_ f init]
                             (reduce f init (event-store/events store from))
                             (throw (ex-info "storage failed" {}))))))]
    (projection/ensure-db-file! opts)
    (seed-todos! store 2)
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"storage failed"
                          (projection/catch-up! (assoc opts :event-store broken-store))))
    (let [ds (query-ds opts)]
      (is (nil? (last-projected-event-number ds)))
      (is (zero? (todo-count ds)))
      (projection/catch-up! opts)
      (is (= 1 (last-projected-event-number ds)))
      (is (= 2 (todo-count ds))))))

(deftest catch-up-does-not-repair-an-uninitialized-file
  (let [opts (assoc (temp-opts) :projection/register [{:projection/create #'create-broken}])
        file (projection/db-file opts)]
    (.mkdirs (.getParentFile file))
    (spit file "")
    (is (thrown? java.sql.SQLException (projection/catch-up! opts)))
    (is (empty? (table-names (query-ds opts))) "no schema or state table was created")))

(deftest failed-builds-roll-back-schema-seeds-and-events-before-publication
  (doseq [failure-phase [:schema :event :store]]
    (testing (name failure-phase)
      (let [base (temp-opts)
            inner (:event-store base)
            fail? (atom true)
            fail! #(throw (ex-info "initialization failed" {}))
            opts (assoc base
                        :event-store (reify event-store/EventSource
                                       (events [_ from]
                                         (reify clojure.lang.IReduceInit
                                           (reduce [_ f init]
                                             (let [result (reduce f init (event-store/events inner from))]
                                               (when (and @fail? (= :store failure-phase)) (fail!))
                                               result)))))
                        :projection/register
                        [{:projection/create #(concat (create-todos)
                                                      (todo-created {:todo/id "seed" :todo/text "Seed"}))}
                         {:projection/create #(when (and @fail? (= :schema failure-phase)) (fail!))}
                         {:projection/event-type :todo/created
                          :projection/fn (fn [event]
                                           (when (and @fail? (= :event failure-phase)
                                                      (= 1 (:todo/id event)))
                                             (fail!))
                                           (todo-created event))}])]
        (seed-todos! inner 2)
        (try
          (projection/ensure-db-file! opts)
          (is false "expected build failure")
          (catch clojure.lang.ExceptionInfo e
            (is (= :db-build-failed (:error (ex-data e))))
            (is (= (str (projection/db-file opts)) (:db/path (ex-data e))))
            (is (= "initialization failed" (.getMessage (.getCause e))))
            (let [failed-file (io/file (:db/tmp-dir (ex-data e)) "projection.db")]
              (is (.isFile failed-file))
              (is (empty? (table-names (file-ds failed-file)))
                  "the state table, schema and seed inserts all rolled back"))))
        (is (not (.exists (projection/db-file opts))))
        (is (empty? (directory-names (:db/dir opts))) "no staging file remains")
        (reset! fail? false)
        (is (= (projection/db-file opts) (projection/ensure-db-file! opts)))
        (let [ds (query-ds opts)]
          (is (= 1 (last-projected-event-number ds)))
          (is (= 3 (todo-count ds))))))))

(deftest build-db-file-supports-default-and-custom-temp-directories
  (doseq [custom? [true false]]
    (let [opts (cond-> (temp-opts) (not custom?) (dissoc :db/tmp-dir))]
      (seed-todos! (:event-store opts) 1)
      (is (nil? (projection/build-db-file! opts)))
      (is (= "todo 0" (:text (todo-row (query-ds opts) 0))))
      (when custom?
        (is (empty? (directory-names (:db/tmp-dir opts))))))))

(deftest build-db-file-never-replaces-an-existing-destination
  (let [opts (temp-opts)
        file (projection/db-file opts)
        sidecar (io/file (str file "-wal"))]
    (.mkdirs (.getParentFile file))
    (spit file "caller-owned file")
    (spit sidecar "caller-owned sidecar")
    (try
      (projection/build-db-file! opts)
      (is false "expected an existing-destination error")
      (catch clojure.lang.ExceptionInfo e
        (is (= :db-already-exists (:error (ex-data e))))
        (is (= (str file) (:db/path (ex-data e))))))
    (is (= "caller-owned file" (slurp file)))
    (is (= "caller-owned sidecar" (slurp sidecar)))
    (is (= #{(.getName file) (.getName sidecar)} (directory-names (:db/dir opts))))
    (is (empty? (directory-names (:db/tmp-dir opts))) "the unused build is discarded")))

(deftest ensure-db-file-trusts-an-existing-filename-without-replaying
  (let [opts (temp-opts)
        file (projection/ensure-db-file! opts)
        no-replays (reify event-store/EventSource
                     (events [_ _] (throw (ex-info "must not replay" {}))))]
    (is (= file (projection/ensure-db-file! (assoc opts :event-store no-replays
                                                      :projection/register [{:projection/create #'create-broken}]))))
    (is (= file (projection/ensure-db-file! (select-keys opts [:db/dir :projection/version]))))
    (is (nil? (projection/catch-up! (assoc opts :projection/register
                                         [{:projection/create #'create-broken}]))))
    (is (zero? (todo-count (query-ds opts))))))

(deftest uuid-versions-build-and-catch-up-independently
  (let [opts (temp-opts)
        store (:event-store opts)
        started (promise)
        release (promise)
        next-opts (assoc opts :projection/version version-b
                        :projection/register
                        [{:projection/create #(do (deliver started true)
                                                  (when (= ::timeout (deref release 10000 ::timeout))
                                                    (throw (ex-info "Timed out waiting for build" {})))
                                                  (create-todos))}
                         {:projection/event-type :todo/created
                          :projection/fn #(todo-created (update % :todo/text str " new"))}])]
    (seed-todos! store 1)
    (projection/ensure-db-file! opts)
    (let [legacy (io/file (:db/dir opts) "v1.db")]
      (spit legacy "legacy integer database")
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"ensure-db-file!"
                            (projection/catch-up! next-opts)))
      (let [build (future (projection/ensure-db-file! next-opts))]
        (try
          (is (= true (deref started 10000 ::timeout)))
          (append! store 1 {:event/type :todo/created :todo/id 1 :todo/text "Next"})
          (is (nil? (projection/catch-up! opts)) "the old version remains writable during a new build")
          (deliver release true)
          (is (= (projection/db-file next-opts) (deref build 10000 ::timeout)))
          (is (= "Next" (:text (todo-row (query-ds opts) 1))))
          (is (= "Next new" (:text (todo-row (query-ds next-opts) 1))))
          (append! store 2 {:event/type :todo/created :todo/id 2 :todo/text "Later"})
          (projection/catch-up! next-opts)
          (is (= 1 (last-projected-event-number (query-ds opts))))
          (is (= 2 (last-projected-event-number (query-ds next-opts))))
          (is (= "legacy integer database" (slurp legacy)))
          (finally
            (deliver release true)
            (future-cancel build)))))))

(deftest concurrent-ensure-never-replaces-the-published-database
  (let [opts (temp-opts)
        store (:event-store opts)
        replayed (promise)
        release (promise)
        slow-store (reify event-store/EventSource
                     (events [_ from]
                       (reify clojure.lang.IReduceInit
                         (reduce [_ f init]
                           (let [result (reduce f init (event-store/events store from))]
                             (deliver replayed true)
                             (when (= ::timeout (deref release 10000 ::timeout))
                               (throw (ex-info "Timed out waiting for publication" {})))
                             result)))))]
    (seed-todos! store 1)
    (let [slow-build (future (projection/ensure-db-file! (assoc opts :event-store slow-store)))]
      (try
        (is (= true (deref replayed 10000 ::timeout)))
        (is (not (.exists (projection/db-file opts))) "the partial build is not published")
        (let [file (projection/ensure-db-file! opts)
              ds (query-ds opts)]
          (append! store 1 {:event/type :todo/created :todo/id 1 :todo/text "Second"})
          (projection/catch-up! opts)
          (with-open [held (jdbc/get-connection ds)]
            (is (= 2 (todo-count held)))
            (deliver release true)
            (is (= file (deref slow-build 10000 ::timeout)))
            (is (= 1 (last-projected-event-number ds)) "the cursor cannot regress")
            (is (= 2 (todo-count ds)))
            (is (= 2 (todo-count held)) "old and new connections see the same database")))
        (is (= #{(str version-a ".db")} (directory-names (:db/dir opts))))
        (is (empty? (directory-names (:db/tmp-dir opts))) "both build directories are cleaned")
        (finally
          (deliver release true)
          (future-cancel slow-build))))))

(deftest malformed-register-entries-fail-before-any-work
  (doseq [entry [nil 42 [] {}
                 {:projection/func #'todo-created}
                 {:projection/create nil}
                 {:projection/create #'register}
                 {:projection/fn #'todo-created}
                 {:projection/event-type nil :projection/fn #'todo-created}
                 {:projection/event-type false :projection/fn #'todo-created}
                 {:projection/event-type :todo/created}
                 {:projection/event-type :todo/created :projection/func #'todo-created}
                 {:projection/event-type :todo/created :projection/fn nil}
                 {:projection/event-type :todo/created :projection/fn :not-a-function}
                 {:projection/event-type :todo/created :projection/fn #'register}
                 {:projection/create #'create-todos :projection/fn #'todo-created}]]
    (testing (pr-str entry)
      (let [calls (atom [])
            opts (assoc (temp-opts)
                        :event-store (reify event-store/EventSource
                                       (events [_ _] (swap! calls conj :events) []))
                        :projection/register [{:projection/create #(do (swap! calls conj :schema)
                                                                        (create-todos))}
                                              entry])]
        (doseq [operation [projection/catch-up! projection/build-db-file! projection/ensure-db-file!]]
          (try
            (operation opts)
            (is false "expected invalid registration")
            (catch clojure.lang.ExceptionInfo e
              (is (= :invalid-projection-register (:error (ex-data e))))
              (is (= 1 (:projection/index (ex-data e))) "index is in the original register")))
          (is (not (.exists (io/file (:db/dir opts)))))
          (is (not (.exists (io/file (:db/tmp-dir opts)))))
          (is (empty? @calls)))))))

(deftest register-must-be-an-ordered-sequence
  (doseq [value [{} #{} "invalid" 42]]
    (let [opts (assoc (temp-opts) :projection/register value)]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"must be a sequence of maps"
                            (projection/catch-up! opts)))
      (is (not (.exists (io/file (:db/dir opts))))))))

(deftest validated-register-preserves-combined-entries-and-handler-order
  (let [opts (assoc (temp-opts) :projection/register
                    (list {:projection/create #'create-todos
                           :projection/event-type :todo/created
                           :projection/fn #'todo-created
                           :description "Extra metadata is allowed"}
                          {:projection/event-type :todo/created :projection/fn todo-completed}))]
    (projection/ensure-db-file! opts)
    (append! (:event-store opts) 0 {:event/type :todo/created :todo/id "1" :todo/text "Ordered"})
    (projection/catch-up! opts)
    (is (= {:id "1" :text "Ordered" :completed 1} (todo-row (query-ds opts) "1")))
    (is (= 0 (last-projected-event-number (query-ds opts))))))

(deftest empty-registers-and-no-op-results-are-valid
  (doseq [entries [[] [{:projection/create (constantly nil)}
                       {:projection/event-type :ignored :projection/fn (constantly nil)}
                       {:projection/event-type :ignored :projection/fn (constantly [])}
                       {:projection/event-type :ignored :projection/fn (constantly [nil])}]]]
    (let [opts (assoc (temp-opts) :projection/register entries)]
      (projection/ensure-db-file! opts)
      (append! (:event-store opts) 0 {:event/type :ignored})
      (is (nil? (projection/catch-up! opts)))
      (is (= 0 (last-projected-event-number (query-ds opts)))))))

(deftest directory-names-are-escaped-as-file-uris
  (let [root (temp-dir "sqlite-projection-escaped")
        opts (assoc (temp-opts)
                    :db/dir (str (.resolve root "db #?% café"))
                    :db/tmp-dir (str (.resolve root "tmp #?% café")))]
    (projection/ensure-db-file! opts)
    (seed-todos! (:event-store opts) 1)
    (is (nil? (projection/catch-up! opts)))
    (is (.isFile (projection/db-file opts)))
    (is (= 1 (todo-count (query-ds opts))))))

(deftest the-same-projection-version-can-be-used-for-separate-streams
  (let [a (temp-opts)
        b (temp-opts)]
    (seed-todos! (:event-store a) 1)
    (seed-todos! (:event-store b) 2)
    (projection/ensure-db-file! a)
    (projection/ensure-db-file! b)
    (is (not= (projection/db-file a) (projection/db-file b)))
    (is (= 1 (todo-count (query-ds a))))
    (is (= 2 (todo-count (query-ds b))))))

(deftest replay-and-old-db-cleanup-remain-caller-owned
  (doseq [name ['replay! 'delete-old-db-files!]]
    (is (false? (contains? (ns-publics 'simplemono.sqlite-projection) name)))))

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'simplemono.sqlite-projection-test)]
    (System/exit (if (zero? (+ fail error)) 0 1))))
