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

(defn event-store-source
  "Application-side adapter; the projection library knows nothing about stores."
  [store]
  (fn [after]
    (if (= after Long/MAX_VALUE)
      []
      (let [from (if (nil? after) 0 (inc after))]
        (eduction (map-indexed (fn [i event]
                                {:cursor (+ from i) :events [event]}))
                  (event-store/events store from))))))

(defn temp-opts []
  (let [root (temp-dir "sqlite-projection-test")
        store (memory/store)]
    {:source (event-store-source store)
     :store store ; Test fixture only; not a projection option.
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
  (:cursor
   (query-one ds {:select [:cursor]
                  :from [:projection_cursor]
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

(deftest catch-up-at-the-last-addressable-event-does-not-overflow
  (let [opts (temp-opts)
        reads (atom [])
        terminal-store (memory/store
                        (atom (sorted-map Long/MAX_VALUE
                                          {:event/type :todo/created
                                           :todo/id "last" :todo/text "Last event"})))
        terminal-opts (assoc opts :source (event-store-source (counting-store terminal-store reads)))]
    (projection/ensure-db-file! opts)
    (let [ds (query-ds opts)]
      ;; Simulate an existing projection through the position before MAX_VALUE.
      (jdbc/execute! ds ["insert into projection_cursor (cursor) values (?)"
                         (dec Long/MAX_VALUE)])
      (is (nil? (projection/catch-up! terminal-opts)))
      (is (= Long/MAX_VALUE (last-projected-event-number ds)))
      (is (= "Last event" (:text (todo-row ds "last"))))
      (is (nil? (projection/catch-up! terminal-opts)) "a terminal cursor is already caught up")
      (is (= [Long/MAX_VALUE] @reads) "never request a position beyond Long/MAX_VALUE")
      (is (= 1 (todo-count ds))))))

(deftest stored-nil-and-false-are-not-end-of-stream-markers
  (doseq [event [nil false]]
    (let [opts (temp-opts)
          store (:store opts)]
      (projection/ensure-db-file! opts)
      (seed-todos! store 1)
      (append! store 1 event)
      (try
        (projection/catch-up! opts)
        (is false "a stored typeless value must fail, not silently end replay")
        (catch clojure.lang.ExceptionInfo e
          (is (= :missing-event-type (:error (ex-data e))))
          (is (= 1 (:cursor (ex-data e))))
          (is (= 0 (:event-index (ex-data e))))))
      (is (nil? (last-projected-event-number (query-ds opts))))
      (is (zero? (todo-count (query-ds opts)))))))

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
                        :source (fn [_] (swap! calls conj :source) [])
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
  (doseq [k [:db/ds :db/path :event-store]
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
          k [:db/dir :projection/version :source :projection/register]
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
                    :source (fn [_] (swap! calls conj :source) [])
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
        opts (assoc opts :source (event-store-source (counting-store (:store opts) reads)))
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
        store (:store opts)
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
        store (:store opts)]
    (projection/ensure-db-file! opts)
    (let [ds (query-ds opts)]
      (is (= #{"todos" "projection_cursor"} (table-names ds)))
      (is (= ["cursor"]
             (mapv :name (jdbc/execute! ds ["PRAGMA table_info(projection_cursor)"]
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
        store (:store opts)
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
        store (:store opts)]
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
            store (:store base)
            reads (atom [])
            opts (assoc base :source (event-store-source (counting-store store reads)))]
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
          store (:store base)
          reads (atom [])
          opts (assoc base :source (event-store-source (counting-store store reads)))]
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
        inner (:store opts)
        reads (atom [])
        opts (assoc opts :source (event-store-source (counting-store inner reads)))]
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
                  (util/one-at-a-time #(do (swap! reads conj %) (find @events %)) from)))
        opts (assoc (temp-opts) :source (event-store-source store))]
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
        store (:store opts)]
    (projection/ensure-db-file! opts)
    (append! store 0 {:event/type :todo/created :todo/id "first" :todo/text "First"})
    (append! store 1 {:todo/text "No type"})
    (try
      (projection/catch-up! opts)
      (is false "expected missing event type")
      (catch clojure.lang.ExceptionInfo e
        (is (= :missing-event-type (:error (ex-data e))))
        (is (= 1 (:cursor (ex-data e))))
        (is (= 0 (:event-index (ex-data e))))))
    (let [ds (query-ds opts)]
      (is (nil? (last-projected-event-number ds)))
      (is (zero? (todo-count ds))))))

(deftest a-failed-catch-up-rolls-back-the-entire-run
  (let [fail? (atom true)
        failure (ex-info "projection blew up: private detail"
                         {:private/detail "do not copy" :event-number 999})
        opts (assoc (temp-opts) :projection/register
                    [{:projection/create #'create-todos}
                     {:projection/event-type :todo/created
                      :projection/fn (fn [event]
                                       (when (and @fail? (= "broken" (:todo/id event)))
                                         (throw failure))
                                       (todo-created event))}])
        store (:store opts)]
    (append! store 0 {:event/type :todo/created :todo/id "first" :todo/text "First"})
    (projection/ensure-db-file! opts)
    (append! store 1 {:event/type :todo/created :todo/id "second" :todo/text "Second"})
    (append! store 2 {:event/type :todo/created :todo/id "broken" :todo/text "private payload"})
    (try
      (projection/catch-up! opts)
      (is false "expected projection failure")
      (catch clojure.lang.ExceptionInfo e
        (is (= {:error :projection-failed
                :cursor 2
                :event-index 0
                :projection/event-type :todo/created}
               (ex-data e)))
        (is (= "Failed to project event 0 at cursor 2 (:todo/created)" (.getMessage e)))
        (is (identical? failure (.getCause e)) "preserve the original exception, including its data")))
    (let [ds (query-ds opts)]
      (is (= 0 (last-projected-event-number ds)))
      (is (nil? (todo-row ds "second")) "the successful event before the failure rolled back")
      (is (= 1 (todo-count ds)))
      (reset! fail? false)
      (is (nil? (projection/catch-up! opts)))
      (is (= 2 (last-projected-event-number ds)))
      (is (= 3 (todo-count ds))))))

(deftest sql-failures-identify-the-event-without-copying-its-payload
  (let [opts (temp-opts)
        store (:store opts)]
    (seed-todos! store 1)
    (projection/ensure-db-file! opts)
    (append! store 1 {:event/type :todo/created :todo/id 1 :todo/text "Second"})
    (append! store 2 {:event/type :todo/created :todo/id 0 :todo/text "private SQL parameter"})
    (try
      (projection/catch-up! opts)
      (is false "expected primary-key violation")
      (catch Exception e
        (is (instance? clojure.lang.ExceptionInfo e))
        (is (= {:error :projection-failed
                :cursor 2
                :event-index 0
                :projection/event-type :todo/created}
               (ex-data e)))
        (is (= "Failed to project event 0 at cursor 2 (:todo/created)" (.getMessage e)))
        (is (instance? java.sql.SQLException (.getCause e)))
        (when-some [cause (.getCause e)]
          (is (re-find #"UNIQUE constraint failed" (.getMessage cause))))))
    (let [ds (query-ds opts)]
      (is (= 0 (last-projected-event-number ds)))
      (is (= 1 (todo-count ds)))
      (is (nil? (todo-row ds 1)) "earlier successful statements still roll back"))))

(deftest later-handler-failures-are-wrapped-once-and-roll-back-earlier-handlers
  (let [failure (IllegalArgumentException. "handler failed")
        opts (update (temp-opts) :projection/register conj
                     {:projection/event-type :todo/created
                      :projection/fn (fn [_] (throw failure))})]
    (projection/ensure-db-file! opts)
    (seed-todos! (:store opts) 1)
    (try
      (projection/catch-up! opts)
      (is false "expected second handler failure")
      (catch Exception e
        (is (= {:error :projection-failed
                :cursor 0
                :event-index 0
                :projection/event-type :todo/created}
               (ex-data e)))
        (is (identical? failure (.getCause e)))))
    (is (nil? (last-projected-event-number (query-ds opts))))
    (is (zero? (todo-count (query-ds opts))))))

(deftest invalid-handler-results-and-formatting-errors-also-have-event-context
  (doseq [result [:not-sql [:not-a-map] {:unknown-clause "private value"}]]
    (let [opts (assoc (temp-opts) :projection/register
                      [{:projection/event-type :bad/result :projection/fn (constantly result)}])]
      (projection/ensure-db-file! opts)
      (append! (:store opts) 0 {:event/type :bad/result :private/data "private payload"})
      (try
        (projection/catch-up! opts)
        (is false "expected invalid handler result")
        (catch clojure.lang.ExceptionInfo e
          (is (= {:error :projection-failed
                  :cursor 0
                  :event-index 0
                  :projection/event-type :bad/result}
                 (ex-data e)))
          (is (instance? clojure.lang.ExceptionInfo (.getCause e)))))
      (is (nil? (last-projected-event-number (query-ds opts)))))))

(deftest an-event-source-failure-also-rolls-back-catch-up
  (let [opts (temp-opts)
        store (:store opts)
        broken-source (fn [after]
                        (reify clojure.lang.IReduceInit
                          (reduce [_ f init]
                            (reduce f init ((:source opts) after))
                            (throw (ex-info "storage failed" {})))))]
    (projection/ensure-db-file! opts)
    (seed-todos! store 2)
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"storage failed"
                          (projection/catch-up! (assoc opts :source broken-source))))
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
            inner (:store base)
            fail? (atom true)
            failure (ex-info "initialization failed" {})
            fail! #(throw failure)
            opts (assoc base
                        :source (fn [after]
                                  (reify clojure.lang.IReduceInit
                                    (reduce [_ f init]
                                      (let [result (reduce f init ((:source base) after))]
                                        (when (and @fail? (= :store failure-phase)) (fail!))
                                        result))))
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
            (let [cause (.getCause e)]
              (if (= :event failure-phase)
                (do
                  (is (= {:error :projection-failed
                          :cursor 1
                          :event-index 0
                          :projection/event-type :todo/created}
                         (ex-data cause)))
                  (is (identical? failure (.getCause cause))
                      "build failure -> contextual projection failure -> original exception"))
                (is (identical? failure cause) "schema and source errors keep their existing handling")))
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
      (seed-todos! (:store opts) 1)
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
        no-replays (fn [_] (throw (ex-info "must not replay" {})))]
    (is (= file (projection/ensure-db-file! (assoc opts :source no-replays
                                                      :projection/register [{:projection/create #'create-broken}]))))
    (is (= file (projection/ensure-db-file! (select-keys opts [:db/dir :projection/version]))))
    (is (nil? (projection/catch-up! (assoc opts :projection/register
                                         [{:projection/create #'create-broken}]))))
    (is (zero? (todo-count (query-ds opts))))))

(deftest uuid-versions-build-and-catch-up-independently
  (let [opts (temp-opts)
        store (:store opts)
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
        store (:store opts)
        replayed (promise)
        release (promise)
        slow-source (fn [after]
                      (reify clojure.lang.IReduceInit
                        (reduce [_ f init]
                          (let [result (reduce f init ((:source opts) after))]
                            (deliver replayed true)
                            (when (= ::timeout (deref release 10000 ::timeout))
                              (throw (ex-info "Timed out waiting for publication" {})))
                            result))))]
    (seed-todos! store 1)
    (let [slow-build (future (projection/ensure-db-file! (assoc opts :source slow-source)))]
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
                        :source (fn [_] (swap! calls conj :source) [])
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
    (append! (:store opts) 0 {:event/type :todo/created :todo/id "1" :todo/text "Ordered"})
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
      (append! (:store opts) 0 {:event/type :ignored})
      (is (nil? (projection/catch-up! opts)))
      (is (= 0 (last-projected-event-number (query-ds opts)))))))

(deftest directory-names-are-escaped-as-file-uris
  (let [root (temp-dir "sqlite-projection-escaped")
        opts (assoc (temp-opts)
                    :db/dir (str (.resolve root "db #?% café"))
                    :db/tmp-dir (str (.resolve root "tmp #?% café")))]
    (projection/ensure-db-file! opts)
    (seed-todos! (:store opts) 1)
    (is (nil? (projection/catch-up! opts)))
    (is (.isFile (projection/db-file opts)))
    (is (= 1 (todo-count (query-ds opts))))))

(deftest the-same-projection-version-can-be-used-for-separate-streams
  (let [a (temp-opts)
        b (temp-opts)]
    (seed-todos! (:store a) 1)
    (seed-todos! (:store b) 2)
    (projection/ensure-db-file! a)
    (projection/ensure-db-file! b)
    (is (not= (projection/db-file a) (projection/db-file b)))
    (is (= 1 (todo-count (query-ds a))))
    (is (= 2 (todo-count (query-ds b))))))

(deftest replay-and-old-db-cleanup-remain-caller-owned
  (doseq [name ['replay! 'delete-old-db-files!]]
    (is (false? (contains? (ns-publics 'simplemono.sqlite-projection) name)))))

(defn batch-source [batches reads]
  (fn [after]
    (swap! reads conj after)
    ;; Capture a finite history for this invocation; do not chase later writes.
    (eduction (filter #(or (nil? after) (> (:cursor %) after))) @batches)))

(defn created [id]
  {:event/type :todo/created :todo/id id :todo/text (str "todo " id)})

(deftest source-cursors-are-not-counts-and-resume-is-exclusive
  (let [batches (atom [{:cursor 1000 :events [(created "one")
                                           {:event/type :todo/completed :todo/id "one"}]}
                       {:cursor 1100 :events []}])
        reads (atom [])
        opts (assoc (temp-opts) :source (batch-source batches reads))]
    (projection/ensure-db-file! opts)
    (let [ds (query-ds opts)]
      (is (= 1100 (last-projected-event-number ds)))
      (is (= 1 (:completed (todo-row ds "one"))))
      (swap! batches conj {:cursor 2500 :events [(created "two") {:event/type :ignored}]})
      (is (nil? (projection/catch-up! opts)))
      (is (= 2500 (last-projected-event-number ds)))
      (is (= 2 (todo-count ds)))
      (is (nil? (projection/catch-up! opts)))
      (is (= [nil 1100 2500] @reads)))))

(deftest empty-batches-advance-the-cursor-and-then-become-idle
  (let [batches (atom [{:cursor 0 :events []}])
        reads (atom [])
        opts (assoc (temp-opts) :source (batch-source batches reads)
                    :projection/register [])]
    (projection/ensure-db-file! opts)
    (is (= 0 (last-projected-event-number (query-ds opts))))
    (swap! batches conj {:cursor 1000 :events []})
    (projection/catch-up! opts)
    (is (= 1000 (last-projected-event-number (query-ds opts))))
    (with-open [writer (jdbc/get-connection (query-ds opts))]
      (jdbc/execute! writer ["BEGIN IMMEDIATE"])
      (try
        (is (nil? (projection/catch-up! opts)))
        (is (= 1000 (last-projected-event-number writer)))
        (finally (jdbc/execute! writer ["ROLLBACK"]))))
    (is (= [nil 0 1000] @reads))))

(deftest failure-inside-a-batch-rolls-back-that-batch-and-earlier-batches
  (let [batches (atom [{:cursor 100 :events [(created "one")]}])
        reads (atom [])
        fail? (atom true)
        failure (ex-info "private handler detail" {:cursor :not-the-real-cursor})
        opts (assoc (temp-opts)
                    :source (batch-source batches reads)
                    :projection/register
                    [{:projection/create #'create-todos}
                     {:projection/event-type :todo/created
                      :projection/fn (fn [event]
                                       (when (and @fail? (= "broken" (:todo/id event)))
                                         (throw failure))
                                       (todo-created event))}])]
    (projection/ensure-db-file! opts)
    (swap! batches into [{:cursor 200 :events [(created "two")]}
                         {:cursor 300 :events [(created "three") (created "broken")]}])
    (let [error (try (projection/catch-up! opts) (catch Exception e e))
          ds (query-ds opts)]
      (is (= {:error :projection-failed :cursor 300 :event-index 1
              :projection/event-type :todo/created}
             (ex-data error)))
      (is (identical? failure (.getCause error)))
      (is (= 100 (last-projected-event-number ds)))
      (is (= 1 (todo-count ds)))
      (reset! fail? false)
      (projection/catch-up! opts)
      (is (= 300 (last-projected-event-number ds)))
      (is (= 4 (todo-count ds)))
      (is (= [nil 100 100] @reads) "retry starts at the committed transaction, not the failed one"))))

(deftest a-batch-is-not-partially-visible-to-other-connections
  (let [base (temp-opts)
        batches (atom [])
        observed (atom nil)
        opts (assoc base :source (batch-source batches (atom []))
                    :projection/register
                    [{:projection/create #'create-todos}
                     {:projection/event-type :todo/created
                      :projection/fn (fn [event]
                                       (when (= "two" (:todo/id event))
                                         (reset! observed [(todo-count (query-ds base))
                                                           (last-projected-event-number (query-ds base))]))
                                       (todo-created event))}])]
    (projection/ensure-db-file! opts)
    (swap! batches conj {:cursor 42 :events [(created "one") (created "two")]})
    (projection/catch-up! opts)
    (is (= [0 nil] @observed) "another connection sees neither the first event nor the new cursor")
    (is (= 2 (todo-count (query-ds opts))))
    (is (= 42 (last-projected-event-number (query-ds opts))))))

(deftest malformed-batches-roll-back-without-invoking-their-handlers
  (doseq [bad (concat
                [nil false 42 "private batch" [] {}]
                (map #(hash-map :cursor % :events [(created "bad")])
                     [nil false -1 1.0 1N (int 1) (biginteger "1") "1000" {} (random-uuid)])
                [{:cursor 20} {:cursor 20 :events nil} {:cursor 20 :events false}
                 {:cursor 20 :events {}} {:cursor 20 :events #{}}
                 {:cursor 20 :events (list (created "bad"))}])]
    (let [handled (atom [])
          opts (assoc (temp-opts) :source (constantly [])
                      :projection/register
                      [{:projection/create #'create-todos}
                       {:projection/event-type :todo/created
                        :projection/fn (fn [event]
                                         (swap! handled conj (:todo/id event))
                                         (todo-created event))}])]
      (projection/ensure-db-file! opts)
      (let [error (try
                    (projection/catch-up! (assoc opts :source
                                                (constantly [{:cursor 10 :events [(created "good")]} bad])))
                    (catch Exception e e))]
        (is (= :invalid-batch (:error (ex-data error))))
        (is (= ["good"] @handled))
        (is (nil? (last-projected-event-number (query-ds opts))))
        (is (zero? (todo-count (query-ds opts))))))))

(deftest repeated-or-decreasing-cursors-are-errors-even-with-no-events
  (doseq [cursor [0 10 15 20]]
    (let [opts (assoc (temp-opts) :source (constantly [{:cursor 10 :events []}]))]
      (projection/ensure-db-file! opts)
      (let [error (try
                    (projection/catch-up! (assoc opts :source
                                                (constantly [{:cursor 20 :events [(created "prefix")]}
                                                             {:cursor cursor :events []}])))
                    (catch Exception e e))]
        (is (= {:error :invalid-batch :batch/key :cursor :after-cursor 20}
               (ex-data error)))
        (is (= 10 (last-projected-event-number (query-ds opts))))
        (is (zero? (todo-count (query-ds opts))))))))

(deftest source-functions-and-results-are-validated
  (doseq [source [:not-a-function {} [] #{} true 42 #'register]
          operation [projection/catch-up! projection/build-db-file! projection/ensure-db-file!]]
    (let [opts (assoc (temp-opts) :source source)
          error (try (operation opts) (catch Exception e e))]
      (is (= :invalid-source (:error (ex-data error))))
      (is (not (.exists (io/file (:db/dir opts)))))))
  (doseq [result [nil false {} #{} "" 42]]
    (let [opts (assoc (temp-opts) :source (constantly []))]
      (projection/ensure-db-file! opts)
      (let [error (try (projection/catch-up! (assoc opts :source (constantly result)))
                       (catch Exception e e))]
        (is (= :invalid-source-result (:error (ex-data error))))
        (is (nil? (last-projected-event-number (query-ds opts))))))))

(defn empty-source [_] [])

(deftest a-var-containing-a-source-function-is-supported
  (let [opts (assoc (temp-opts) :source #'empty-source)]
    (projection/ensure-db-file! opts)
    (is (nil? (projection/catch-up! opts)))))

(deftest invalid-batches-also-prevent-publication-of-a-new-db
  (let [opts (assoc (temp-opts) :source (constantly [{:cursor 10 :events [(created "one")]}
                                                  {:cursor 10 :events []}]))
        error (try (projection/ensure-db-file! opts) (catch Exception e e))]
    (is (= :db-build-failed (:error (ex-data error))))
    (is (= :invalid-batch (:error (ex-data (.getCause error)))))
    (is (not (.exists (projection/db-file opts))))))

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'simplemono.sqlite-projection-test)]
    (System/exit (if (zero? (+ fail error)) 0 1))))
