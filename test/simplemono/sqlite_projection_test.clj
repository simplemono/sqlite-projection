(ns simplemono.sqlite-projection-test
  (:require [clojure.test :refer [deftest is testing run-tests]]
            [honey.sql :as sql]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [simplemono.event-store :as event-store]
            [simplemono.event-store.memory :as memory]
            [simplemono.event-store.util :as util]
            [simplemono.sqlite-projection :as projection])
  (:import (java.nio.file Files)))

(defn create-todos
  []
  [{:create-table [:todos :if-not-exists]
    :with-columns [[:id :text [:primary-key]]
                   [:text :text [:not nil]]
                   [:completed :integer [:not nil] [:default 0]]]}])

(defn create-broken
  []
  (throw (ex-info "broken projection schema" {})))

(defn todo-created
  [event]
  [{:insert-into :todos
    :values [{:id (str (:todo/id event))
              :text (:todo/text event)
              :completed 0}]}])

(defn todo-completed
  [event]
  [{:update :todos
    :set {:completed 1}
    :where [:= :id (str (:todo/id event))]}])

(def register
  [{:projection/create #'create-todos}
   {:projection/event-type :todo/created
    :projection/fn #'todo-created}
   {:projection/event-type :todo/completed
    :projection/fn #'todo-completed}])

(defn temp-dir
  [prefix]
  (Files/createTempDirectory prefix
                             (make-array java.nio.file.attribute.FileAttribute 0)))

(defn temp-path
  []
  (str (.resolve (temp-dir "sqlite-projection-test")
                 "projection.db")))

(defn temp-ds
  []
  (jdbc/get-datasource (str "jdbc:sqlite:" (temp-path))))

(defn query-one
  [ds statement]
  (jdbc/execute-one! ds (sql/format statement)
                     {:builder-fn rs/as-unqualified-maps}))

(defn stored-projection-version
  [ds]
  (:user_version
   (jdbc/execute-one! ds ["PRAGMA user_version"]
                      {:builder-fn rs/as-unqualified-maps})))

(defn last-projected-event-number
  [ds]
  (:event_number
   (query-one ds {:select [:event_number]
                  :from [:event_projection_last_event_number]
                  :limit 1})))

(defn append!
  [store n event]
  (is (true? (event-store/try-append! store n event))))

(defn todo-row
  [ds id]
  (query-one ds {:select [:*]
                 :from [:todos]
                 :where [:= :id (str id)]}))

(deftest catch-up-projects-events-and-advances-cursor
  (let [store (memory/store)
        ds (temp-ds)
        id (random-uuid)]
    (append! store 0 {:event/type :todo/created
                      :todo/id id
                      :todo/text "Write tests"})
    (append! store 1 {:event/type :todo/completed
                      :todo/id id})
    (append! store 2 {:event/type :something/ignored
                      :x 1})
    (is (nil? (projection/catch-up! {:event-store store
                                     :db/ds ds
                                     :projection/version 1
                                     :projection/register register})))
    (is (= {:id (str id)
            :text "Write tests"
            :completed 1}
           (todo-row ds id)))
    (is (= 1 (stored-projection-version ds)))
    (is (= 2 (last-projected-event-number ds))
        "an ignored event still advances the cursor past it")

    (testing "a second catch-up is a no-op"
      (is (nil? (projection/catch-up! {:event-store store
                                       :db/ds ds
                                       :projection/version 1
                                       :projection/register register})))
      (is (= 2 (last-projected-event-number ds))))))

(deftest catch-up-continues-from-cursor
  (let [store (memory/store)
        ds (temp-ds)
        first-id (random-uuid)
        second-id (random-uuid)
        opts {:event-store store
              :db/ds ds
              :projection/version 1
              :projection/register register}]
    (append! store 0 {:event/type :todo/created
                      :todo/id first-id
                      :todo/text "First"})
    (projection/catch-up! opts)
    (is (= 0 (last-projected-event-number ds)))
    (append! store 1 {:event/type :todo/created
                      :todo/id second-id
                      :todo/text "Second"})
    (is (nil? (projection/catch-up! opts)))
    (is (= 1 (last-projected-event-number ds)))
    (is (= "First" (:text (todo-row ds first-id))))
    (is (= "Second" (:text (todo-row ds second-id))))))

(deftest catch-up-on-an-empty-stream-leaves-the-cursor-unset
  (let [store (memory/store)
        ds (temp-ds)
        opts {:event-store store
              :db/ds ds
              :projection/version 1
              :projection/register register}]
    (is (nil? (projection/catch-up! opts)))
    (is (nil? (last-projected-event-number ds))
        "no event was projected, so there is no cursor to write")
    (is (= 1 (stored-projection-version ds)))

    (testing "the next catch-up starts at event 0"
      (let [id (random-uuid)]
        (append! store 0 {:event/type :todo/created
                          :todo/id id
                          :todo/text "Late"})
        (projection/catch-up! opts)
        (is (= 0 (last-projected-event-number ds)))
        (is (= "Late" (:text (todo-row ds id))))))))

(deftest a-store-that-only-reads-is-enough
  ;; The library never appends, so a projection can be handed something that
  ;; implements EventSource and nothing else. Handing it a writable store is a
  ;; convenience, not a requirement.
  (let [id (random-uuid)
        events {0 {:event/type :todo/created
                   :todo/id id
                   :todo/text "Read only"}}
        requested (atom [])
        store (reify event-store/EventSource
                (events [_ from]
                  (swap! requested conj from)
                  (util/one-at-a-time #(get events %) from)))
        ds (temp-ds)]
    (is (nil? (projection/catch-up! {:event-store store
                                     :db/ds ds
                                     :projection/version 1
                                     :projection/register register})))
    (is (= [0] @requested) "one call to `events`, whatever it costs inside")
    (is (= 0 (last-projected-event-number ds)))
    (is (= "Read only" (:text (todo-row ds id))))))

(defn counting-store
  "Wraps `store` and records where each read started. One entry per `events`
   call, so a test can show the library asks once and lets the store decide
   what that costs."
  [store replays]
  (reify
    event-store/EventAppend
    (try-append! [_ event-number event]
      (event-store/try-append! store event-number event))

    event-store/EventSource
    (events [_ from]
      (swap! replays conj from)
      (event-store/events store from))))

(defn- seed-todos!
  [store n]
  (doseq [i (range n)]
    (append! store i {:event/type :todo/created
                      :todo/id i
                      :todo/text (str "todo " i)})))

(defn- todo-count
  [ds]
  (:count (query-one ds {:select [[[:count :*] :count]] :from [:todos]})))

(deftest a-rebuild-reads-the-stream-in-one-call
  (testing "build-db-file! hands the whole stream over in one call"
    (let [inner (memory/store)
          replays (atom [])
          store (counting-store inner replays)
          path (temp-path)]
      (seed-todos! inner 10)
      (projection/build-db-file! {:event-store store
                                  :db/path path
                                  :projection/version 1
                                  :projection/register register})
      (is (= [0] @replays) "one read, starting at event 0")
      (let [ds (jdbc/get-datasource (str "jdbc:sqlite:" path))]
        (is (= 10 (todo-count ds)))
        (is (= 9 (last-projected-event-number ds)))))))

(deftest catch-up-reads-the-stream-in-one-call-too
  (let [inner (memory/store)
        replays (atom [])
        store (counting-store inner replays)
        ds (temp-ds)]
    (seed-todos! inner 6)
    (projection/catch-up! {:event-store store
                           :db/ds ds
                           :projection/version 1
                           :projection/register register})
    (is (= [0] @replays))
    (is (= 6 (todo-count ds)))
    (is (= 5 (last-projected-event-number ds)))))

(deftest the-replay-resumes-from-the-cursor
  (testing "a second run starts after the events the first one applied"
    (let [inner (memory/store)
          replays (atom [])
          store (counting-store inner replays)
          ds (temp-ds)
          opts {:event-store store
                :db/ds ds
                :projection/version 1
                :projection/register register}]
      (seed-todos! inner 3)
      (projection/catch-up! opts)
      (is (= 2 (last-projected-event-number ds)))
      (append! inner 3 {:event/type :todo/created :todo/id 3 :todo/text "todo 3"})
      (projection/catch-up! opts)
      (is (= [0 3] @replays) "the second run starts at the cursor plus one")
      (is (= 4 (todo-count ds)) "the earlier events are not applied twice")
      (is (= 3 (last-projected-event-number ds))))))

(deftest a-store-that-reads-one-event-at-a-time-still-works
  (testing "storage where a bulk read buys nothing is read singly, and works"
    (let [inner (memory/store)
          ds (temp-ds)
          reads (atom [])
          ;; What every implementation looks like before it writes a bulk read
          ;; of its own: `one-at-a-time` over a function of an event number.
          store (reify event-store/EventSource
                  (events [_ from]
                    (util/one-at-a-time
                     (fn [n]
                       (swap! reads conj n)
                       (reduce (fn [_ e] (reduced e)) nil
                               (event-store/events inner n)))
                     from)))]
      (seed-todos! inner 5)
      (projection/catch-up! {:event-store store
                             :db/ds ds
                             :projection/version 1
                             :projection/register register})
      (is (= 5 (todo-count ds)))
      (is (= [0 1 2 3 4 5] @reads) "each event, then the miss that ends it")
      (is (= 4 (last-projected-event-number ds))))))

(deftest an-event-without-a-type-is-a-bug-not-something-to-skip
  (let [store (memory/store)
        ds (temp-ds)]
    (append! store 0 {:todo/text "No type"})
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"missing :event/type"
                          (projection/catch-up! {:event-store store
                                                 :db/ds ds
                                                 :projection/version 1
                                                 :projection/register register})))))

(deftest a-failed-catch-up-leaves-the-cursor-where-it-was
  (let [store (memory/store)
        ds (temp-ds)
        id (random-uuid)
        boom (fn [event]
               (if (= "broken" (:todo/id event))
                 (throw (ex-info "projection blew up" {}))
                 (todo-created event)))
        opts {:event-store store
              :db/ds ds
              :projection/version 1
              :projection/register register}]
    (append! store 0 {:event/type :todo/created
                      :todo/id id
                      :todo/text "First"})
    (projection/catch-up! opts)
    (append! store 1 {:event/type :todo/created
                      :todo/id "second"
                      :todo/text "Second"})
    (append! store 2 {:event/type :todo/created
                      :todo/id "broken"
                      :todo/text "Third"})
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"projection blew up"
                          (projection/catch-up!
                           (assoc opts :projection/register
                                  [{:projection/create #(concat (create-todos)
                                                                (todo-created {:todo/id "seed"
                                                                               :todo/text "Seed"}))}
                                   {:projection/event-type :todo/created
                                    :projection/fn boom}]))))
    (is (= 0 (last-projected-event-number ds))
        "the transaction rolled back, so the cursor still points at event 0")
    (is (= 1 (stored-projection-version ds)))
    (is (nil? (todo-row ds "seed")) "schema effects rolled back")
    (is (nil? (todo-row ds "second")) "the successful event before the failure rolled back")
    (is (= 1 (todo-count ds)))
    (is (nil? (projection/catch-up! opts)))
    (is (= 2 (last-projected-event-number ds)))
    (is (= 3 (todo-count ds)))))

(deftest failed-initialization-rolls-back-schema-seeds-and-events
  (doseq [failure-phase [:schema :event]]
    (testing (name failure-phase)
      (let [store (memory/store)
            ds (temp-ds)
            fail? (atom true)
            fail! #(throw (ex-info "initialization failed" {}))
            opts {:event-store store
                  :db/ds ds
                  :projection/version 1
                  :projection/register
                  [{:projection/create #(concat (create-todos)
                                                (todo-created {:todo/id "seed" :todo/text "Seed"}))}
                   {:projection/create #(when (and @fail? (= :schema failure-phase)) (fail!))}
                   {:projection/event-type :todo/created
                    :projection/fn (fn [event]
                                     (when (and @fail? (= :event failure-phase)
                                                (= "second" (:todo/id event)))
                                       (fail!))
                                     (todo-created event))}]}]
        (append! store 0 {:event/type :todo/created :todo/id "first" :todo/text "First"})
        (append! store 1 {:event/type :todo/created :todo/id "second" :todo/text "Second"})
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"initialization failed"
                              (projection/catch-up! opts)))
        (is (= 0 (stored-projection-version ds)))
        (is (empty? (jdbc/execute! ds ["select name from sqlite_master where type = 'table'"]))
            "neither the projection schema nor the state table survives")
        (reset! fail? false)
        (is (nil? (projection/catch-up! opts)) "retry does not collide with leftover seed rows")
        (is (= 1 (stored-projection-version ds)))
        (is (= 1 (last-projected-event-number ds)))
        (is (= 3 (todo-count ds)))))))

(deftest a-concurrent-initialization-cannot-invalidate-the-version-check
  (let [store (memory/store)
        ds (temp-ds)
        checked (promise)
        release (promise)
        check-version @#'projection/ensure-compatible-version!
        opts {:event-store store :db/ds ds :projection/version 1 :projection/register register}
        v2-opts (assoc opts :projection/version 2
                       :projection/register [{:projection/create #'create-todos}
                                             {:projection/event-type :todo/created
                                              :projection/fn #(todo-created (assoc % :todo/text "v2"))}])]
    ;; WAL lets v1 commit while v2 holds a read snapshot. Pause immediately
    ;; after v2's version check to exercise the check-to-write race, not timing.
    (jdbc/execute! ds ["PRAGMA journal_mode=WAL"])
    (append! store 0 {:event/type :todo/created :todo/id "first" :todo/text "v1"})
    (with-redefs-fn {#'projection/ensure-compatible-version!
                    (fn [conn expected]
                      (let [actual (check-version conn expected)]
                        (when (= 2 expected)
                          (deliver checked true)
                          (when (= ::timeout (deref release 10000 ::timeout))
                            (throw (ex-info "Timed out waiting for concurrent initialization" {}))))
                        actual))}
      (fn []
        (let [v2 (future (try (projection/catch-up! v2-opts)
                             :committed
                             (catch Exception e e)))]
          (try
            (is (= true (deref checked 10000 ::timeout)))
            (is (nil? (projection/catch-up! opts)))
            (deliver release true)
            (let [result (deref v2 10000 ::timeout)]
              (is (instance? java.sql.SQLException result)
                  "v2 must fail to upgrade its stale snapshot, not restamp v1 data")
              (when (instance? java.sql.SQLException result)
                (is (contains? #{5 517} (.getErrorCode ^java.sql.SQLException result))
                    "SQLITE_BUSY or SQLITE_BUSY_SNAPSHOT")))
            (is (= 1 (stored-projection-version ds)))
            (is (= "v1" (:text (todo-row ds "first"))))
            (is (= 0 (last-projected-event-number ds)))
            (is (thrown-with-msg? clojure.lang.ExceptionInfo #"version mismatch"
                                  (projection/catch-up! v2-opts))
                "retry checks the newly committed version")
            (finally
              (deliver release true)
              (future-cancel v2))))))))

(deftest no-in-place-replay-is-needed-for-rebuilds
  (is (false? (contains? (ns-publics 'simplemono.sqlite-projection)
                         'replay!))))

(deftest invalid-versions-are-rejected-before-any-work
  (doseq [version [-1 0 2147483648 Long/MAX_VALUE (inc (bigint Long/MAX_VALUE))
                   1.5 1.0 "1" true]]
    (testing (pr-str version)
      (let [dir (temp-dir "sqlite-projection-invalid-version")
            tmp-dir (temp-dir "sqlite-projection-invalid-version-tmp")
            path (str (.resolve dir "projection.db"))
            existing (.toFile (.resolve dir "v1.db"))
            calls (atom [])
            opts {:event-store (reify event-store/EventSource
                                 (events [_ _] (swap! calls conj :events) []))
                  :db/ds (jdbc/get-datasource (str "jdbc:sqlite:" path))
                  :db/path path
                  :db/dir (str dir)
                  :db/tmp-dir (str tmp-dir)
                  :projection/version version
                  :projection/register [{:projection/create #(swap! calls conj :schema)}]}]
        (spit existing "keep")
        (doseq [operation [projection/catch-up! projection/build-db-file!
                           projection/db-file projection/ensure-db-file!]]
          (try
            (operation opts)
            (is false "expected invalid version")
            (catch clojure.lang.ExceptionInfo e
              (is (re-find #"integer from 1 to 2147483647" (.getMessage e)))
              (is (= version (:projection/version (ex-data e))))))
          (is (= ["v1.db"] (vec (.list (.toFile dir)))))
          (is (= "keep" (slurp existing)))
          (is (empty? (.list (.toFile tmp-dir))))
          (is (empty? @calls)))))))

(deftest version-boundaries-round-trip-and-remain-catchable
  (doseq [version [1 Integer/MAX_VALUE]
          initialize! [projection/catch-up! projection/build-db-file!]]
    (let [store (memory/store)
          path (temp-path)
          ds (jdbc/get-datasource (str "jdbc:sqlite:" path))
          opts {:event-store store
                :db/ds ds
                :db/path path
                :projection/version version
                :projection/register register}]
      (append! store 0 {:event/type :todo/created :todo/id "first" :todo/text "First"})
      (is (nil? (initialize! opts)))
      (is (= version (stored-projection-version ds)))
      (is (= 0 (last-projected-event-number ds)))
      (is (nil? (projection/catch-up! opts)) "an idle second run accepts the stamp")
      (append! store 1 {:event/type :todo/created :todo/id "second" :todo/text "Second"})
      (is (nil? (projection/catch-up! opts)))
      (is (= version (stored-projection-version ds)))
      (is (= 1 (last-projected-event-number ds)))
      (is (= 2 (todo-count ds)))
      (is (= (str "v" version ".db")
             (.getName (projection/db-file {:db/dir "unused" :projection/version version})))))))

(deftest version-mismatch-requires-rebuild
  (let [store (memory/store)
        ds (temp-ds)
        id (random-uuid)
        opts {:event-store store
              :db/ds ds
              :projection/version 1
              :projection/register register}]
    (append! store 0 {:event/type :todo/created
                      :todo/id id
                      :todo/text "v1"})
    (projection/catch-up! opts)
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"version mismatch"
                          (projection/catch-up! (assoc opts :projection/version 2))))
    (let [path (str (.resolve (temp-dir "sqlite-projection-v2")
                              "todo-projection-v2.db"))]
      (is (nil? (projection/build-db-file! {:event-store store
                                            :db/path path
                                            :projection/version 2
                                            :projection/register register})))
      (let [rebuilt-ds (jdbc/get-datasource (str "jdbc:sqlite:" path))]
        (is (= 2 (stored-projection-version rebuilt-ds)))
        (is (= "v1" (:text (todo-row rebuilt-ds id))))
        (is (= 0 (last-projected-event-number rebuilt-ds))
            "the rebuilt file can be caught up from where the replay stopped")))))

(deftest build-db-file-builds-caller-supplied-path
  (let [store (memory/store)
        dir (temp-dir "sqlite-projection-build")
        tmp-dir (temp-dir "sqlite-projection-tmp")
        path (str (.resolve dir "todo-projection-v1.db"))
        id (random-uuid)]
    (append! store 0 {:event/type :todo/created
                      :todo/id id
                      :todo/text "Built"})
    (is (nil? (projection/build-db-file! {:event-store store
                                          :db/path path
                                          :db/tmp-dir (str tmp-dir)
                                          :projection/version 1
                                          :projection/register register})))
    (let [ds (jdbc/get-datasource (str "jdbc:sqlite:" path))]
      (is (= "Built" (:text (todo-row ds id))))
      (is (= 1 (stored-projection-version ds))))))

(deftest build-db-file-never-replaces-an-existing-destination
  (let [dir (temp-dir "sqlite-projection-existing")
        tmp-dir (temp-dir "sqlite-projection-existing-tmp")
        path (str (.resolve dir "projection.db"))
        sidecar (str path "-wal")]
    (spit path "caller-owned file")
    (spit sidecar "caller-owned sidecar")
    (try
      (projection/build-db-file! {:event-store (memory/store)
                                  :db/path path
                                  :db/tmp-dir (str tmp-dir)
                                  :projection/version 1
                                  :projection/register register})
      (is false "expected an existing-destination error")
      (catch clojure.lang.ExceptionInfo e
        (is (= :db-already-exists (:error (ex-data e))))
        (is (= path (:db/path (ex-data e))))))
    (is (= "caller-owned file" (slurp path)))
    (is (= "caller-owned sidecar" (slurp sidecar)))
    (is (= #{"projection.db" "projection.db-wal"}
           (set (.list (.toFile dir)))) "no staging file remains")
    (is (empty? (.list (.toFile tmp-dir))) "the unused build is discarded")))

(deftest build-db-file-keeps-final-path-empty-when-build-fails
  (let [store (memory/store)
        dir (temp-dir "sqlite-projection-failed-build")
        tmp-dir (temp-dir "sqlite-projection-failed-build-tmp")
        path (str (.resolve dir "todo-projection-v1.db"))]
    (append! store 0 {:event/type :todo/created
                      :todo/id (random-uuid)
                      :todo/text "Built"})
    (try
      (projection/build-db-file! {:event-store store
                                  :db/path path
                                  :db/tmp-dir (str tmp-dir)
                                  :projection/version 1
                                  :projection/register [{:projection/create #'create-broken}]})
      (is false "expected build failure")
      (catch clojure.lang.ExceptionInfo e
        (is (= :db-build-failed (:error (ex-data e))))
        (is (= path (:db/path (ex-data e))))
        (is (some? (:db/tmp-dir (ex-data e))))))
    (is (false? (.exists (java.io.File. path))))))

(deftest invalid-register-is-rejected
  (let [store (memory/store)
        ds (temp-ds)]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #":projection/create value must be a function"
                          (projection/catch-up! {:event-store store
                                                 :db/ds ds
                                                 :projection/version 1
                                                 :projection/register [{:projection/create :not-a-function
                                                                        :projection/event-type :todo/created
                                                                        :projection/fn #'todo-created}]})))))

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
      (let [path (temp-path)
            tmp-dir (temp-dir "sqlite-projection-invalid-register")
            calls (atom [])
            store (reify event-store/EventSource
                    (events [_ _] (swap! calls conj :events) []))
            opts {:event-store store
                  :db/ds (jdbc/get-datasource (str "jdbc:sqlite:" path))
                  :db/path path
                  :db/tmp-dir (str tmp-dir)
                  :projection/version 1
                  :projection/register [{:projection/create #(do (swap! calls conj :schema)
                                                                  (create-todos))}
                                        entry]}]
        (doseq [operation [projection/catch-up! projection/build-db-file!]]
          (try
            (operation opts)
            (is false "expected invalid registration")
            (catch clojure.lang.ExceptionInfo e
              (is (= :invalid-projection-register (:error (ex-data e))))
              (is (= 1 (:projection/index (ex-data e))) "index is in the original register")))
          (is (not (.exists (java.io.File. path))))
          (is (empty? (.list (.toFile tmp-dir))))
          (is (empty? @calls) "no callbacks or event reads run"))))))

(deftest register-must-be-an-ordered-sequence
  (doseq [value [{} #{} "invalid" 42]]
    (let [path (temp-path)]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"must be a sequence of maps"
                            (projection/catch-up! {:event-store (memory/store)
                                                   :db/ds (jdbc/get-datasource (str "jdbc:sqlite:" path))
                                                   :projection/version 1
                                                   :projection/register value})))
      (is (not (.exists (java.io.File. path)))))))

(deftest validated-register-preserves-combined-entries-and-handler-order
  (let [store (memory/store)
        ds (temp-ds)]
    (append! store 0 {:event/type :todo/created :todo/id "1" :todo/text "Ordered"})
    (projection/catch-up! {:event-store store
                           :db/ds ds
                           :projection/version 1
                           :projection/register
                           (list {:projection/create #'create-todos
                                  :projection/event-type :todo/created
                                  :projection/fn #'todo-created
                                  :description "Extra metadata is allowed"}
                                 {:projection/event-type :todo/created
                                  :projection/fn todo-completed})})
    (is (= {:id "1" :text "Ordered" :completed 1} (todo-row ds "1")))
    (is (= 0 (last-projected-event-number ds)))))

(deftest an-empty-register-is-valid
  (let [store (memory/store)
        ds (temp-ds)]
    (append! store 0 {:event/type :something/ignored})
    (is (nil? (projection/catch-up! {:event-store store
                                     :db/ds ds
                                     :projection/version 1
                                     :projection/register []})))
    (is (= 0 (last-projected-event-number ds)))))

(deftest ensure-db-file-builds-only-the-missing-version
  (let [store (memory/store)
        dir (str (temp-dir "sqlite-projection-ensure"))
        id (random-uuid)
        opts {:event-store store
              :db/dir dir
              :projection/version 1
              :projection/register register}]
    (append! store 0 {:event/type :todo/created
                      :todo/id id
                      :todo/text "Ensure"})
    (let [file (projection/ensure-db-file! opts)]
      (is (= "v1.db" (.getName file)) "the version is in the file name")
      (is (.exists file))
      (let [ds (jdbc/get-datasource (str "jdbc:sqlite:" file))]
        (is (= "Ensure" (:text (todo-row ds id))))
        (is (= 1 (stored-projection-version ds)))))

    (testing "an existing file is not rebuilt"
      (let [no-replays (reify event-store/EventSource
                         (events [_ _]
                           (throw (ex-info "an existing file must not replay" {}))))
            file (projection/ensure-db-file! (assoc opts :event-store no-replays))]
        (is (.exists file))))

    (testing "a version jump builds its own file and leaves the old one"
      (let [file (projection/ensure-db-file! (assoc opts :projection/version 3))
            ds (jdbc/get-datasource (str "jdbc:sqlite:" file))]
        (is (= "v3.db" (.getName file)))
        (is (= 3 (stored-projection-version ds)))
        (is (= "Ensure" (:text (todo-row ds id))))
        (is (.exists (projection/db-file opts))
            "v1.db stays servable while and after v3 builds")))))

(deftest concurrent-ensure-never-replaces-the-published-database
  (let [store (memory/store)
        dir (temp-dir "sqlite-projection-concurrent")
        tmp-dir (temp-dir "sqlite-projection-concurrent-tmp")
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
                             result)))))
        opts {:event-store store
              :db/dir (str dir)
              :db/tmp-dir (str tmp-dir)
              :projection/version 1
              :projection/register register}]
    (append! store 0 {:event/type :todo/created :todo/id "first" :todo/text "First"})
    (let [slow-build (future (projection/ensure-db-file! (assoc opts :event-store slow-store)))]
      (try
        (is (= true (deref replayed 10000 ::timeout)))
        (is (not (.exists (projection/db-file opts))) "the partial build is not published")
        (let [file (projection/ensure-db-file! opts)
              ds (jdbc/get-datasource (str "jdbc:sqlite:" file))]
          (append! store 1 {:event/type :todo/created :todo/id "second" :todo/text "Second"})
          (projection/catch-up! (assoc opts :db/ds ds))
          (with-open [held (jdbc/get-connection ds)]
            (is (= 2 (todo-count held)))
            (deliver release true)
            (is (= file (deref slow-build 10000 ::timeout)))
            (is (= 1 (last-projected-event-number ds)) "the cursor cannot regress")
            (is (= 2 (todo-count ds)))
            (is (= 2 (todo-count held)) "old and new connections see the same DB")))
        (is (= ["v1.db"] (vec (.list (.toFile dir)))) "no staging files remain")
        (is (empty? (.list (.toFile tmp-dir))) "both build directories are cleaned")
        (finally
          (deliver release true)
          (future-cancel slow-build))))))

(deftest ensure-db-file-does-not-swallow-build-failures
  (try
    (projection/ensure-db-file! {:event-store (memory/store)
                                 :db/dir (str (temp-dir "sqlite-projection-ensure-failure"))
                                 :projection/version 1
                                 :projection/register [{:projection/create #'create-broken}]})
    (is false "expected build failure")
    (catch clojure.lang.ExceptionInfo e
      (is (= :db-build-failed (:error (ex-data e)))))))

(deftest old-db-cleanup-is-caller-owned
  (is (false? (contains? (ns-publics 'simplemono.sqlite-projection)
                         'delete-old-db-files!))))

(deftest an-unstamped-db-with-a-cursor-is-rejected
  ;; The stamp and the cursor are written in one transaction, so a cursor next
  ;; to user_version 0 cannot come out of this library. Adopting such a file
  ;; would project new events onto state of an unknown version.
  (let [store (memory/store)
        ds (temp-ds)
        opts {:event-store store
              :db/ds ds
              :projection/version 1
              :projection/register register}]
    (append! store 0 {:event/type :todo/created
                      :todo/id (random-uuid)
                      :todo/text "Stamped"})
    (projection/catch-up! opts)
    (jdbc/execute! ds ["PRAGMA user_version = 0"])
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"no version stamp"
                          (projection/catch-up! opts)))))

(deftest a-legacy-empty-state-table-is-still-adoptable
  ;; Older releases created the state table before the projection transaction.
  ;; Continue accepting an empty table with no stamp or projected cursor; new
  ;; catch-ups create that table inside the transaction.
  (let [store (memory/store)
        ds (temp-ds)
        id (random-uuid)
        opts {:event-store store
              :db/ds ds
              :projection/version 1
              :projection/register register}]
    (jdbc/execute! ds (sql/format {:create-table [:event_projection_last_event_number
                                                  :if-not-exists]
                                   :with-columns [[:event_number :integer [:primary-key]]]}))
    (append! store 0 {:event/type :todo/created
                      :todo/id id
                      :todo/text "Adopted"})
    (is (nil? (projection/catch-up! opts)))
    (is (= 0 (last-projected-event-number ds)))
    (is (= 1 (stored-projection-version ds)))))

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'simplemono.sqlite-projection-test)]
    (System/exit (if (zero? (+ fail error)) 0 1))))
