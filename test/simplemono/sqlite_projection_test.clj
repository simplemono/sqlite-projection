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
        boom (fn [_event] (throw (ex-info "projection blew up" {})))
        opts {:event-store store
              :db/ds ds
              :projection/version 1
              :projection/register register}]
    (append! store 0 {:event/type :todo/created
                      :todo/id id
                      :todo/text "First"})
    (projection/catch-up! opts)
    (append! store 1 {:event/type :todo/created
                      :todo/id (random-uuid)
                      :todo/text "Second"})
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"projection blew up"
                          (projection/catch-up!
                           (assoc opts :projection/register
                                  [{:projection/create #'create-todos}
                                   {:projection/event-type :todo/created
                                    :projection/fn boom}]))))
    (is (= 0 (last-projected-event-number ds))
        "the transaction rolled back, so the cursor still points at event 0")
    (is (= 1 (count (jdbc/execute! ds (sql/format {:select [:*] :from [:todos]}))))
        "and the failed run's rows rolled back with it")))

(deftest no-in-place-replay-is-needed-for-rebuilds
  (is (false? (contains? (ns-publics 'simplemono.sqlite-projection)
                         'replay!))))

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

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'simplemono.sqlite-projection-test)]
    (System/exit (if (zero? (+ fail error)) 0 1))))
