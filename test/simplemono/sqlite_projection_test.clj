(ns simplemono.sqlite-projection-test
  (:require [clojure.test :refer [deftest is testing run-tests]]
            [honey.sql :as sql]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [simplemono.event-store :as event-store]
            [simplemono.event-store.memory :as memory]
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

(deftest catch-up-uses-get-event-until-first-miss
  (let [id (random-uuid)
        events {0 {:event/type :todo/created
                   :todo/id id
                   :todo/text "No LIST"}}
        requested (atom [])
        latest-called? (atom false)
        store (reify event-store/EventStore
                (try-append! [_ _ _] (throw (UnsupportedOperationException.)))
                (get-event [_ event-number]
                  (swap! requested conj event-number)
                  (get events event-number))
                (latest-event-number [_]
                  (reset! latest-called? true)
                  (throw (ex-info "latest-event-number should not be called" {}))))
        ds (temp-ds)]
    (is (nil? (projection/catch-up! {:event-store store
                                     :db/ds ds
                                     :projection/version 1
                                     :projection/register register})))
    (is (= [0 1] @requested))
    (is (false? @latest-called?)
        "latest-event-number is a LIST on an object store; catch-up must avoid it")
    (is (= 0 (last-projected-event-number ds)))
    (is (= "No LIST" (:text (todo-row ds id))))))

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
