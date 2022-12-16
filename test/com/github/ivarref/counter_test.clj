(ns com.github.ivarref.counter-test
  (:require
    [clojure.test :as test :refer [deftest is]]
    [com.github.ivarref.double-trouble :as dt]
    [com.github.ivarref.double-trouble.counter :as counter]
    [com.github.ivarref.double-trouble.counter-str :as counter-str]
    [com.github.ivarref.gen-fn :as gen-fn]
    [com.github.ivarref.log-init :as log-init]
    [com.github.sikt-no.datomic-testcontainers :as dtc]
    [datomic.api :as d]))

(require '[com.github.ivarref.debug])

(log-init/init-logging!
  [[#{"datomic.*" "com.datomic.*" "org.apache.*"} :warn]
   [#{"*"} :info]])

(def ^:dynamic *conn* nil)

(def test-schema
  [#:db{:ident :e/id, :cardinality :db.cardinality/one, :valueType :db.type/string :unique :db.unique/identity}
   #:db{:ident :e/id-long, :cardinality :db.cardinality/one, :valueType :db.type/long :unique :db.unique/identity}
   #:db{:ident :e/version, :cardinality :db.cardinality/one, :valueType :db.type/long}
   #:db{:ident :e/info, :cardinality :db.cardinality/one, :valueType :db.type/string}
   #:db{:ident :e/sha, :cardinality :db.cardinality/one, :valueType :db.type/string :index true}
   #:db{:ident :e/status, :cardinality :db.cardinality/one, :valueType :db.type/ref}
   #:db{:ident :e/status-kw, :cardinality :db.cardinality/one, :valueType :db.type/keyword}
   #:db{:ident :Status/INIT}
   #:db{:ident :Status/PROCESSING}])

(defn with-new-conn [f]
  (let [in-mem-conn (let [uri (str "datomic:mem://test-" (random-uuid))]
                      (d/delete-database uri)
                      (d/create-database uri)
                      (d/connect uri))
        remote-conn (dtc/get-conn {:delete? true})
        tx! (fn [tx-data]
              @(d/transact in-mem-conn tx-data)
              @(d/transact remote-conn tx-data))]
    (reset! dt/healthy? true)
    (tx! dt/schema)
    (tx! test-schema)
    (tx! [(gen-fn/datomic-fn :dt/counter #'counter/counter)])
    (tx! [(gen-fn/datomic-fn :dt/counter-str #'counter-str/counter-str)])
    (binding [*conn* in-mem-conn]
      (f))
    (binding [*conn* remote-conn]
      (f))))

(test/use-fixtures :each with-new-conn)

(deftest counter-test
  (let [tx [[:dt/counter "some-counter" "a" :e/id-long]
            {:db/id "a" :e/info "janei"}]
        {:strs [a]} (:tempids @(d/transact *conn* tx))
        res (select-keys (d/pull (d/db *conn*) [:*] a) [:e/id-long :e/info])]
    (is (= #:e{:id-long 1 :info "janei"} res)))
  (let [tx [[:dt/counter "some-counter" "a" :e/id-long]
            {:db/id "a" :e/info "janei2"}]
        {:strs [a]} (:tempids @(d/transact *conn* tx))
        res (select-keys (d/pull (d/db *conn*) [:*] a) [:e/id-long :e/info])]
    (is (= #:e{:id-long 2 :info "janei2"} res)))
  (let [tx [[:dt/counter "some-counter-2" "a" :e/id-long]
            {:db/id "a" :e/info "janei3"}]
        {:strs [a]} (:tempids @(d/transact *conn* tx))
        res (select-keys (d/pull (d/db *conn*) [:*] a) [:e/id-long :e/info])]
    (is (= #:e{:id-long 1 :info "janei3"} res)))
  (let [tx [[:dt/counter "some-counter" "a" :e/id-long]
            {:db/id "a" :e/info "janei4"}]
        {:strs [a]} (:tempids @(d/transact *conn* tx))
        res (select-keys (d/pull (d/db *conn*) [:*] a) [:e/id-long :e/info])]
    (is (= #:e{:id-long 3 :info "janei4"} res))))

(deftest counter-tempid-test
  (dt/ensure-partition! *conn* :counters)
  (let [tmpid (d/tempid :counters)
        tx [[:dt/counter "some-counter" tmpid :e/id-long]
            {:db/id tmpid :e/info "janei"}]
        {:keys [db-after] :as tx-result} @(d/transact *conn* tx)]
    (is (= [:e/id-long 1] (dt/resolve-lookup-ref tx-result :e/id-long)))
    (is (= #:e{:id-long 1 :info "janei"} (d/pull db-after [:e/id-long :e/info] (dt/resolve-lookup-ref tx-result :e/id-long)))))
  (let [tmpid (d/tempid :counters)
        tx [[:dt/counter "some-counter" tmpid :e/id-long]
            {:db/id tmpid :e/info "janei2"}]
        {:keys [db-after] :as tx-result} @(d/transact *conn* tx)]
    (is (= #:e{:id-long 2 :info "janei2"} (d/pull db-after [:e/id-long :e/info] (dt/resolve-lookup-ref tx-result :e/id-long))))))

(deftest counter-str-tempid-test
  (dt/ensure-partition! *conn* :counters)
  (let [tmpid (d/tempid :counters)
        tx [[:dt/counter-str "some-counter" tmpid :e/id]
            {:db/id tmpid :e/info "janei"}]
        {:keys [db-after] :as tx-result} @(d/transact *conn* tx)]
    (is (= [:e/id "1"] (dt/resolve-lookup-ref tx-result :e/id)))
    (is (= #:e{:id "1" :info "janei"} (d/pull db-after [:e/id :e/info] (dt/resolve-lookup-ref tx-result :e/id)))))
  (let [tmpid (d/tempid :counters)
        tx [[:dt/counter-str "some-counter" tmpid :e/id]
            {:db/id tmpid :e/info "janei2"}]
        {:keys [db-after] :as tx-result} @(d/transact *conn* tx)]
    (is (= #:e{:id "2" :info "janei2"} (d/pull db-after [:e/id :e/info] (dt/resolve-lookup-ref tx-result :e/id))))))
