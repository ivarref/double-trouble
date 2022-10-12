(ns com.github.ivarref.counter-test
  (:require
    [clojure.test :as test :refer [deftest is]]
    [com.github.ivarref.double-trouble :as dt]
    [com.github.ivarref.double-trouble.counter :as counter]
    [com.github.ivarref.gen-fn :as gen-fn]
    [com.github.ivarref.log-init :as log-init]
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
  (let [conn (let [uri (str "datomic:mem://test-" (random-uuid))]
               (d/delete-database uri)
               (d/create-database uri)
               (d/connect uri))]
    (reset! dt/healthy? true)
    @(d/transact conn dt/schema)
    @(d/transact conn test-schema)
    @(d/transact conn [(gen-fn/datomic-fn :dt/counter #'counter/counter)])
    (binding [*conn* conn]
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
