(ns com.github.ivarref.sac-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [clojure.tools.logging :as log]
            [com.github.ivarref.double-trouble :as dt]
            [com.github.ivarref.double-trouble.sac :as sac]
            [com.github.ivarref.gen-fn :as gen-fn]
            [com.github.ivarref.log-init :as log-init]
            [datomic.api :as d]))

(require '[com.github.ivarref.debug])

(log-init/init-logging!
  [[#{"datomic.*" "com.datomic.*" "org.apache.*"} :warn]
   [#{"*"} :info]])

(def ^:dynamic *conn* nil)
(defonce c (atom nil))

(defn conn []
  (or *conn* @c))

(defn db []
  (d/db (conn)))

(def test-schema
  [#:db{:ident :e/id, :cardinality :db.cardinality/one, :valueType :db.type/string :unique :db.unique/identity}
   #:db{:ident :e/status, :cardinality :db.cardinality/one, :valueType :db.type/ref}
   #:db{:ident :e/status-kw, :cardinality :db.cardinality/one, :valueType :db.type/keyword}
   #:db{:ident :Status/INIT}
   #:db{:ident :Status/PROCESSING}
   #:db{:ident :Status/DONE}])

(defn with-new-conn [f]
  (let [conn (let [uri (str "datomic:mem://test-" (random-uuid))]
               (d/delete-database uri)
               (d/create-database uri)
               (d/connect uri))]
    (try
      (reset! dt/healthy? true)
      @(d/transact conn dt/schema)
      @(d/transact conn test-schema)
      @(d/transact conn [(gen-fn/datomic-fn :dt/sac #'sac/sac)])
      (reset! c conn)
      (binding [*conn* conn]
        (f))
      (finally
        #_(d/release conn)))))

(use-fixtures :each with-new-conn)

(defn root-cause [e]
  (if-let [root (ex-cause e)]
    (root-cause root)
    e))

(defmacro err-code [& body]
  `(try
     (do ~@body)
     (log/error "No error message")
     nil
     (catch Exception e#
       (:com.github.ivarref.double-trouble/code (ex-data (root-cause e#))))))

(defn is-ref? [db attr]
  (= :db.type/ref
     (d/q '[:find ?ident .
            :in $ ?e
            :where
            [?e :db/valueType ?typ]
            [?typ :db/ident ?ident]]
          db
          attr)))

(defn get-val [db e a]
  (let [v (d/q '[:find ?v .
                 :in $ ?e ?a
                 :where
                 [?e ?a ?v]]
               db
               e
               a)]
    (if (is-ref? db a)
      (get-val db v :db/ident)
      v)))

(deftest sac-not-found
  (is (= :could-not-find-entity (err-code @(d/transact *conn* [[:dt/sac [:e/id "a"] :e/status :Status/INIT]])))))

(deftest unknown-attr
  (is (= :could-not-find-attr (err-code @(d/transact *conn* [[:dt/sac [:e/id "a"] :e/missing :Status/INIT]])))))

(deftest unknown-ref-value
  @(d/transact *conn* [{:e/id "a" :e/status :Status/INIT}])
  (is (= :could-not-find-ref-value (err-code @(d/transact *conn* [[:dt/sac [:e/id "a"] :e/status :Status/Missing]])))))

(deftest nil->nil
  @(d/transact *conn* [{:e/id "a"}])
  (is (= :nil-not-supported (err-code @(d/transact *conn* [[:dt/sac [:e/id "a"] :e/status nil]])))))

(deftest nil->nil2
  @(d/transact *conn* [{:e/id "a" :e/status :Status/INIT}])
  (is (= :nil-not-supported (err-code @(d/transact *conn* [[:dt/sac [:e/id "a"] :e/status nil]])))))

(deftest same-value-cancels
  @(d/transact *conn* [{:e/id "a" :e/status :Status/INIT}])
  (is (= :no-change (err-code @(d/transact *conn* [[:dt/sac [:e/id "a"] :e/status :Status/INIT]])))))

(deftest same-value-kw-cancels
  @(d/transact *conn* [{:e/id "a" :e/status-kw :Status/INIT}])
  (is (= :no-change (err-code @(d/transact *conn* [[:dt/sac [:e/id "a"] :e/status-kw :Status/INIT]])))))

(deftest no-previous-value
  @(d/transact *conn* [{:e/id "a"}])
  @(d/transact *conn* [[:dt/sac [:e/id "a"] :e/status :Status/INIT]])
  (is (= :Status/INIT (get-val (db) [:e/id "a"] :e/status))))

(deftest new-value
  @(d/transact *conn* [{:e/id "a" :e/status :Status/INIT}])
  @(d/transact *conn* [[:dt/sac [:e/id "a"] :e/status :Status/PROCESSING]])
  (is (= :Status/PROCESSING (get-val (db) [:e/id "a"] :e/status))))

(deftest new-kw-value
  @(d/transact *conn* [{:e/id "a" :e/status-kw :Status/INIT}])
  @(d/transact *conn* [[:dt/sac [:e/id "a"] :e/status-kw :Status/PROCESSING]])
  (is (= :Status/PROCESSING (get-val (db) [:e/id "a"] :e/status-kw))))