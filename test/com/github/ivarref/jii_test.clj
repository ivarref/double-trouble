(ns com.github.ivarref.jii-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [clojure.tools.logging :as log]
            [com.github.ivarref.gen-fn :as gen-fn]
            [com.github.ivarref.double-trouble :as dt]
            [com.github.ivarref.double-trouble.jii :as jii]
            [datomic.api :as d]
            [com.github.ivarref.log-init :as log-init]))

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
   #:db{:ident :e/version, :cardinality :db.cardinality/one, :valueType :db.type/long}
   #:db{:ident :e/version-str, :cardinality :db.cardinality/one, :valueType :db.type/string}])

(defn with-new-conn [f]
  (let [conn (let [uri (str "datomic:mem://test-" (random-uuid))]
               (d/delete-database uri)
               (d/create-database uri)
               (d/connect uri))]
    (try
      @(d/transact conn dt/schema)
      @(d/transact conn test-schema)
      @(d/transact conn [(gen-fn/datomic-fn :dt/jii #'jii/jii)])
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
     (let [res# (do ~@body)]
       (if (true? (some->> res# meta :dt/error-map?))
         (:com.github.ivarref.double-trouble/code res#)
         (do
           (log/error "No error message")
           (is (= 1 2 "No error code"))
           nil)))
     (catch Exception e#
       (let [ex-data# (ex-data (root-cause e#))]
         (or (:com.github.ivarref.double-trouble/code ex-data#)
             (:db/error ex-data#))))))

(defn get-val [db e a]
  (d/q '[:find ?v .
         :in $ ?e ?a
         :where
         [?e ?a ?v]]
       db
       e
       a))

(deftest unknown-attr
  (is (= :could-not-find-attr (err-code @(d/transact *conn* [[:dt/jii [:e/id "a"] :e/asdf]])))))

(deftest jii-not-found
  (is (= :could-not-find-entity (err-code @(d/transact *conn* [[:dt/jii [:e/id "a"] :e/version]])))))

(deftest nil-not-supported
  @(d/transact *conn* [{:e/id "a"}])
  (is (= :nil-not-supported (err-code @(d/transact *conn* [[:dt/jii [:e/id "a"] :e/version]])))))

(deftest must-be-int
  @(d/transact *conn* [{:e/id "a" :e/version-str "asdf"}])
  (is (= :must-be-int (err-code @(d/transact *conn* [[:dt/jii [:e/id "a"] :e/version-str]])))))

(deftest happy-case
  @(d/transact *conn* [{:e/id "a" :e/version 1}])
  @(d/transact *conn* [[:dt/jii [:e/id "a"] :e/version]])
  (is (= 2 (get-val (db) [:e/id "a"] :e/version))))

