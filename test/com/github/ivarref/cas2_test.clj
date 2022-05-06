(ns com.github.ivarref.cas2-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [clojure.edn :as edn]
            [com.github.ivarref.log-init :as log-init]
            [datomic.api :as d]
            [com.github.ivarref.no-double-trouble :as ndt]
            [com.github.ivarref.dbfns.generate-fn :as gen-fn]
            [com.github.ivarref.stacktrace]
            [com.github.ivarref.debug]
            [clojure.tools.logging :as log]))

(log-init/init-logging!
  [[#{"datomic.*" "com.datomic.*" "org.apache.*"} :warn]
   [#{"*"} (edn/read-string
             (System/getProperty "TAOENSSO_TIMBRE_MIN_LEVEL_EDN" ":info"))]])

(def ^:dynamic *conn* nil)

(def test-schema
  [#:db{:ident :e/id, :cardinality :db.cardinality/one, :valueType :db.type/string :unique :db.unique/identity}
   #:db{:ident :e/sha, :cardinality :db.cardinality/one, :valueType :db.type/string}
   #:db{:ident :e/version, :cardinality :db.cardinality/one, :valueType :db.type/long}
   #:db{:ident :e/version-str, :cardinality :db.cardinality/one, :valueType :db.type/string}
   #:db{:ident :e/version-uuid, :cardinality :db.cardinality/one, :valueType :db.type/uuid}
   #:db{:ident :e/info, :cardinality :db.cardinality/one, :valueType :db.type/string}
   #:db{:ident :e/modified, :cardinality :db.cardinality/one, :valueType :db.type/instant}])

(defn with-new-conn [f]
  (let [conn (let [uri (str "datomic:mem://test-" (random-uuid))]
               (d/delete-database uri)
               (d/create-database uri)
               (d/connect uri))]
    (try
      @(d/transact conn ndt/schema)
      @(d/transact conn test-schema)
      @(d/transact conn [(gen-fn/generate-function 'com.github.ivarref.no-double-trouble.dbfns.cas2/cas2 :ndt/cas2 false)])
      (binding [*conn* conn]
        (f))
      (finally
        (d/release conn)))))

(use-fixtures :each with-new-conn)

(defn root-cause [e]
  (if-let [root (ex-cause e)]
    (root-cause root)
    e))

(defmacro err-msg [& body]
  `(try
     (do ~@body)
     (log/error "No error message")
     nil
     (catch Exception e#
       (ex-message (root-cause e#)))))

(deftest test-rejects
  (is (= "Entity cannot be string" (err-msg @(d/transact *conn* [[:ndt/cas2 "string-not-allowed" :e/version nil 1]]))))
  (is (= "Entity cannot be tempid/datomic.db.DbId" (err-msg @(d/transact *conn* [[:ndt/cas2 (d/tempid :db.part/user) :e/version nil 1]]))))
  (is (= "Old-val must be nil for new entities" (err-msg @(d/transact *conn* [[:ndt/cas2 [:e/id "a" :as "tempid"] :e/version 2 1]])))))

(deftest happy-case
  (let [{:keys [db-after]} @(d/transact *conn* [[:ndt/cas2 [:e/id "a" :as "tempid"] :e/version nil 1]
                                                {:db/id "tempid" :e/id "a" :e/info "1"}])]
    (is (= #:e{:id "a" :info "1" :version 1} (d/pull db-after [:e/id :e/info :e/version] [:e/id "a"]))))
  (let [{:keys [db-after]} @(d/transact *conn* [[:ndt/cas2 [:e/id "a" :as "tempid"] :e/version 1 2]
                                                {:db/id "tempid" :e/id "a" :e/info "2"}])]
    (is (= #:e{:id "a" :info "2" :version 2} (d/pull db-after [:e/id :e/info :e/version] [:e/id "a"]))))

  (is (= ":db.error/cas-failed Compare failed: 999 2" (err-msg @(d/transact *conn* [[:ndt/cas2 [:e/id "a" :as "tempid"] :e/version 999 3]
                                                                                    {:db/id "tempid" :e/id "a" :e/info "2"}])))))

