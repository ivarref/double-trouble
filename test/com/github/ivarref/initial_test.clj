(ns com.github.ivarref.initial-test
  (:require [clojure.test :as test :refer [deftest is]]
            [com.github.ivarref.no-double-trouble :as ndt]
            [com.github.ivarref.log-init :as log-init]
            [datomic.api :as d]
            [clojure.edn :as edn]))

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
      (binding [*conn* conn]
        (f))
      (finally
        (d/release conn)))))

(test/use-fixtures :each with-new-conn)

(deftest rewrite-cas-str-test
  (is (= [[:db/cas "TEMPID" :e/version nil 1]
          {:db/id "TEMPID", :e/id "a", :e/info "asdf"}
          [:db/add "TEMPID" :e/asdf "a"]]
         (ndt/rewrite-cas-str [[:db/cas "a" :e/version nil 1]
                               {:db/id "a" :e/id "a" :e/info "asdf"}
                               [:db/add "a" :e/asdf "a"]]
                              "TEMPID"))))

(deftest nil-test
  (let [{:keys [db-after]} @(d/transact *conn* (ndt/rewrite-cas-str [[:db/cas "new" :e/version nil 1]
                                                                     {:db/id "new" :e/id "a" :e/info "asdf"}]))]
    (is (= #:e{:version 1 :info "asdf"} (d/pull db-after [:e/version :e/info] [:e/id "a"])))))
