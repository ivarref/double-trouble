(ns com.github.ivarref.initial-test
  (:require [clojure.test :as test :refer [deftest is]]
            [com.github.ivarref.no-double-trouble :as ndt]
            [datomic.api :as d])
  (:import (java.util Date)))


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

(deftest nil-test-with-explicit-tempid
  (let [temp-id (d/tempid :db.part/user)
        {:keys [db-after]} @(d/transact *conn* [[:db/cas temp-id :e/version nil 1]
                                                {:db/id temp-id :e/id "a" :e/info "asdf"}])]
    (is (= #:e{:version 1} (d/pull db-after [:e/version] [:e/id "a"])))))
