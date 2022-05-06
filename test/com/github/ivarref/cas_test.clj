(ns com.github.ivarref.cas-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [clojure.edn :as edn]
            [com.github.ivarref.log-init :as log-init]
            [datomic.api :as d]
            [com.github.ivarref.no-double-trouble :as ndt]
            [com.github.ivarref.dbfns.generate-fn :as gen-fn]
            [com.github.ivarref.stacktrace]
            [com.github.ivarref.debug]
            [com.github.ivarref.no-double-trouble.dbfns.cas :as cas]
            [clojure.tools.logging :as log]))

(log-init/init-logging!
  [[#{"datomic.*" "com.datomic.*" "org.apache.*"} :warn]
   [#{"*"} (edn/read-string
             (System/getProperty "TAOENSSO_TIMBRE_MIN_LEVEL_EDN" ":info"))]])

(def ^:dynamic *conn* nil)

(def test-schema
  [#:db{:ident :e/id, :cardinality :db.cardinality/one, :valueType :db.type/string :unique :db.unique/identity}
   #:db{:ident :e/id2, :cardinality :db.cardinality/one, :valueType :db.type/string :unique :db.unique/value}
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
      @(d/transact conn [(gen-fn/generate-function 'com.github.ivarref.no-double-trouble.dbfns.cas/cas :ndt/cas false)])
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
  (is (= "Entity cannot be string" (err-msg @(d/transact *conn* [[:ndt/cas "string-not-allowed" :e/version nil 1]]))))
  (is (= "Entity cannot be tempid/datomic.db.DbId" (err-msg @(d/transact *conn* [[:ndt/cas (d/tempid :db.part/user) :e/version nil 1]]))))
  (is (= "Old-val must be nil for new entities" (err-msg @(d/transact *conn* [[:ndt/cas [:e/id "a" :as "tempid"] :e/version 2 1]])))))

(deftest unknown-tempid-should-throw
  (is (= "Could not resolve tempid" (err-msg (ndt/resolve-tempids (d/db *conn*) [[:ndt/cas "unknown" :e/version nil 1]
                                                                                 {:db/id "tempid" :e/id "a" :e/info "1"}])))))

(deftest resolve-tempid
  (is (= [[:ndt/cas [:e/id "a" :as "tempid"] :e/version nil 1]
          {:db/id "tempid" :e/id "a" :e/info "1"}]
         (ndt/resolve-tempids (d/db *conn*) [[:ndt/cas "tempid" :e/version nil 1]
                                             {:db/id "tempid" :e/id "a" :e/info "1"}]))))

(deftest nil-test
  @(d/transact *conn* [{:e/id "a" :e/info "1"}])
  @(d/transact *conn* [[:db/cas [:e/id "a"] :e/version nil 1]]))


(deftest ndt-nil-test
  @(d/transact *conn* [{:e/id "a" :e/info "1"}])
  (is (= [[:db/cas [:e/id "a"] :e/version nil 1]]
         (cas/cas (d/db *conn*) [:e/id "a"] :e/version nil 1))))


(deftest nil-test-2
  @(d/transact *conn* [{:e/id "a" :e/info "1"}])
  (is (= ":db.error/cas-failed Compare failed: 2 " (err-msg @(d/transact *conn* [[:db/cas [:e/id "a"] :e/version 2 1]])))))


(deftest happy-case
  (let [{:keys [db-after]} @(d/transact *conn* [[:ndt/cas [:e/id "a" :as "tempid"] :e/version nil 1]
                                                {:db/id "tempid" :e/id "a" :e/info "1"}])]
    (is (= #:e{:id "a" :info "1" :version 1} (d/pull db-after [:e/id :e/info :e/version] [:e/id "a"]))))
  (let [{:keys [db-after]} @(d/transact *conn* [[:ndt/cas [:e/id "a" :as "tempid"] :e/version 1 2]
                                                {:db/id "tempid" :e/id "a" :e/info "2"}])]
    (is (= #:e{:id "a" :info "2" :version 2} (d/pull db-after [:e/id :e/info :e/version] [:e/id "a"]))))

  (is (= ":db.error/cas-failed Compare failed: 999 2" (err-msg @(d/transact *conn* [[:ndt/cas [:e/id "a" :as "tempid"] :e/version 999 3]
                                                                                    {:db/id "tempid" :e/id "a" :e/info "2"}])))))


(deftest datomic-insert-unique-value-behaviour
  (let [{:keys [db-after]} @(d/transact *conn* [{:db/id "tempid" :e/id2 "a" :e/info "1"}
                                                [:db/add "tempid" :e/version 1]])]
    (is (= #:e{:id2 "a", :info "1", :version 1}
           (d/pull db-after [:e/id2 :e/info :e/version] [:e/id2 "a"]))))

  @(d/transact *conn* [[:db/cas [:e/id2 "a"] :e/version 1 2]])
  (is (= #:e{:id2 "a", :info "1", :version 2} (d/pull (d/db *conn*) [:e/id2 :e/info :e/version] [:e/id2 "a"])))

  @(d/transact *conn* [{:db/id [:e/id2 "a"] :e/info "2"}
                       [:db/cas [:e/id2 "a"] :e/version 2 3]])

  (is (= #:e{:id2 "a", :info "2", :version 3} (d/pull (d/db *conn*) [:e/id2 :e/info :e/version] [:e/id2 "a"]))))

(defn expand [x]
  (ndt/expand-tx (d/db *conn*) x))

(defn transact [x]
  @(ndt/transact *conn* (ndt/sha x) x))

(defn pull [e]
  (dissoc (d/pull (d/db *conn*) [:*] e)
          :db/id))

(deftest tx-translations
  (is (= [{:db/id "tempid", :e/id2 "a", :e/info "1"}
          [:db/add "tempid" :e/version 1]]
         (expand
           [{:db/id "tempid", :e/id2 "a", :e/info "1"}
            [:ndt/cas [:e/id2 "a" :as "tempid"] :e/version nil 1]])
         (expand
           [{:db/id "tempid", :e/id2 "a", :e/info "1"}
            [:ndt/cas "tempid" :e/version nil 1]]))))

(deftest ndt-insert-unique-value-behaviour
  (transact [{:db/id "tempid", :e/id2 "a", :e/info "1"}
             [:ndt/cas "tempid" :e/version nil 1]])
  (is (= #:e{:id2 "a", :info "1", :version 1} (pull [:e/id2 "a"])))

  #_(transact [{:db/id "tempid", :e/id2 "a", :e/info "1"}
               [:ndt/cas "tempid" :e/version nil 1]])

  #_(let [{:keys [db-after]} @(ndt/transact *conn* (ndt/sha "demo"))]
      (is (=  (d/pull db-after [:e/id2 :e/info :e/version] [:e/id2 "a"]))))

  #_(is (= [[:db/cas [:e/id2 "a"] :e/version 1 2]]
           (ndt/expand-tx (d/db *conn*)
                          [[:ndt/cas [:e/id2 "a"] :e/version 1 2]])))

  #_@(ndt/transact *conn* (ndt/sha "demo") [[:ndt/cas [:e/id2 "a"] :e/version 1 2]])
  #_(is (= #:e{:id2 "a", :info "1", :version 2} (d/pull (d/db *conn*) [:e/id2 :e/info :e/version] [:e/id2 "a"]))))
