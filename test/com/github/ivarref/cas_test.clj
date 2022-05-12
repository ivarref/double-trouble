(ns com.github.ivarref.cas-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [clojure.edn :as edn]
            [com.github.ivarref.log-init :as log-init]
            [datomic.api :as d]
            [com.github.ivarref.no-more-double-trouble :as nmdt]
            [com.github.ivarref.dbfns.generate-fn :as gen-fn]
            [com.github.ivarref.stacktrace]
            [com.github.ivarref.debug]
            [com.github.ivarref.no-more-double-trouble.dbfns.cas :as cas]
            [clojure.tools.logging :as log]
            [clojure.string :as str]))

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
      @(d/transact conn nmdt/schema)
      @(d/transact conn test-schema)
      @(d/transact conn [(gen-fn/generate-function 'com.github.ivarref.no-more-double-trouble.dbfns.cas/cas :nmdt/cas false)])
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

(defn expand [x]
  (nmdt/expand-tx (d/db *conn*) x))

(defn transact [x]
  @(nmdt/transact *conn* (nmdt/sha x) x))

(defn pull [e]
  (dissoc (d/pull (d/db *conn*) [:*] e) :db/id))


(deftest test-rejects
  (is (= "Entity cannot be string" (err-msg @(d/transact *conn* [[:nmdt/cas "string-not-allowed" :e/version nil 1]]))))
  (is (= "Entity cannot be tempid/datomic.db.DbId" (err-msg @(d/transact *conn* [[:nmdt/cas (d/tempid :db.part/user) :e/version nil 1]]))))
  (is (= "Old-val must be nil for new entities" (err-msg @(d/transact *conn* [[:nmdt/cas [:e/id "a" :as "tempid"] :e/version 2 1]])))))

(deftest unknown-tempid-should-throw
  (is (= "Could not resolve tempid" (err-msg (nmdt/resolve-tempids (d/db *conn*) [[:nmdt/cas "unknown" :e/version nil 1]
                                                                                  {:db/id "tempid" :e/id "a" :e/info "1"}])))))

(deftest resolve-tempid
  (is (= [[:nmdt/cas [:e/id "a" :as "tempid"] :e/version nil 1]
          {:db/id "tempid" :e/id "a" :e/info "1"}]
         (nmdt/resolve-tempids (d/db *conn*) [[:nmdt/cas "tempid" :e/version nil 1]
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
  (let [{:keys [db-after]} @(d/transact *conn* [[:nmdt/cas [:e/id "a" :as "tempid"] :e/version nil 1]
                                                {:db/id "tempid" :e/id "a" :e/info "1"}])]
    (is (= #:e{:id "a" :info "1" :version 1} (d/pull db-after [:e/id :e/info :e/version] [:e/id "a"]))))
  (let [{:keys [db-after]} @(d/transact *conn* [[:nmdt/cas [:e/id "a" :as "tempid"] :e/version 1 2]
                                                {:db/id "tempid" :e/id "a" :e/info "2"}])]
    (is (= #:e{:id "a" :info "2" :version 2} (d/pull db-after [:e/id :e/info :e/version] [:e/id "a"]))))

  (is (= ":db.error/cas-failed Compare failed: 999 2" (err-msg @(d/transact *conn* [[:nmdt/cas [:e/id "a" :as "tempid"] :e/version 999 3]
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

(deftest tx-translations
  (is (= [{:db/id "tempid", :e/id2 "a", :e/info "1"}
          [:db/add "tempid" :e/version 1]]
         (expand
           [{:db/id "tempid", :e/id2 "a", :e/info "1"}
            [:nmdt/cas [:e/id2 "a" :as "tempid"] :e/version nil 1]])
         (expand
           [{:db/id "tempid", :e/id2 "a", :e/info "1"}
            [:nmdt/cas "tempid" :e/version nil 1]]))))

(deftest ndt-insert-unique-value-behaviour
  (transact [{:db/id "tempid", :e/id2 "a", :e/info "1"}
             [:nmdt/cas "tempid" :e/version nil 1]])
  (is (= #:e{:id2 "a", :info "1", :version 1} (pull [:e/id2 "a"])))

  (is (= "Cannot use tempid for existing :db.unique/value entities"
         (err-msg (transact [{:db/id "tempid", :e/id2 "a", :e/info "1"}
                             [:nmdt/cas "tempid" :e/version nil 1]]))))

  (transact [[:nmdt/cas [:e/id2 "a"] :e/version 1 2]])
  (is (= #:e{:id2 "a", :info "1", :version 2} (pull [:e/id2 "a"])))

  (transact [{:db/id [:e/id2 "a"] :e/info "2"}
             [:nmdt/cas [:e/id2 "a"] :e/version 2 3]])
  (is (= #:e{:id2 "a", :info "2", :version 3} (pull [:e/id2 "a"]))))


(deftest duplicate-sha
  @(nmdt/transact *conn* (nmdt/sha "hello") [{:db/id "tempid" :e/id "a" :e/info "1"}
                                             [:nmdt/cas "tempid" :e/version nil 1]])
  (is (true? (str/starts-with?
               (err-msg @(nmdt/transact *conn* (nmdt/sha "hello") [{:db/id "tempid" :e/id "b" :e/info "2"}
                                                                   [:nmdt/cas "tempid" :e/version nil 1]]))
               ":db.error/unique-conflict Unique conflict: :com.github.ivarref.no-more-double-trouble/sha-1"))))


#_(deftest resolved-tempids
    (let [tempids (:tempids (transact [{:db/id "tempid", :e/id "a", :e/info "1"}
                                       [:nmdt/cas "tempid" :e/version nil 1]]))]
      (is (= tempids (:tempids (transact [{:db/id "tempid", :e/id "a", :e/info "1"}
                                          [:nmdt/cas "tempid" :e/version nil 1]]))))))



(deftest transacted?
  (is (true? (:transacted? (transact [{:db/id "tempid", :e/id "a", :e/info "1"}
                                      [:nmdt/cas "tempid" :e/version nil 1]]))))

  (is (false? (:transacted? (transact [{:db/id "tempid", :e/id "a", :e/info "1"}
                                       [:nmdt/cas "tempid" :e/version nil 1]]))))

  (is (true? (:transacted? (transact [{:e/id "a" :e/info "2"}
                                      [:nmdt/cas [:e/id "a"] :e/version 1 2]]))))

  (is (false? (:transacted? (transact [{:e/id "a" :e/info "2"}
                                       [:nmdt/cas [:e/id "a"] :e/version 1 2]]))))

  (is (= 2 (:v (transact [{:e/id "a" :e/info "2"}
                          [:nmdt/cas [:e/id "a"] :e/version 1 2]])))))
