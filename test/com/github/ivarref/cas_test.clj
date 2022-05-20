(ns com.github.ivarref.cas-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [clojure.edn :as edn]
            [com.github.ivarref.log-init :as log-init]
            [datomic.api :as d]
            [com.github.ivarref.double-trouble :as dt]
            [com.github.ivarref.gen-fn :as gen-fn]
            [com.github.ivarref.stacktrace]
            [com.github.ivarref.debug]
            [com.github.ivarref.double-trouble.cas :as cas]
            [clojure.tools.logging :as log]
            [clojure.string :as str]))

(log-init/init-logging!
  [[#{"datomic.*" "com.datomic.*" "org.apache.*"} :warn]
   [#{"*"} (edn/read-string
             (System/getProperty "TAOENSSO_TIMBRE_MIN_LEVEL_EDN" ":info"))]])

(def ^:dynamic *conn* nil)

(def test-schema
  [#:db{:ident :e/id, :cardinality :db.cardinality/one, :valueType :db.type/string :unique :db.unique/identity}
   #:db{:ident :e/identity, :cardinality :db.cardinality/one, :valueType :db.type/string :unique :db.unique/identity}
   #:db{:ident :e/unique-value, :cardinality :db.cardinality/one, :valueType :db.type/string :unique :db.unique/value}
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
      @(d/transact conn dt/schema)
      @(d/transact conn test-schema)
      @(d/transact conn [(gen-fn/datomic-fn :dt/cas #'cas/cas)])
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
  (dt/expand-tx (d/db *conn*) x))

(defn transact [x]
  @(dt/transact *conn* x))

(defn pull [e]
  (dissoc (d/pull (d/db *conn*) [:*] e) :db/id))

(defmacro fail [msg]
  `(is (= 1 0) ~msg))

(deftest test-rejects
  (is (= "Entity cannot be string" (err-msg @(d/transact *conn* [[:dt/cas "string-not-allowed" :e/version nil 1 "some-sha"]]))))
  (is (= "Entity cannot be tempid/datomic.db.DbId" (err-msg @(d/transact *conn* [[:dt/cas (d/tempid :db.part/user) :e/version nil 1 "some-sha"]]))))
  (is (= "Old-val must be nil for new entities" (err-msg @(d/transact *conn* [[:dt/cas [:e/id "a" :as "tempid"] :e/version 2 1 "some-sha"]])))))

(deftest unknown-tempid-should-throw
  (is (= "Could not resolve tempid" (err-msg (dt/resolve-tempids (d/db *conn*) [[:dt/cas "unknown" :e/version nil 1 "some-sha"]
                                                                                {:db/id "tempid" :e/id "a" :e/info "1"}])))))

(deftest resolve-tempid
  (is (= [[:dt/cas [:e/id "a" :as "tempid"] :e/version nil 1 "some-sha"]
          {:db/id "tempid" :e/id "a" :e/info "1"}]
         (dt/resolve-tempids (d/db *conn*) [[:dt/cas "tempid" :e/version nil 1 "some-sha"]
                                            {:db/id "tempid" :e/id "a" :e/info "1"}]))))

(deftest dt-nil-test
  @(d/transact *conn* [{:e/id "a" :e/info "1"}])
  (is (= [[:db/add [:e/id "a"] :e/version 1]
          [:db/add "datomic.tx" :com.github.ivarref.double-trouble/sha-1 "some-sha"]]
         (cas/cas (d/db *conn*) [:e/id "a"] :e/version nil 1 "some-sha"))))


(deftest happy-case
  (let [{:keys [db-after]} @(d/transact *conn* [[:dt/cas [:e/id "a" :as "tempid"] :e/version nil 1 "my-sha"]
                                                {:db/id "tempid" :e/id "a" :e/info "1"}])]
    (is (= #:e{:id "a" :info "1" :version 1} (d/pull db-after [:e/id :e/info :e/version] [:e/id "a"]))))
  (let [{:keys [db-after]} @(d/transact *conn* [[:dt/cas [:e/id "a" :as "tempid"] :e/version 1 2 "my-sha-2"]
                                                {:db/id "tempid" :e/id "a" :e/info "2"}])]
    (is (= #:e{:id "a" :info "2" :version 2} (d/pull db-after [:e/id :e/info :e/version] [:e/id "a"]))))
  (is (= ":db.error/cas-failed Compare failed: 999 2" (err-msg @(d/transact *conn* [[:dt/cas [:e/id "a" :as "tempid"] :e/version 999 3 "my-sha-3"]
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
          [:db/add "tempid" :e/version 1]
          [:db/add "datomic.tx" :com.github.ivarref.double-trouble/sha-1 "my-sha"]]
         (expand
           [{:db/id "tempid", :e/id2 "a", :e/info "1"}
            [:dt/cas [:e/id2 "a" :as "tempid"] :e/version nil 1 "my-sha"]])
         (expand
           [{:db/id "tempid", :e/id2 "a", :e/info "1"}
            [:dt/cas "tempid" :e/version nil 1 "my-sha"]]))))

(deftest ndt-insert-unique-value-behaviour
  (transact [{:db/id "tempid", :e/id2 "a", :e/info "1"}
             [:dt/cas "tempid" :e/version nil 1 "my-sha"]])
  (is (= #:e{:id2 "a", :info "1", :version 1} (pull [:e/id2 "a"])))

  (is (= "Cannot use tempid for existing :db.unique/value entities"
         (err-msg (transact [{:db/id "tempid", :e/id2 "a", :e/info "1"}
                             [:dt/cas "tempid" :e/version nil 1 "my-sha"]]))))

  (transact [[:dt/cas [:e/id2 "a"] :e/version 1 2 "my-sha-2"]])
  (is (= #:e{:id2 "a", :info "1", :version 2} (pull [:e/id2 "a"])))

  (transact [{:db/id [:e/id2 "a"] :e/info "2"}
             [:dt/cas [:e/id2 "a"] :e/version 2 3 "my-sha-3"]])
  (is (= #:e{:id2 "a", :info "2", :version 3} (pull [:e/id2 "a"]))))


(deftest duplicate-sha
  @(dt/transact *conn* [{:db/id "tempid" :e/id "a" :e/info "1"}
                        [:dt/cas "tempid" :e/version nil 1 "my-sha"]])
  (is (true? (str/starts-with?
               (err-msg @(dt/transact *conn* [{:db/id "tempid" :e/id "b" :e/info "2"}
                                              [:dt/cas "tempid" :e/version nil 1 "my-sha"]]))
               ":db.error/unique-conflict Unique conflict: :com.github.ivarref.double-trouble/sha-1"))))

(deftest cas-unique-conflict-error-ordering
  @(d/transact *conn* [#:db{:ident :e/identity, :cardinality :db.cardinality/one, :valueType :db.type/string :unique :db.unique/identity}
                       #:db{:ident :e/unique-value, :cardinality :db.cardinality/one, :valueType :db.type/string :unique :db.unique/value}])
  @(d/transact *conn* [{:e/identity "a" :e/version 1}
                       {:e/unique-value "b" :e/version 2}])
  (try
    @(d/transact *conn* [{:e/unique-value "b" :e/version 3}])
    (fail "Should not get here")
    (catch Exception e
      (is (true?
            (str/starts-with?
              (ex-message e)
              "java.lang.IllegalStateException: :db.error/unique-conflict Unique conflict: :e/unique-value")))))
  (dotimes [_ 1000]
    (try
      @(d/transact *conn* (vec (shuffle [[:db/cas [:e/identity "a"] :e/version 2 1]
                                         {:e/unique-value "b" :e/version 3}])))
      (is (= 1 0))
      (catch Exception e
        (is (= "java.lang.IllegalStateException: :db.error/cas-failed Compare failed: 2 1"
               (ex-message e)))))))

#_(deftest resolved-tempids
    (let [tempids (:tempids (transact [{:db/id "tempid", :e/id "a", :e/info "1"}
                                       [:nmdt/cas "tempid" :e/version nil 1]]))]
      (is (= tempids (:tempids (transact [{:db/id "tempid", :e/id "a", :e/info "1"}
                                          [:nmdt/cas "tempid" :e/version nil 1]]))))))



(deftest transacted?
  (is (true? (:transacted? (transact [{:db/id "tempid", :e/id "a", :e/info "1"}
                                      [:dt/cas "tempid" :e/version nil 1 "sha-1"]]))))

  (is (false? (:transacted? (transact [{:db/id "tempid", :e/id "a", :e/info "1"}
                                       [:dt/cas "tempid" :e/version nil 1 "sha-1"]]))))

  (is (true? (:transacted? (transact [{:e/id "a" :e/info "2"}
                                      [:dt/cas [:e/id "a"] :e/version 1 2 "sha-2"]]))))

  (is (= [#:e{:id "a", :info "2"}
          [:dt/cas [:e/id "a"] :e/version 1 2 "sha-2"]]
         (dt/resolve-tempids (d/db *conn*)
                             [{:e/id "a" :e/info "2"}
                              [:dt/cas [:e/id "a"] :e/version 1 2 "sha-2"]])))

  (try
    @(d/transact *conn* [{:e/id "a" :e/info "2"}
                         [:dt/cas [:e/id "a"] :e/version 1 2 "sha-2"]])
    (fail "Should not get here")
    (catch Exception e
      (if (dt/already-transacted? e)
        (is (= 1 1))
        (fail "Should not get here"))))

  #_(is (= 2 (:v (transact [{:e/id "a" :e/info "2"}
                            [:dt/cas [:e/id "a"] :e/version 1 2 "sha-2"]])))))
