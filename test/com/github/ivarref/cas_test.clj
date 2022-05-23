(ns com.github.ivarref.cas-test
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.test :refer [deftest is use-fixtures]]
            [clojure.tools.logging :as log]
            [com.github.ivarref.double-trouble :as dt]
            [com.github.ivarref.double-trouble.cas :as cas]
            [com.github.ivarref.gen-fn :as gen-fn]
            [com.github.ivarref.log-init :as log-init]
            [com.github.ivarref.stacktrace]
            [datomic.api :as d]))

(require '[com.github.ivarref.debug])

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
   #:db{:ident :e/ref, :cardinality :db.cardinality/one, :valueType :db.type/ref}
   #:db{:ident :e/enum-1}
   #:db{:ident :e/enum-2}
   #:db{:ident :e/enum-3}
   #:db{:ident :e/modified, :cardinality :db.cardinality/one, :valueType :db.type/instant}])

(defn with-new-conn [f]
  (let [conn (let [uri (str "datomic:mem://test-" (random-uuid))]
               (d/delete-database uri)
               (d/create-database uri)
               (d/connect uri))]
    (try
      (reset! dt/healthy? true)
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
  (let [db (d/db *conn*)
        full-tx (dt/resolve-tempids db x)]
    (vec (mapcat (fn [tx]
                   (if (and (vector? tx) (= :dt/cas (first tx)))
                     (apply cas/cas (into [db] (drop 1 tx)))
                     [tx]))
                 full-tx))))

(defn transact [x]
  @(d/transact *conn* x))

(defn resolve-tempids [x]
  (dt/resolve-tempids (d/db *conn*) x))

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
  (transact (resolve-tempids [{:db/id "tempid", :e/id2 "a", :e/info "1"}
                              [:dt/cas "tempid" :e/version nil 1 "my-sha"]]))
  (is (= #:e{:id2 "a", :info "1", :version 1} (pull [:e/id2 "a"])))

  (is (= "Cannot use tempid for existing :db.unique/value entities"
         (err-msg (transact (resolve-tempids [{:db/id "tempid", :e/id2 "a", :e/info "1"}
                                              [:dt/cas "tempid" :e/version nil 1 "my-sha"]])))))

  (transact [[:dt/cas [:e/id2 "a"] :e/version 1 2 "my-sha-2"]])
  (is (= #:e{:id2 "a", :info "1", :version 2} (pull [:e/id2 "a"])))

  (transact [{:db/id [:e/id2 "a"] :e/info "2"}
             [:dt/cas [:e/id2 "a"] :e/version 2 3 "my-sha-3"]])
  (is (= #:e{:id2 "a", :info "2", :version 3} (pull [:e/id2 "a"]))))


(deftest duplicate-sha
  @(d/transact *conn* (resolve-tempids [{:db/id "tempid" :e/id "a" :e/info "1"}
                                        [:dt/cas "tempid" :e/version nil 1 "my-sha"]]))
  (is (true? (str/starts-with?
               (err-msg @(d/transact *conn* (resolve-tempids [{:db/id "tempid" :e/id "b" :e/info "2"}
                                                              [:dt/cas "tempid" :e/version nil 1 "my-sha"]])))
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


(deftest initial-duplicate-is-fine
  @(d/transact *conn*
               (dt/resolve-tempids (d/db *conn*)
                                   [{:db/id "tempid", :e/id "a", :e/info "1"}
                                    [:dt/cas "tempid" :e/version nil 1 "sha-1"]]))
  (is (some? (cas/already-written?->tx
               (d/db *conn*)
               [:e/id "a"]
               :e/version
               nil
               1
               "sha-1")))
  (try
    @(d/transact *conn*
                 (dt/resolve-tempids (d/db *conn*)
                                     [{:db/id "tempid", :e/id "a", :e/info "1"}
                                      [:dt/cas "tempid" :e/version nil 1 "sha-1"]]))
    (fail "Should not get here")
    (catch Exception e
      (is (= :already-transacted (dt/error-code e)))
      (is (true? (dt/already-transacted? e)))))

  #_(is (true? (:transacted? (transact [{:e/id "a" :e/info "2"}
                                        [:dt/cas [:e/id "a"] :e/version 1 2 "sha-2"]]))))

  #_(is (= [#:e{:id "a", :info "2"}
            [:dt/cas [:e/id "a"] :e/version 1 2 "sha-2"]]
           (dt/resolve-tempids (d/db *conn*)
                               [{:e/id "a" :e/info "2"}
                                [:dt/cas [:e/id "a"] :e/version 1 2 "sha-2"]])))

  #_(try
      @(d/transact *conn* [{:e/id "a" :e/info "2"}
                           [:dt/cas [:e/id "a"] :e/version 1 2 "sha-2"]])
      (fail "Should not get here")
      (catch Exception e
        (is (true? (dt/already-transacted? e)))))

  #_(try
      @(d/transact *conn* [[:dt/cas [:e/id "a"] :e/version 222 3 "sha-3"]])
      (fail "Should not get here")
      (catch Exception e
        (is (= :cas-failure (dt/error-code e)))
        (is (false? (dt/already-transacted? e))))))

(deftest transact-bad-cas
  @(d/transact *conn* [{:e/id "a" :db/id "tempid"}
                       [:dt/cas [:e/id "a" :as "tempid"] :e/version nil 1 "sha-1"]])
  ;(is (= "Cas failure" (err-msg (dry-cas [:dt/cas [:e/id "a"] :e/version 123 2 "sha-2"]))))
  (is (= ":db.error/cas-failed Compare failed: 123 1" (err-msg @(d/transact *conn* [[:dt/cas [:e/id "a"] :e/version 123 2 "sha-2"]])))))

(defn dtx [tx-data]
  @(dt/transact *conn* tx-data))

(deftest transact-wrapper-test
  (dtx [{:e/id "a" :e/version 1}])
  (is (true? (:transacted? (dtx [[:dt/cas [:e/id "a"] :e/version 1 2 "my-sha"]]))))
  (let [{:keys [transacted? db-after db-before] :as res} (dtx [[:dt/cas [:e/id "a"] :e/version 1 2 "my-sha"]])]
    (is (= 1 (:e/version (d/pull db-before [:e/version] [:e/id "a"]))))
    (is (= 2 (:e/version (d/pull db-after [:e/version] [:e/id "a"]))))
    (is (false? transacted?))
    (is (= res (dtx [[:dt/cas [:e/id "a"] :e/version 1 2 "my-sha"]])))))

(deftest duplicate-sha->unhealthy
  (is (true? @dt/healthy?))
  @(d/transact *conn* [{:e/id "a" :e/version 1}
                       {:e/id "b" :e/version 1}])
  (dtx [[:dt/cas [:e/id "a"] :e/version 1 2 "sha"]])
  (try
    (dtx [[:dt/cas [:e/id "b"] :e/version 1 2 "sha"]])
    (fail "Should not get here")
    (catch Exception e
      (is (true? (dt/duplicate-sha? e)))))
  (is (false? @dt/healthy?)))

(deftest cas-helpers
  @(d/transact *conn* [{:e/id "a" :e/version 1}])
  (try
    (dtx [[:dt/cas [:e/id "a"] :e/version 2 3 "sha"]])
    (fail "Should not get here")
    (catch Exception e
      (is (true? (dt/cas-failure? e :e/version)))
      (is (= 1 (dt/expected-val e)))
      (is (= 2 (dt/given-val e))))))

(deftest support-long-eids
  @(d/transact *conn* [{:e/id "a" :e/version 1}])
  (let [eid (:db/id (d/pull (d/db *conn*) [:db/id] [:e/id "a"]))]
    (dtx [[:dt/cas eid :e/version 1 2 "sha"]])))

(defn pull-ref [e attr]
  (d/q '[:find ?ident .
         :in $ ?e ?a
         :where
         [?e ?a ?v]
         [?v :db/ident ?ident]]
       (d/db *conn*)
       e
       attr))

(deftest regular-cas-improvement
  (dtx [{:e/id "a" :e/version 1 :e/ref :e/enum-1}])
  (is (= :e/enum-1 (pull-ref [:e/id "a"] :e/ref)))
  (dtx [[:db/cas [:e/id "a"] :e/ref :e/enum-1 :e/enum-2]])
  (is (= :e/enum-2 (pull-ref [:e/id "a"] :e/ref)))

  (try
    (dtx [[:db/cas [:e/id "a"] :e/ref :e/enum-1 :e-enum-3]])
    (fail "Should not get here")
    (catch Exception e
      (is (true? (dt/cas-failure? e :e/ref))))))
