(ns com.github.ivarref.sac-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [clojure.tools.logging :as log]
            [com.github.ivarref.double-trouble :as dt]
            [com.github.ivarref.double-trouble.cas :as cas]
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
    (try
      (reset! dt/healthy? true)
      @(d/transact conn dt/schema)
      @(d/transact conn test-schema)
      @(d/transact conn [(gen-fn/datomic-fn :dt/sac #'sac/sac)
                         (gen-fn/datomic-fn :dt/cas #'cas/cas)])
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

(deftest cas-sac-ordering
  @(d/transact *conn* [{:e/id "a" :e/version 1 :e/status :Status/INIT}])
  (is (= :no-change (err-code @(dt/transact *conn* [[:dt/cas [:e/id "a"] :e/version 1 2 "sha"]
                                                    [:dt/sac [:e/id "a"] :e/status :Status/INIT]]))))

  @(dt/transact *conn* [[:dt/cas [:e/id "a"] :e/version 1 2 "sha"]
                        [:dt/sac [:e/id "a"] :e/status :Status/PROCESSING]])

  (is (= :no-change (err-code @(dt/transact *conn* [[:dt/cas [:e/id "a"] :e/version 10 20 "sha"]
                                                    [:dt/sac [:e/id "a"] :e/status :Status/PROCESSING]]))))

  (is (= :no-change (err-code @(dt/transact *conn* [[:dt/cas [:e/id "a"] :e/version 1 2 "sha"]
                                                    [:dt/sac [:e/id "a"] :e/status :Status/PROCESSING]]))))
  (is (= :no-change (err-code @(dt/transact *conn* [[:dt/sac [:e/id "a"] :e/status :Status/PROCESSING]
                                                    [:dt/cas [:e/id "a"] :e/version 10 20 "sha"]]))))
  (let [{:keys [db-before db-after transacted?]} @(dt/transact *conn* [[:dt/sac [:e/id "a"] :e/status :Status/PROCESSING]
                                                                       [:dt/cas [:e/id "a"] :e/version 10 20 "sha"]])]
    (is (false? transacted?))
    (is (= :Status/INIT (get-val db-before [:e/id "a"] :e/status)))
    (is (= :Status/PROCESSING (get-val db-after [:e/id "a"] :e/status)))))

(deftest db-cas-sac-error-ordering
  "dt/sac have higher priority, i.e. 'wins' the error message, over db/cas, regardless of ordering"
  (let [{:strs [a]} (:tempids @(d/transact *conn* [{:db/id "a" :e/id "a" :e/version 1 :e/sha "begin"}]))]
    @(dt/transact *conn* [{:e/id "a" :e/version 2}
                          [:dt/sac [:e/id "a"] :e/sha "original"]])
    (let [tx-res @(dt/transact *conn* [[:dt/sac [:e/id "a"] :e/sha "original"]
                                       [:db/cas a :e/version 100 3]])]
      (is (= :no-change (err-code tx-res)))
      (is (false? (:transacted? tx-res))))
    (let [tx-res @(dt/transact *conn* [[:db/cas a :e/version 100 3]
                                       [:dt/sac [:e/id "a"] :e/sha "original"]])]
      (is (= :no-change (err-code tx-res)))
      (is (false? (:transacted? tx-res))))))

(deftest db-cas-lookup-ref
  "Verify that db/cas handles lookup refs"
  @(d/transact *conn* [{:db/id "a" :e/id "a" :e/version 1 :e/sha "begin"}])
  @(d/transact *conn* [[:db/cas [:e/id "a"] :e/version 1 2]])
  (is (= 2 (:e/version (d/pull (d/db *conn*) [:*] [:e/id "a"])))))
