(ns com.github.ivarref.initial-test
  (:require [clojure.test :as test :refer [deftest is]]
            [com.github.ivarref.no-double-trouble :as ndt]
            [com.github.ivarref.log-init :as log-init]
            [datomic.api :as d]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]))

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

(comment)
[[:db/add "ent" :e/version 1]
 {:db/id "ent" :e/id "a" :e/info "a"}]

(comment)

(deftest add-vs-cas
  (let [{:keys [db-after]} @(d/transact *conn* [[:db/add "ent" :e/version 1]
                                                {:db/id "ent" :e/id "a" :e/info "a"}])]
    (is (= #:e{:id "a" :version 1 :info "a"} (d/pull db-after [:e/id :e/version :e/info] [:e/id "a"])))))


(deftest nil-test
  (let [tempid (d/tempid :db.part/user)
        {:keys [db-after]} @(d/transact *conn* [[:db/cas tempid :e/version nil 1]
                                                {:db/id tempid :e/id "a" :e/info "1"}])]
    (is (= #:e{:id "a" :version 1 :info "1"} (d/pull db-after [:e/id :e/version :e/info] [:e/id "a"])))
    (let [tempid (d/tempid :db.part/user)
          ; The following transaction success, though it shouldn't:
          {:keys [db-after]} @(d/transact *conn* [[:db/cas tempid :e/version nil 2]
                                                  {:db/id tempid :e/id "a" :e/info "2"}])]
      (is (= #:e{:id "a" :version 2 :info "2"} (d/pull db-after [:e/id :e/version :e/info] [:e/id "a"]))))))


(deftest this-throws-as-expected
  @(d/transact *conn* [{:e/id "a" :e/version 999}])
  (let [tempid (d/tempid :db.part/user)]
    (try
      @(d/transact *conn* [[:db/cas tempid :e/version 123 1]
                           {:db/id tempid :e/id "a" :e/info "a"}])
      (throw (Exception. "should not get here"))
      (catch Exception e
        (if-let [cause (ex-cause e)]
          (is (= :db.error/cas-failed (:db/error (ex-data cause))))
          (throw e))))))


(deftest this-should-throw-but-does-not
  @(d/transact *conn* [{:e/id "a" :e/version 1}])
  (let [tempid (d/tempid :db.part/user)]
    @(d/transact *conn* [[:db/cas tempid :e/version nil 2]
                         {:db/id tempid :e/id "a" :e/info "a"}])))


(deftest whatever
  (let [id (d/tempid :db.part/user)]
    (def id-1 (d/entity (d/db *conn*) id))
    (def id-2 (d/entid (d/db *conn*) id))))

(defn dump-version [pom-props]
  (some->> (io/resource "d.datomic-pro/pom.xml")
           (slurp)
           (str/split-lines)
           (filter #(str/includes? % "<version"))
           (first)
           (seq)
           (drop (count "version="))
           (str/join "")))

(comment
  (some->> (io/resource "META-INF/maven/com.datomic/datomic-pro/pom.xml")
           (slurp)
           (str/split-lines)
           (filter #(str/includes? % "<version>"))
           (first)))

#_(deftest nil-test
    (let [{:keys [db-after]} @(d/transact *conn* (ndt/rewrite-cas-str [[:db/cas "new" :e/version nil 1]
                                                                       {:db/id "new" :e/id "a" :e/info "asdf"}]))]
      (is (= #:e{:version 1 :info "asdf"} (d/pull db-after [:e/version :e/info] [:e/id "a"])))))
