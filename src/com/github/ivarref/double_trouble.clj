(ns com.github.ivarref.double-trouble
  (:require [com.github.ivarref.double-trouble.cas :as cas]
            [com.github.ivarref.double-trouble.generated :as gen]
            [com.github.ivarref.double-trouble.sha :as sha]
            [datomic.api :as d])
  (:import (clojure.lang IBlockingDeref IDeref IPending)
           (datomic Connection Database Datom)
           (java.util List)
           (java.util.concurrent Future TimeUnit TimeoutException)))

(defonce healthy? (atom true))

(def schema
  (into
    [#:db{:ident :com.github.ivarref.double-trouble/sha-1 :cardinality :db.cardinality/one :valueType :db.type/string :unique :db.unique/value}
     #:db{:ident :com.github.ivarref.double-trouble/counter-name :cardinality :db.cardinality/one :valueType :db.type/string :unique :db.unique/identity}
     #:db{:ident :com.github.ivarref.double-trouble/counter-value :cardinality :db.cardinality/one :valueType :db.type/long}]
    gen/schema))

(defn sha [m]
  (sha/sha-1 m))

(defn root-cause [e]
  (if-let [root (ex-cause e)]
    (root-cause root)
    e))

(defn resolve-tempid [db full-tx single]
  (cond
    (and (vector? single)
         (>= (count single) 1)
         (= :dt/cas (first single))
         (not= 6 (count single)))
    (throw (ex-info ":dt/cas requires exactly 6 arguments" {:tx single}))

    (and (vector? single)
         (>= (count single) 1)
         (= :dt/cas (first single))
         (= 6 (count single)))
    (let [[op e a old-v new-v sha-val] single]
      (if (string? e)
        (if-let [ref (->> full-tx
                          (filter map?)
                          (filter #(= e (:db/id %)))
                          (first))]
          (if-let [new-single (reduce-kv (fn [_ k v]
                                           (when (or (cas/is-unique-value? db k)
                                                     (cas/is-identity? db k))
                                             (when (and (cas/is-unique-value? db k)
                                                        (some? (:db/id (d/pull db [:db/id] [k v]))))
                                               (throw (ex-info "Cannot use tempid for existing :db.unique/value entities" {})))
                                             (reduced [op [k v :as e] a old-v new-v sha-val])))
                                         nil
                                         (dissoc ref :db/id))]
            new-single
            (throw (ex-info (str "Could not resolve tempid") {:tempid e})))
          (throw (ex-info (str "Could not resolve tempid") {:tempid e})))

        single))
    :else
    single))

(defn resolve-tempids [db tx]
  (mapv (partial resolve-tempid db tx) tx))

(defn is-ref? [db attr]
  (= :db.type/ref
     (d/q '[:find ?ident .
            :in $ ?e
            :where
            [?e :db/valueType ?typ]
            [?typ :db/ident ?ident]]
          db
          attr)))

(defn resolve-enum-ref [db tx]
  (if (and (vector? tx)
           (= 5 (count tx))
           (= :db/cas (first tx)))
    (let [[op e a v-old v-new] tx]
      (if (and (is-ref? db a)
               (keyword? v-old))
        [op e a (:db/id (d/pull db [:db/id] v-old)) v-new]
        tx))
    tx))

(defn resolve-enum-refs [db full-tx]
  (mapv (partial resolve-enum-ref db) full-tx))

(defn error-code [e]
  (when-let [dat (ex-data (root-cause e))]
    (get dat :com.github.ivarref.double-trouble/code)))

(defn already-transacted? [e]
  (= :already-transacted (error-code e)))

(defn no-change? [e]
  (= :no-change (error-code e)))

(defn duplicate-sha? [e]
  (= :sha-exists (error-code e)))

(defn cas-failure?
  ([exception]
   (when-let [ex-dat (ex-data (root-cause exception))]
     (= :db.error/cas-failed (:db/error ex-dat))))
  ([exception attr]
   (when-let [ex-dat (ex-data (root-cause exception))]
     (and (= :db.error/cas-failed (:db/error ex-dat))
          (= attr (:a ex-dat))))))

(defn expected-val [e]
  (get (ex-data (root-cause e)) :v))

(defn given-val [e]
  (get (ex-data (root-cause e)) :v-old))

(defn return-already-transacted [conn e]
  (let [{:com.github.ivarref.double-trouble/keys [tx]} (ex-data (root-cause e))]
    (with-meta
      (merge (ex-data (root-cause e))
             {:transacted? false
              :db-after    (d/as-of (d/db conn) tx)
              :db-before   (d/as-of (d/db conn) (dec tx))})
      {:dt/error-map? true})))

(defn order-tx [_db ftx]
  (loop [ftx ftx
         {:keys [other sac cas] :as m} {}]
    (if (empty? ftx)
      (reduce into [] [sac cas other])
      (let [[h & rst] ftx
            kw (cond (and (vector? h)
                          (not-empty h)
                          (= :dt/sac (first h)))
                     :sac
                     (and (vector? h)
                          (not-empty h)
                          (= :dt/cas (first h)))
                     :cas
                     :else :other)]
        (recur rst (update m kw (fnil conj []) h))))))


(defn expand-tx [db full-tx]
  (->> full-tx
       (resolve-tempids db)
       (resolve-enum-refs db)
       (order-tx db)))

(defmacro handle-dt-cas [conn future-result]
  `(try
     (let [res# ~future-result]
       (assoc (select-keys res# [:db-before :db-after]) :transacted? true))
     (catch Exception e#
       (when (duplicate-sha? e#)
         (reset! healthy? false))
       (if
         (or (no-change? e#) (already-transacted? e#))
         (return-already-transacted ~conn e#)
         (throw e#)))))

; Borrowed from clojure.core
(defn ^:private deref-future
  ([^Future fut]
   (.get fut))
  ([^Future fut timeout-ms timeout-val]
   (try (.get fut timeout-ms TimeUnit/MILLISECONDS)
        (catch TimeoutException _
          timeout-val))))

(defn transact [conn tx]
  (assert (instance? Connection conn) "conn must be an instance of datomic.Connection")
  (assert (vector? tx) "tx must be a vector")
  (let [fut (d/transact conn (expand-tx (d/db conn) tx))]
    (reify
      IDeref
      (deref [_]
        (handle-dt-cas conn (deref-future fut)))
      IBlockingDeref
      (deref [_ timeout-ms timeout-val]
        (handle-dt-cas conn (deref-future fut timeout-ms timeout-val)))
      IPending
      (isRealized [_] (.isDone fut))
      Future
      (get [_]
        (handle-dt-cas conn (.get fut)))
      (get [_ timeout unit]
        (handle-dt-cas conn (.get fut timeout unit)))
      (isCancelled [_] (.isCancelled fut))
      (isDone [_] (.isDone fut))
      (cancel [_ interrupt?] (.cancel fut interrupt?)))))

(defn ensure-partition!
  "Ensures that `new-partition` is installed in the database."
  [conn new-partition]
  (assert (keyword? new-partition))
  (let [db (d/db conn)]
    (if-let [eid (d/q '[:find ?e .
                        :in $ ?part
                        :where
                        [?e :db/ident ?part]]
                      db
                      new-partition)]
      eid
      (get-in
        @(d/transact conn [{:db/id "new-part" :db/ident new-partition}
                           [:db/add :db.part/db :db.install/partition "new-part"]])
        [:tempids "new-part"]))))

(defn resolve-lookup-refs [{:keys [tx-data db-after]} attr]
  (assert (keyword? attr) "Expected attr to be a keyword")
  (assert (or (instance? List tx-data) (vector? tx-data)) (str "Expected tx-data to be a vector. Was: " (type tx-data)))
  (assert (instance? Database db-after) "Expected db-after to be a datomic.Database")
  (let [res (reduce (fn [o datom]
                      (assert (instance? Datom datom) "Expected v to be a datomic.Datom")
                      (if (= (d/entid db-after attr) (d/entid db-after (.a datom)))
                        (conj o [attr (.v datom)])
                        o))
                    #{}
                    (into [] tx-data))]
    (if (empty? res)
      (throw (ex-info (str "Did not find attribute " attr " in transaction") {:tx-data tx-data :attr attr}))
      res)))

(defn resolve-lookup-ref [tx-result attr]
  (let [res (resolve-lookup-refs tx-result attr)]
    (cond
      (> (count res) 1)
      (throw (ex-info "More than a single match" {:attr attr :found res}))

      (= 1 (count res))
      (first res)

      :else
      nil)))
