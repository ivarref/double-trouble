(ns com.github.ivarref.no-double-trouble
  (:require [com.github.ivarref.no-double-trouble.impl :as impl]
            [com.github.ivarref.no-double-trouble.sha :as sha]
            [com.github.ivarref.no-double-trouble.dbfns.cas :as cas]
            [com.github.ivarref.no-double-trouble.generated :as gen]
            [datomic.api :as d])
  (:import (datomic Connection)
           (clojure.lang IDeref IBlockingDeref IPending)
           (java.util.concurrent Future TimeUnit TimeoutException)))

(def schema
  (into
    [#:db{:ident :com.github.ivarref.no-double-trouble/sha-1, :cardinality :db.cardinality/one, :valueType :db.type/string}]
    gen/fns))


(defn sha [m]
  (sha/sha-1 m))

(defn root-cause [e]
  (if-let [root (ex-cause e)]
    (root-cause root)
    e))

(defn cas-failure-for-attr [^Throwable e attr]
  (when-let [root-cause (root-cause e)]
    (when-let [m (ex-data root-cause)]
      (when (and (= :db.error/cas-failed (:db/error m))
                 (= attr (:a m)))
        m))))


(defn return-cas-success-value [db cas-lock sha]
  (let [[_op e a v-old _v] cas-lock]
    (when (some? v-old)
      (when-let [[v tx] (d/q '[:find [?v ?tx]
                               :in $ ?e ?a ?v-old ?sha
                               :where
                               [?e ?a ?v-old ?tx false]
                               [?tx :com.github.ivarref.no-double-trouble/sha-1 ?sha ?tx true]
                               [?e ?a ?v ?tx true]]
                             (d/history db)
                             e
                             a
                             v-old
                             sha)]
        {:v           v
         :transacted? false
         :db-after    (d/as-of db tx)
         :db-before   (d/as-of db (dec tx))}))))

(defn resolve-tempid [db full-tx single]
  (cond
    (and (vector? single)
         (>= (count single) 1)
         (= :ndt/cas (first single))
         (not= 5 (count single)))
    (throw (ex-info ":ndt/cas requires exactly 5 arguments" {:tx single}))

    (and (vector? single)
         (>= (count single) 1)
         (= :ndt/cas (first single))
         (= 5 (count single)))
    (let [[op e a old-v new-v] single]
      (if (string? e)
        (if-let [ref (->> full-tx
                          (filter map?)
                          (filter #(= e (:db/id %)))
                          (first))]
          (if-let [new-single (reduce-kv (fn [_ k v]
                                           (when (or (cas/is-unique-value? db k)
                                                     (cas/is-identity? db k))
                                             (reduced [op [k v :as e] a old-v new-v])))
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


(defn expand-tx [db full-tx]
  (let [full-tx (resolve-tempids db full-tx)]
    (vec (mapcat (fn [tx]
                   (if (and (vector? tx) (= :ndt/cas (first tx)))
                     (apply cas/cas (into [db] (drop 1 tx)))
                     [tx]))
                 full-tx))))

; Borrowed from clojure.core
(defn ^:private deref-future
  ([^Future fut]
   (.get fut))
  ([^Future fut timeout-ms timeout-val]
   (try (.get fut timeout-ms TimeUnit/MILLISECONDS)
        (catch TimeoutException _
          timeout-val))))

(defmacro handle-cas [conn a new-v cas-op sha get-res]
  `(try
     (let [res# ~get-res]
       (assoc res# :v ~new-v :transacted? true))
     (catch Exception exception#
       (if (not (cas-failure-for-attr exception# ~a))
         (throw exception#)
         (or (return-cas-success-value (d/db ~conn) ~cas-op ~sha)
             (throw exception#))))))

(defn transact [conn sha tx]
  (assert (instance? Connection conn) "conn must be an instance of datomic.Connection")
  (assert (and (string? sha) (= 40 (count sha))) "sha must be a string of length 40")
  (assert (vector? tx) "tx must be a vector")
  (let [tx (resolve-tempids (d/db conn) tx)
        cas-op (->> tx
                    (filter vector?)
                    (filter not-empty)
                    (filter #(= :ndt/cas (first %)))
                    (first))
        _ (when (nil? cas-op)
            (throw (ex-info "Transaction must contain :ndt/cas operation" {:tx tx :sha sha})))
        [op e a old-v new-v] cas-op]
    (assert (= 5 (count cas-op)) "tx must be a :ndt/cas operation")
    (assert (keyword? a) ":a must be a keyword")
    (assert (some? new-v) ":v must be some?")
    #_(if-let [return-early (return-cas-success-value (d/db conn) cas-op sha)]
        (reify
          IDeref
          (deref [_]
            return-early)
          IBlockingDeref
          (deref [_ _timeout-ms _timeout-val]
            return-early)
          IPending
          (isRealized [_] true)
          Future
          (get [_]
            return-early)
          (get [_ _timeout _unit]
            return-early)
          (isCancelled [_] false)
          (isDone [_] true)
          (cancel [_ _interrupt?] false)))
    (let [full-tx (into [{:db/id                                      "datomic.tx"
                          :com.github.ivarref.no-double-trouble/sha-1 sha}]
                        tx)
          fut (d/transact conn full-tx)]
      (reify
        IDeref
        (deref [_]
          (handle-cas conn a new-v cas-op sha (deref-future fut)))
        IBlockingDeref
        (deref [_ timeout-ms timeout-val]
          (handle-cas conn a new-v cas-op sha (deref-future fut timeout-ms timeout-val)))
        IPending
        (isRealized [_] (.isDone fut))
        Future
        (get [_]
          (handle-cas conn a new-v cas-op sha (.get fut)))
        (get [_ timeout unit]
          (handle-cas conn a new-v cas-op sha (.get fut timeout unit)))
        (isCancelled [_] (.isCancelled fut))
        (isDone [_] (.isDone fut))
        (cancel [_ interrupt?] (.cancel fut interrupt?))))))

