(ns com.github.ivarref.no-double-trouble
  (:require [com.github.ivarref.no-double-trouble.impl :as impl]
            [com.github.ivarref.no-double-trouble.sha :as sha]
            [datomic.api :as d])
  (:import (datomic Connection)))

(def schema
  [#:db{:ident :com.github.ivarref.no-double-trouble/sha-1, :cardinality :db.cardinality/one, :valueType :db.type/string}])

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


;(reify
;  clojure.lang.IDeref
;  (deref [_] (deref-future fut))
;  clojure.lang.IBlockingDeref
;  (deref
; [_ timeout-ms timeout-val
;    (deref-future fut timeout-ms timeout-val)
;  clojure.lang.IPending
;  (isRealized [_] (.isDone fut))
;  java.util.concurrent.Future
;  (get [_] (.get fut))
;  (get [_ timeout unit] (.get fut timeout unit))
;  (isCancelled [_] (.isCancelled fut))
;  (isDone [_] (.isDone fut))
;  (cancel [_ interrupt?] (.cancel fut interrupt?))]))

(defn rewrite-cas-str [full-tx & [new-tempid]]
  (let [[op tempid-str a old-v new-v] (first full-tx)]
    (if (string? tempid-str)
      (let [new-tempid (or new-tempid (d/tempid :db.part/user))]
        (into [[op new-tempid a old-v new-v]]
              (mapv (fn [x]
                      (cond (and (map? x) (= tempid-str (get x :db/id)))
                            (assoc x :db/id new-tempid)

                            (and (vector? x)
                                 (contains? #{:db/add :db/retract} (first x))
                                 (= tempid-str (second x)))
                            (into [(first x) new-tempid] (drop 2 x))

                            :else x))
                    (rest full-tx))))
      full-tx)))

(defn transact [conn sha tx]
  (assert (instance? Connection conn) "conn must be an instance of datomic.Connection")
  (assert (and (string? sha) (= 40 (count sha))) "sha must be a string of length 40")
  (assert (vector? tx) "tx must be a vector")
  (assert (vector? (first tx)) "first entry in tx must be a :db/cas operation")
  (let [[op e a old-v new-v :as cas-op] (first tx)]
    (assert (= 5 (count cas-op)) "first entry in tx must be a :db/cas operation")
    (assert (= op :db/cas) "first entry in tx must be a :db/cas operation")
    (assert (keyword? a) "first entry in tx must be a :db/cas operation")
    (assert (some? new-v) "first entry in tx must be a :db/cas operation"))

  (if-let [return-early (return-cas-success-value (d/db conn) (first tx) sha)]
    return-early
    (let [[_op _e a _v-old v] (first tx)
          full-tx (into [{:db/id                                      "datomic.tx"
                          :com.github.ivarref.no-double-trouble/sha-1 sha}]
                        tx)]
      (try
        (let [res @(d/transact conn full-tx)]
          (assoc res :v v :transacted? true))
        (catch Exception exception
          (if (not (cas-failure-for-attr exception a))
            (throw exception)
            (or (return-cas-success-value (d/db conn) (first tx) sha)
                (throw exception))))))))
