(ns com.github.ivarref.double-trouble
  (:require [com.github.ivarref.double-trouble.cas :as cas]
            [com.github.ivarref.double-trouble.generated :as gen]
            [com.github.ivarref.double-trouble.sha :as sha]
            [datomic.api :as d])
  (:import (clojure.lang IBlockingDeref IDeref IPending)
           (datomic Connection)
           (java.util.concurrent Future TimeUnit TimeoutException)))

(defonce healthy? (atom true))

(def schema
  (into
    [#:db{:ident :com.github.ivarref.double-trouble/sha-1 :cardinality :db.cardinality/one :valueType :db.type/string :unique :db.unique/value}]
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


(defn error-code [e]
  (when-let [dat (ex-data (root-cause e))]
    (get dat :com.github.ivarref.double-trouble/code)))

(defn already-transacted? [e]
  (= :already-transacted (error-code e)))

(defn duplicate-sha? [e]
  (= :sha-exists (error-code e)))

(defn cas-failure? [e attr]
  (when-let [ex-dat (ex-data (root-cause e))]
    (and (= :db.error/cas-failed (:db/error ex-dat))
         (= attr (:a ex-dat)))))

(defn expected-val [e]
  (get (ex-data (root-cause e)) :v))

(defn given-val [e]
  (get (ex-data (root-cause e)) :v-old))

(defn return-already-transacted [conn e]
  (let [{:com.github.ivarref.double-trouble/keys [tx]} (ex-data (root-cause e))]
    {:transacted? false
     :db-after    (d/as-of (d/db conn) tx)
     :db-before   (d/as-of (d/db conn) (dec tx))}))

(defmacro handle-dt-cas [conn future-result]
  `(try
     (let [res# ~future-result]
       (assoc (select-keys res# [:db-before :db-after]) :transacted? true))
     (catch Exception exception#
       (if (already-transacted? exception#)
         (return-already-transacted ~conn exception#)
         (do
           (when (duplicate-sha? exception#)
             (reset! healthy? false))
           (throw exception#))))))

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
  (let [fut (d/transact conn (resolve-tempids (d/db conn) tx))]
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
