(ns com.github.ivarref.no-double-trouble.dbfns.cas
  (:require [clojure.walk :as walk]
            [datomic.api :as d])
  (:import (java.util HashSet List)
           (datomic Database)
           (datomic.db DbId)))

(defn to-clojure-types [m]
  (walk/prewalk
    (fn [e]
      (cond (instance? HashSet e)
            (into #{} e)

            (and (instance? List e) (not (vector? e)))
            (vec e)

            :else e))
    m))

(defn is-identity? [db attr]
  (= :db.unique/identity
     (d/q '[:find ?ident .
            :in $ ?e
            :where
            [?e :db/unique ?typ]
            [?typ :db/ident ?ident]]
          db
          attr)))

(defn is-unique-value? [db attr]
  (= :db.unique/value
     (d/q '[:find ?ident .
            :in $ ?e
            :where
            [?e :db/unique ?typ]
            [?typ :db/ident ?ident]]
          db
          attr)))

(defn cas-inner [db e-or-lookup-ref a old-val new-val]
  (cond
    (string? e-or-lookup-ref)
    (d/cancel {:cognitect.anomalies/category :cognitect.anomalies/incorrect
               :cognitect.anomalies/message  "Entity cannot be string"})

    (instance? DbId e-or-lookup-ref)
    (d/cancel {:cognitect.anomalies/category :cognitect.anomalies/incorrect
               :cognitect.anomalies/message  "Entity cannot be tempid/datomic.db.DbId"})

    (and (vector? e-or-lookup-ref)
         (= 2 (count e-or-lookup-ref))
         (or (is-identity? db (first e-or-lookup-ref))
             (is-unique-value? db (first e-or-lookup-ref))))
    (cond
      (some? (:db/id (d/pull db [:db/id] e-or-lookup-ref)))
      [[:db/cas e-or-lookup-ref a old-val new-val]]

      :else
      (d/cancel {:cognitect.anomalies/category :cognitect.anomalies/incorrect
                 :cognitect.anomalies/message  "Could not find entity"}))

    (and (vector? e-or-lookup-ref)
         (= 4 (count e-or-lookup-ref))
         (keyword? (first e-or-lookup-ref))
         (= :as (nth e-or-lookup-ref 2))
         (string? (last e-or-lookup-ref))
         (or (is-identity? db (first e-or-lookup-ref))
             (is-unique-value? db (first e-or-lookup-ref))))
    (let [e (vec (take 2 e-or-lookup-ref))]
      (cond
        (some? (:db/id (d/pull db [:db/id] e)))
        [[:db/cas e a old-val new-val]]

        (nil? old-val)
        [[:db/add (last e-or-lookup-ref) a new-val]]

        :else
        (d/cancel {:cognitect.anomalies/category :cognitect.anomalies/incorrect
                   :cognitect.anomalies/message  "Old-val must be nil for new entities"})))

    :else
    (do
      (println e-or-lookup-ref)
      (d/cancel {:cognitect.anomalies/category :cognitect.anomalies/incorrect
                 :cognitect.anomalies/message  "Unhandled state"}))))



(defn cas [db e-or-lookup-ref attr old-val new-val]
  (cas-inner
    db
    (to-clojure-types e-or-lookup-ref)
    (to-clojure-types attr)
    (to-clojure-types old-val)
    (to-clojure-types new-val)))
