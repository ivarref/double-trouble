(ns com.github.ivarref.double-trouble.cas
  (:require [clojure.walk :as walk]
            [datomic.api :as d])
  (:import (java.util HashSet List)
           (datomic.db DbId)))

(defn to-clojure-types [m]
  (walk/prewalk
    (fn [e]
      (cond (instance? String e)
            e

            (instance? HashSet e)
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

(defn get-val [db e a]
  (d/q '[:find ?v .
         :in $ ?e ?a
         :where
         [?e ?a ?v]]
       db
       e
       a))

(defn sha-exists? [db sha]
  (some? (d/q '[:find ?tx .
                :in $ ?sha
                :where
                [?tx :com.github.ivarref.double-trouble/sha-1 ?sha]]
              db
              sha)))

(defn cas-ok [db lookup-ref a new-val sha]
  (if (sha-exists? db sha)
    (d/cancel {:cognitect.anomalies/category                 :cognitect.anomalies/incorrect
               :cognitect.anomalies/message                  (str "SHA already exists! SHA: " sha)
               :com.github.ivarref.double-trouble/code       :sha-exists
               :com.github.ivarref.double-trouble/lookup-ref lookup-ref
               :com.github.ivarref.double-trouble/sha        sha})
    [[:db/add lookup-ref a new-val]
     [:db/add "datomic.tx" :com.github.ivarref.double-trouble/sha-1 sha]]))

(defn already-written?->tx [db e a old-val new-val sha]
  (if (some? old-val)
    (d/q '[:find ?tx .
           :in $ ?e ?a ?old-val ?new-val ?sha
           :where
           [?e ?a ?old-val ?tx false]
           [?e ?a ?new-val ?tx true]
           [?tx :com.github.ivarref.double-trouble/sha-1 ?sha ?tx true]]
         (d/history db)
         e
         a
         old-val
         new-val
         sha)
    (let [tx-write (d/q '[:find [?tx ?op]
                          :in $ ?e ?a ?new-val ?sha
                          :where
                          [?e ?a ?new-val ?tx ?op]
                          [?tx :com.github.ivarref.double-trouble/sha-1 ?sha ?tx true]]
                        (d/history db)
                        e
                        a
                        new-val
                        sha)]
      (if (and (= 2 (count tx-write))
               (true? (last tx-write)))
        (first tx-write)
        nil))))

(defn cas-mismatch [db e a old-val new-val sha]
  ; Cas is not fine. See if values are already written
  (let [tx-written (already-written?->tx db e a old-val new-val sha)]
    (if (some? tx-written)
      (d/cancel {:cognitect.anomalies/category              :cognitect.anomalies/conflict
                 :cognitect.anomalies/message               "Already transacted"
                 :com.github.ivarref.double-trouble/code    :already-transacted
                 :com.github.ivarref.double-trouble/e       e
                 :com.github.ivarref.double-trouble/a       a
                 :com.github.ivarref.double-trouble/old-val old-val
                 :com.github.ivarref.double-trouble/new-val new-val
                 :com.github.ivarref.double-trouble/tx      tx-written
                 :com.github.ivarref.double-trouble/sha     sha})
      ; This will throw a regular cas exception, but we expand to it anyway.
      ; This is done so that existing exception handling doesn't need to updated.
      [[:db/cas e a old-val new-val]])))

(defn cas-inner-2 [db lookup-ref
                   a
                   old-val
                   new-val
                   sha]
  (if-let [e (:db/id (d/pull db [:db/id] lookup-ref))]
    (let [curr-val (get-val db e a)]
      (if (= curr-val old-val)
        (cas-ok db lookup-ref a new-val sha)
        (cas-mismatch db e a old-val new-val sha)))
    (d/cancel {:cognitect.anomalies/category                 :cognitect.anomalies/incorrect
               :cognitect.anomalies/message                  "Could not find entity"
               :com.github.ivarref.double-trouble/code       :could-not-find-entity
               :com.github.ivarref.double-trouble/lookup-ref lookup-ref})))

(defn cas-inner [db e-or-lookup-ref a old-val new-val sha]
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
    (cas-inner-2 db e-or-lookup-ref a old-val new-val sha)

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
        (cas-inner-2 db e a old-val new-val sha)

        (nil? old-val)
        [[:db/add (last e-or-lookup-ref) a new-val]
         [:db/add "datomic.tx" :com.github.ivarref.double-trouble/sha-1 sha]]

        :else
        (d/cancel {:cognitect.anomalies/category :cognitect.anomalies/incorrect
                   :cognitect.anomalies/message  "Old-val must be nil for new entities"})))

    :else
    (d/cancel {:cognitect.anomalies/category :cognitect.anomalies/incorrect
               :cognitect.anomalies/message  "Unhandled state"})))



(defn cas [db e-or-lookup-ref attr old-val new-val sha]
  (cas-inner
    db
    (to-clojure-types e-or-lookup-ref)
    (to-clojure-types attr)
    (to-clojure-types old-val)
    (to-clojure-types new-val)
    (to-clojure-types sha)))
