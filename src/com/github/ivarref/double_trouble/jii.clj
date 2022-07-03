(ns com.github.ivarref.double-trouble.jii
  (:require [datomic.api :as d]))

(defn get-val [db e a]
  (d/q '[:find ?v .
         :in $ ?e ?a
         :where
         [?e ?a ?v]]
       db
       e
       a))

(defn jii [db e-or-lookup-ref attr]
  (cond
    (or (not (keyword? attr))
        (nil? (:db/id (d/pull db [:db/id] attr))))
    (d/cancel {:cognitect.anomalies/category           :cognitect.anomalies/incorrect
               :cognitect.anomalies/message            "Could not find attr"
               :com.github.ivarref.double-trouble/code :could-not-find-attr
               :com.github.ivarref.double-trouble/attr attr})

    (nil? (:db/id (d/pull db [:db/id] e-or-lookup-ref)))
    (d/cancel {:cognitect.anomalies/category                 :cognitect.anomalies/incorrect
               :cognitect.anomalies/message                  "Could not find entity"
               :com.github.ivarref.double-trouble/code       :could-not-find-entity
               :com.github.ivarref.double-trouble/lookup-ref e-or-lookup-ref})

    (nil? (get-val db e-or-lookup-ref attr))
    (d/cancel {:cognitect.anomalies/category                 :cognitect.anomalies/incorrect
               :cognitect.anomalies/message                  "nil not supported"
               :com.github.ivarref.double-trouble/code       :nil-not-supported
               :com.github.ivarref.double-trouble/lookup-ref e-or-lookup-ref
               :com.github.ivarref.double-trouble/attr       attr})

    (not (int? (get-val db e-or-lookup-ref attr)))
    (d/cancel {:cognitect.anomalies/category                 :cognitect.anomalies/incorrect
               :cognitect.anomalies/message                  "Attribute must be int"
               :com.github.ivarref.double-trouble/code       :must-be-int
               :com.github.ivarref.double-trouble/lookup-ref e-or-lookup-ref
               :com.github.ivarref.double-trouble/attr       attr})

    :else
    [[:db/add e-or-lookup-ref attr (inc (get-val db e-or-lookup-ref attr))]]))
