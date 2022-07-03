(ns com.github.ivarref.double-trouble.sac
  (:require [datomic.api :as d]))

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
  (d/q '[:find ?v .
         :in $ ?e ?a
         :where
         [?e ?a ?v]]
       db
       e
       a))

(defn sac [db e-or-lookup-ref attr new-val]
  (cond
    (or (not (keyword? attr))
        (nil? (:db/id (d/pull db [:db/id] attr))))
    (d/cancel {:cognitect.anomalies/category           :cognitect.anomalies/incorrect
               :cognitect.anomalies/message            "Could not find attr"
               :com.github.ivarref.double-trouble/code :could-not-find-attr
               :com.github.ivarref.double-trouble/attr attr})

    (nil? new-val)
    (d/cancel {:cognitect.anomalies/category                 :cognitect.anomalies/incorrect
               :cognitect.anomalies/message                  "nil not supported"
               :com.github.ivarref.double-trouble/code       :nil-not-supported
               :com.github.ivarref.double-trouble/lookup-ref e-or-lookup-ref})

    (nil? (:db/id (d/pull db [:db/id] e-or-lookup-ref)))
    (d/cancel {:cognitect.anomalies/category                 :cognitect.anomalies/incorrect
               :cognitect.anomalies/message                  "Could not find entity"
               :com.github.ivarref.double-trouble/code       :could-not-find-entity
               :com.github.ivarref.double-trouble/lookup-ref e-or-lookup-ref})

    (and (is-ref? db attr)
         (keyword? new-val)
         (nil? (:db/id (d/pull db [:db/id] [:db/ident new-val]))))
    (d/cancel {:cognitect.anomalies/category                :cognitect.anomalies/incorrect
               :cognitect.anomalies/message                 "Could not find ref value"
               :com.github.ivarref.double-trouble/code      :could-not-find-ref-value
               :com.github.ivarref.double-trouble/ref-value new-val})

    (nil? (get-val db e-or-lookup-ref attr))
    [[:db/add e-or-lookup-ref attr new-val]]

    :else
    (let [resolved-value (if (and (is-ref? db attr)
                                  (keyword? new-val))
                           (:db/id (d/pull db [:db/id] [:db/ident new-val]))
                           new-val)]
      (if (= (get-val db e-or-lookup-ref attr) resolved-value)
        (d/cancel {:cognitect.anomalies/category                 :cognitect.anomalies/incorrect
                   :cognitect.anomalies/message                  "No change"
                   :com.github.ivarref.double-trouble/code       :no-change
                   :com.github.ivarref.double-trouble/lookup-ref e-or-lookup-ref
                   :com.github.ivarref.double-trouble/attr       attr})
        [[:db/add e-or-lookup-ref attr new-val]]))))
