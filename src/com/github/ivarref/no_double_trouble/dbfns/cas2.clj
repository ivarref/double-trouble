(ns com.github.ivarref.no-double-trouble.dbfns.cas2
  (:require [clojure.walk :as walk]
            [datomic.api :as d])
  (:import (java.util HashSet List)
           (datomic Database)))

(defn to-clojure-types [m]
  (walk/prewalk
    (fn [e]
      (cond (instance? HashSet e)
            (into #{} e)

            (and (instance? List e) (not (vector? e)))
            (vec e)

            :else e))
    m))

; [:cas/contains [:rh/lookup-ref id] :state #{:scheduled :done nil} :scheduled]

(defn cas-contains-inner [db e-or-lookup-ref attr coll key]
  (cond
    (string? e-or-lookup-ref)
    (d/cancel {:cognitect.anomalies/category :cognitect.anomalies/incorrect
               :cognitect.anomalies/message  "Entity cannot be string"})
    :else
    nil))


(defn cas2 [db e-or-lookup-ref attr old-val new-val]
  (cas-contains-inner
    db
    (to-clojure-types e-or-lookup-ref)
    (to-clojure-types attr)
    (to-clojure-types old-val)
    (to-clojure-types new-val)))
