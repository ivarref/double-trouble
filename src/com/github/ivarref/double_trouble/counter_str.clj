(ns com.github.ivarref.double-trouble.counter-str
  (:require [datomic.api :as d])
  (:import (datomic.db DbId)))

(defn counter-str [db counter-name tempid attr]
  (assert (string? counter-name) "counter-name must be a string")
  (assert (or (instance? DbId tempid) (string? tempid)) "tempid must be a string or datomic.db.DbId")
  (assert (keyword? attr) "attr must be a keyword")
  (let [next-val (or (some->> (d/q '[:find ?val .
                                     :in $ ?counter-name
                                     :where
                                     [?e :com.github.ivarref.double-trouble/counter-name ?counter-name]
                                     [?e :com.github.ivarref.double-trouble/counter-value ?val]]
                                   db counter-name)
                              (inc))
                     1)]
    [{:com.github.ivarref.double-trouble/counter-name  counter-name
      :com.github.ivarref.double-trouble/counter-value next-val}
     [:db/add tempid attr (str next-val)]]))
