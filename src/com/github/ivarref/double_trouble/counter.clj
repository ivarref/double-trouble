(ns com.github.ivarref.double-trouble.counter
  (:require [datomic.api :as d]))

(defn counter [db counter-name tempid attr]
  (assert (string? counter-name) "counter-name must be a string")
  (assert (string? tempid) "tempid must be a string")
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
     [:db/add tempid attr next-val]]))
