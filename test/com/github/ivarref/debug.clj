(ns com.github.ivarref.debug)

(ns clojure.core)

(defn pp [x]
  "like let, but binds the expressions globally."
  (clojure.pprint/pprint x)
  x)

(defn b []
  (println "************************************"))
