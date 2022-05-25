(ns com.github.ivarref.debug)

(ns clojure.core)

(defn pp [x]
  (clojure.pprint/pprint x)
  x)

(defn b []
  (println "************************************"))

(defmacro def-let
  "like let, but binds the expressions globally."
  [bindings & more]
  (let [let-expr (macroexpand `(let ~bindings))
        names-values (partition 2 (second let-expr))
        defs   (map #(cons 'def %) names-values)]
    (concat (list 'do) defs more)))

