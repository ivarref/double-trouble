(ns com.github.ivarref.gen-fn-test
  (:require [clojure.test :refer [deftest]]
            [com.github.ivarref.double-trouble.cas :as cas]
            [com.github.ivarref.double-trouble.counter :as counter]
            [com.github.ivarref.double-trouble.counter-str :as counter-str]
            [com.github.ivarref.double-trouble.jii :as jii]
            [com.github.ivarref.double-trouble.sac :as sac]
            [com.github.ivarref.gen-fn :as gen-fn]))

(deftest generate-fn
  (let [f "src/com/github/ivarref/double_trouble/generated.clj"]
    (gen-fn/gen-fn! :dt/cas #'cas/cas f :reset? true)
    (gen-fn/gen-fn! :dt/jii #'jii/jii f)
    (gen-fn/gen-fn! :dt/sac #'sac/sac f)
    (gen-fn/gen-fn! :dt/counter #'counter/counter f)
    (gen-fn/gen-fn! :dt/counter-str #'counter-str/counter-str f))
  (= 1 1))
