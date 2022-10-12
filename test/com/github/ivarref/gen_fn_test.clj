(ns com.github.ivarref.gen-fn-test
  (:require [clojure.test :refer [deftest]]
            [com.github.ivarref.double-trouble.cas :as cas]
            [com.github.ivarref.double-trouble.counter :as counter]
            [com.github.ivarref.double-trouble.jii :as jii]
            [com.github.ivarref.double-trouble.sac :as sac]
            [com.github.ivarref.gen-fn :as gen-fn]))

(deftest generate-fn
  (gen-fn/gen-fn! :dt/cas #'cas/cas "src/com/github/ivarref/double_trouble/generated.clj" :reset? true)
  (gen-fn/gen-fn! :dt/jii #'jii/jii "src/com/github/ivarref/double_trouble/generated.clj")
  (gen-fn/gen-fn! :dt/sac #'sac/sac "src/com/github/ivarref/double_trouble/generated.clj")
  (gen-fn/gen-fn! :dt/counter #'counter/counter "src/com/github/ivarref/double_trouble/generated.clj")
  (= 1 1))
