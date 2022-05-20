(ns com.github.ivarref.gen-fn-test
  (:require [clojure.test :refer [deftest]]
            [com.github.ivarref.double-trouble.cas :as cas]
            [com.github.ivarref.gen-fn :as gen-fn]))

(deftest generate-fn
  (gen-fn/gen-fn! :dt/cas #'cas/cas "src/com/github/ivarref/double_trouble/generated.clj")
  (= 1 1))
