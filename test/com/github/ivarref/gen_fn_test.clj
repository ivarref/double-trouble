(ns com.github.ivarref.gen-fn-test
  (:require [clojure.test :refer [deftest]]
            [com.github.ivarref.no-more-double-trouble.dbfns.cas :as cas]
            [com.github.ivarref.gen-fn :as gen-fn]))

(deftest generate-fn
  (gen-fn/gen-fn! :nmdt/cas #'cas/cas "src/com/github/ivarref/no_more_double_trouble/generated.clj")
  (= 1 1))
