(ns com.github.ivarref.gen-fn-test
  (:require [clojure.test :refer [deftest]]
            [com.github.ivarref.no-more-double-trouble.dbfns.cas]
            [com.github.ivarref.dbfns.generate-fn :as gen-fn]))

(deftest generate-fn
  (gen-fn/generate-function 'com.github.ivarref.no-more-double-trouble.dbfns.cas/cas :nmdt/cas true))
