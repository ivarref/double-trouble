(ns com.github.ivarref.ensure-partition-test
  (:require [clojure.test :refer [deftest is]]
            [com.github.ivarref.double-trouble :as dt]
            [datomic.api :as d]))

(deftest ensure-partition
  (let [conn (let [uri (str "datomic:mem://test-" (random-uuid))]
               (d/delete-database uri)
               (d/create-database uri)
               (d/connect uri))]
    (is (= (dt/ensure-partition! conn :janei)
           (dt/ensure-partition! conn :janei)
           (dt/ensure-partition! conn :janei)))
    (is (not= (dt/ensure-partition! conn :test1)
              (dt/ensure-partition! conn :test2)))))
