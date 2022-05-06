(ns com.github.ivarref.no-double-trouble.generated
  (:require [datomic.api]
            [clojure.edn :as edn]))

(defn read-dbfn [s]
  (edn/read-string
    {:readers {'db/id  datomic.db/id-literal
               'db/fn  datomic.function/construct
               'base64 datomic.codec/base-64-literal}}
    s))

(def cas "{:db/ident :ndt/cas\n :db/fn #db/fn \n{:lang \"clojure\", :requires [[clojure.walk :as walk] [datomic.api :as d]], :imports [(java.util HashSet List) (datomic Database) (datomic.db DbId)], :params nil, :code nil}\n}")

(def fns [(read-dbfn cas)])
