(ns com.github.ivarref.no-double-trouble.generated
  (:require [datomic.api]
            [clojure.edn :as edn]))

(defn read-dbfn [s]
  (edn/read-string
    {:readers {'db/id  datomic.db/id-literal
               'db/fn  datomic.function/construct
               'base64 datomic.codec/base-64-literal}}
    s))

(def cas2 "{:db/ident :ndt/cas2\n :db/fn #db/fn \n{:lang \"clojure\", :requires [[clojure.walk :as walk] [datomic.api :as d]], :imports [(java.util HashSet List) (datomic Database)], :params [db e-or-lookup-ref attr old-val new-val], :code (letfn [(to-clojure-types [m] (do (walk/prewalk (fn [e] (cond (instance? HashSet e) (into #{} e) (and (instance? List e) (not (vector? e))) (vec e) :else e)) m))) (primitive? [v] (do (or (keyword? v) (number? v) (string? v)))) (cas-contains-inner [db e-or-lookup-ref attr coll key] (do nil))] (do (cas-contains-inner db (to-clojure-types e-or-lookup-ref) (to-clojure-types attr) (to-clojure-types old-val) (to-clojure-types new-val))))}\n}")
(def cas "{:db/ident :ndt/cas2\n :db/fn #db/fn \n{:lang \"clojure\", :requires [[clojure.walk :as walk] [datomic.api :as d]], :imports [(java.util HashSet List) (datomic Database)], :params [db e-or-lookup-ref attr old-val new-val], :code (letfn [(to-clojure-types [m] (do (walk/prewalk (fn [e] (cond (instance? HashSet e) (into #{} e) (and (instance? List e) (not (vector? e))) (vec e) :else e)) m))) (primitive? [v] (do (or (keyword? v) (number? v) (string? v)))) (cas-contains-inner [db e-or-lookup-ref attr coll key] (do nil))] (do (cas-contains-inner db (to-clojure-types e-or-lookup-ref) (to-clojure-types attr) (to-clojure-types old-val) (to-clojure-types new-val))))}\n}")

(def fns [(read-dbfn cas2)
          (read-dbfn cas)])
