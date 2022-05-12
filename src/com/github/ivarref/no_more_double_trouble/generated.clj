(ns com.github.ivarref.no-more-double-trouble.generated
  (:require [datomic.api]
            [clojure.edn :as edn]))

(defn read-dbfn [s]
  (edn/read-string
    {:readers {'db/id  datomic.db/id-literal
               'db/fn  datomic.function/construct
               'base64 datomic.codec/base-64-literal}}
    s))

(def cas "{:db/ident :nmdt/cas\n :db/fn #db/fn \n{:lang \"clojure\", :requires [[clojure.walk :as walk] [datomic.api :as d]], :imports [(java.util HashSet List) (datomic Database) (datomic.db DbId)], :params [db e-or-lookup-ref attr old-val new-val], :code (letfn [(to-clojure-types [m] (do (walk/prewalk (fn [e] (cond (instance? HashSet e) (into #{} e) (and (instance? List e) (not (vector? e))) (vec e) :else e)) m))) (is-identity? [db attr] (do (= :db.unique/identity (d/q (quote [:find ?ident . :in $ ?e :where [?e :db/unique ?typ] [?typ :db/ident ?ident]]) db attr)))) (is-unique-value? [db attr] (do (= :db.unique/value (d/q (quote [:find ?ident . :in $ ?e :where [?e :db/unique ?typ] [?typ :db/ident ?ident]]) db attr)))) (cas-inner [db e-or-lookup-ref a old-val new-val] (do (cond (string? e-or-lookup-ref) (d/cancel #:cognitect.anomalies{:message \"Entity cannot be string\", :category :cognitect.anomalies/incorrect}) (instance? DbId e-or-lookup-ref) (d/cancel #:cognitect.anomalies{:message \"Entity cannot be tempid/datomic.db.DbId\", :category :cognitect.anomalies/incorrect}) (and (vector? e-or-lookup-ref) (= 2 (count e-or-lookup-ref)) (or (is-identity? db (first e-or-lookup-ref)) (is-unique-value? db (first e-or-lookup-ref)))) (cond (some? (:db/id (d/pull db [:db/id] e-or-lookup-ref))) [[:db/cas e-or-lookup-ref a old-val new-val]] :else (d/cancel #:cognitect.anomalies{:message \"Could not find entity\", :category :cognitect.anomalies/incorrect})) (and (vector? e-or-lookup-ref) (= 4 (count e-or-lookup-ref)) (keyword? (first e-or-lookup-ref)) (= :as (nth e-or-lookup-ref 2)) (string? (last e-or-lookup-ref)) (or (is-identity? db (first e-or-lookup-ref)) (is-unique-value? db (first e-or-lookup-ref)))) (let [e (vec (take 2 e-or-lookup-ref))] (cond (some? (:db/id (d/pull db [:db/id] e))) [[:db/cas e a old-val new-val]] (nil? old-val) [[:db/add (last e-or-lookup-ref) a new-val]] :else (d/cancel #:cognitect.anomalies{:message \"Old-val must be nil for new entities\", :category :cognitect.anomalies/incorrect}))) :else (d/cancel #:cognitect.anomalies{:message \"Unhandled state\", :category :cognitect.anomalies/incorrect}))))] (do (cas-inner db (to-clojure-types e-or-lookup-ref) (to-clojure-types attr) (to-clojure-types old-val) (to-clojure-types new-val))))}\n}")

(def fns [(read-dbfn cas)])
