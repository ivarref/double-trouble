(ns com.github.ivarref.double-trouble.generated
  (:require [clojure.edn :as edn]
            [datomic.api]))

; Generated code below, do not edit:
(def generated {:nmdt/cas "{:db/ident :nmdt/cas :db/fn #db/fn {:lang \"clojure\", :requires [[clojure.walk :as walk] [datomic.api :as d]], :imports [(java.util HashSet List) (datomic.db DbId)], :params [db e-or-lookup-ref attr old-val new-val sha], :code (let [] (letfn [(to-clojure-types [m] (do (walk/prewalk (fn [e] (cond (instance? String e) e (instance? HashSet e) (into #{} e) (and (instance? List e) (not (vector? e))) (vec e) :else e)) m))) (is-identity? [db attr] (do (= :db.unique/identity (d/q (quote [:find ?ident . :in $ ?e :where [?e :db/unique ?typ] [?typ :db/ident ?ident]]) db attr)))) (is-unique-value? [db attr] (do (= :db.unique/value (d/q (quote [:find ?ident . :in $ ?e :where [?e :db/unique ?typ] [?typ :db/ident ?ident]]) db attr)))) (get-val [db e a] (do (d/q (quote [:find ?v . :in $ ?e ?a :where [?e ?a ?v]]) db e a))) (sha-exists? [db sha] (do (some? (d/q (quote [:find ?tx . :in $ ?sha :where [?tx :com.github.ivarref.no-more-double-trouble/sha-1 ?sha]]) db sha)))) (cas-inner-2 [db lookup-ref a old-val new-val sha] (do (if-let [e (:db/id (d/pull db [:db/id] lookup-ref))] (let [curr-val (get-val db e a)] (if (= curr-val old-val) (if (sha-exists? db sha) (d/cancel {:com.github.ivarref.no-more-double-trouble/sha sha, :cognitect.anomalies/message (str \"SHA already exists! SHA: \" sha), :cognitect.anomalies/category :cognitect.anomalies/incorrect, :com.github.ivarref.no-more-double-trouble/lookup-ref lookup-ref}) [[:db/add lookup-ref a new-val] {:com.github.ivarref.no-more-double-trouble/sha-1 sha, :db/id \"datomic.tx\"}]) (let [new-val-write (d/q (quote [:find ?new-val . :in $ ?e ?a ?old-val ?sha :where [?e ?a ?old-val ?tx false] [?e ?a ?new-val ?tx true] [?tx :com.github.ivarref.no-more-double-trouble/sha-1 ?sha ?tx true]]) (d/history db) e a old-val sha)] (if (some? new-val-write) (d/cancel {:com.github.ivarref.no-more-double-trouble/a a, :com.github.ivarref.no-more-double-trouble/sha sha, :cognitect.anomalies/message \"Can recover\", :cognitect.anomalies/category :cognitect.anomalies/conflict, :com.github.ivarref.no-more-double-trouble/e e, :com.github.ivarref.no-more-double-trouble/new-val-write new-val-write, :com.github.ivarref.no-more-double-trouble/old-val old-val, :com.github.ivarref.no-more-double-trouble/new-val new-val}))))) (d/cancel {:cognitect.anomalies/message \"Could not find entity\", :cognitect.anomalies/category :cognitect.anomalies/incorrect, :com.github.ivarref.no-more-double-trouble/lookup-ref lookup-ref})))) (cas-inner [db e-or-lookup-ref a old-val new-val sha] (do (cond (string? e-or-lookup-ref) (d/cancel {:cognitect.anomalies/message \"Entity cannot be string\", :cognitect.anomalies/category :cognitect.anomalies/incorrect}) (instance? DbId e-or-lookup-ref) (d/cancel {:cognitect.anomalies/message \"Entity cannot be tempid/datomic.db.DbId\", :cognitect.anomalies/category :cognitect.anomalies/incorrect}) (and (vector? e-or-lookup-ref) (= 2 (count e-or-lookup-ref)) (or (is-identity? db (first e-or-lookup-ref)) (is-unique-value? db (first e-or-lookup-ref)))) (cas-inner-2 db e-or-lookup-ref a old-val new-val sha) (and (vector? e-or-lookup-ref) (= 4 (count e-or-lookup-ref)) (keyword? (first e-or-lookup-ref)) (= :as (nth e-or-lookup-ref 2)) (string? (last e-or-lookup-ref)) (or (is-identity? db (first e-or-lookup-ref)) (is-unique-value? db (first e-or-lookup-ref)))) (let [e (vec (take 2 e-or-lookup-ref))] (cond (some? (:db/id (d/pull db [:db/id] e))) [[:db/cas e a old-val new-val]] (nil? old-val) [[:db/add (last e-or-lookup-ref) a new-val]] :else (d/cancel {:cognitect.anomalies/message \"Old-val must be nil for new entities\", :cognitect.anomalies/category :cognitect.anomalies/incorrect}))) :else (d/cancel {:cognitect.anomalies/message \"Unhandled state\", :cognitect.anomalies/category :cognitect.anomalies/incorrect}))))] (do (cas-inner db (to-clojure-types e-or-lookup-ref) (to-clojure-types attr) (to-clojure-types old-val) (to-clojure-types new-val) (to-clojure-types sha)))))}}"})
; End of generated code

(def schema
  (mapv (fn [s]
          (edn/read-string
            {:readers {'db/id  datomic.db/id-literal
                       'db/fn  datomic.function/construct}}
            s))
        (vals generated)))