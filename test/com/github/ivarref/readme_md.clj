(ns com.github.ivarref.readme-md)

(require '[com.github.ivarref.double-trouble :as dt])
(require '[datomic.api :as d])

(def conn (let [uri (str "datomic:mem://README-example")]
            (d/delete-database uri)
            (d/create-database uri)
            (d/connect uri)))

; Setup:
@(d/transact conn dt/schema)

(def example-schema
  [#:db{:ident :e/id, :cardinality :db.cardinality/one, :valueType :db.type/string :unique :db.unique/identity}
   #:db{:ident :e/version, :cardinality :db.cardinality/one, :valueType :db.type/long}
   #:db{:ident :e/info, :cardinality :db.cardinality/one, :valueType :db.type/string}])
@(d/transact conn example-schema)

; Add sample data:
@(d/transact conn [{:e/id "my-id" :e/version 1 :e/info "Initial version"}])

; Sample payload:
(def payload {:e/id      "my-id"
              :e/info    "Second version"
              :e/version 1})

; Initial commit using :dt/cas is fine:
@(dt/transact conn [(dissoc payload :e/version)
                    [:dt/cas [:e/id "my-id"] :e/version 1 2 (dt/sha payload)]])
; => {:db-before datomic.db.Db@a108c315
;     :db-after datomic.db.Db@260c18eb
;     :tx-data [#datom[13194139534317 50 #inst"2022-05-21T14:57:18.466-00:00" 13194139534317 true]
;               #datom[17592186045420 75 "Second version" 13194139534317 true]
;               #datom[17592186045420 75 "Initial version" 13194139534317 false]
;               #datom[17592186045420 74 2 13194139534317 true]
;               #datom[17592186045420 74 1 13194139534317 false]
;               #datom[13194139534317 72 "ab25e167099299b1d813c9bab401be1ca15b64e1" 13194139534317 true]],
;     :tempids {-9223301668109598104 17592186045420, "datomic.tx" 13194139534317},
;     :transacted? true}
; Notice the key :transacted? with value true

; Transacting one more time is also fine:
@(dt/transact conn [(dissoc payload :e/version)
                    [:dt/cas [:e/id "my-id"] :e/version 1 2 (dt/sha payload)]])
; => {:db-before datomic.db.Db@d06fccb2
;     :db-after datomic.db.Db@d06fccb1
;     :transacted? false}
; Notice the key :transacted? with value false

; Why did the above succeed?
; :dt/cas detected that:
; :e/version 1 -> 2 had already been asserted in a previous transaction and
; the sha asserted in that previous transaction is identical to the sha
; in the current transaction.
; Thus this is a duplicate transaction that should be allowed.
; :dt/cas throws an exception indicating this.

; dt/transact, yes, that is com.github.ivarref.double-trouble/transact,
; catches this particular exception, and returns a result as if the
; transaction had succeeded: a map with the keys :db-before, :db-after and
; :transacted? (false).

(try
  @(d/transact conn [(dissoc payload :e/version)
                     [:dt/cas [:e/id "my-id"] :e/version 1 2 (dt/sha payload)]])
  (catch Exception e
    (if (dt/already-transacted? e)
      :ok
      '...handle-exception...)))


(try
  (let [{:keys [transacted?]} @(dt/transact conn [(dissoc payload :e/version)
                                                  [:dt/cas [:e/id "my-id"] :e/version 1 2 (dt/sha payload)]])]
    {:status (if transacted? 201 200) :body {:message "OK"}})
  (catch Exception e
    (if (dt/cas-failure? e :e/version)
      {:status 409 :body {:message "Conflict"}}
      {:status 500 :body {:message ""}})))
