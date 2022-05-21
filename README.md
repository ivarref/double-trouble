# <img align="right" src="the_clash.png" width="200" height="200"> [No more] double trouble

Handle duplicate Datomic transactions with ease.
A modified compare-and-swap function that handles for duplicates.

## Installation

...

## 2-minute example

```clojure
(require '[com.github.ivarref.double-trouble :as dt])
(require '[datomic.api :as d])

(def conn (let [uri (str "datomic:mem://README-example")]
            (d/delete-database uri)
            (d/create-database uri)
            (d/connect uri)))

; Setup:
@(d/transact conn dt/schema)
(def example-schema [#:db{:ident :e/id, :cardinality :db.cardinality/one, :valueType :db.type/string :unique :db.unique/identity}
                     #:db{:ident :e/version, :cardinality :db.cardinality/one, :valueType :db.type/long}
                     #:db{:ident :e/info, :cardinality :db.cardinality/one, :valueType :db.type/string}])
@(d/transact conn example-schema)

; Add sample data:
@(d/transact conn [{:e/id "my-id" :e/version 1 :e/info "Initial version"}])

; Sample payload:
(def payload {:e/id "my-id"
              :e/info "Second version"
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
; that the sha asserted in that previous transaction matched the sha
; in the current transaction.
; Thus this is a duplicate transaction that should be allowed.
; :dt/cas throws an exception indicating this.

; dt/transact, yes, that is com.github.ivarref.double-trouble/transact,
; catches this particular exception, and returns a result as if the
; transaction had succeeded: a map with the keys :db-before, :db-after and
; :transacted? (false).
```

## Motivation

> The network is reliable

The first assertion of [fallacies of distributed computing](https://en.wikipedia.org/wiki/Fallacies_of_distributed_computing).

`:db/cas`, compare-and-swap, can be used as a lock/protection
if you want to disallow concurrent edits on some entity.
Imagine the following scenario where a network failure
occurs after a transaction has successfully completed:

```
1. The client sends HTTP request to Clojure backend.
2. Backend issues @(d/transact ... [[:db/cas [...] :e/version 1 2]]) to the Datomic transactor.
3. The transactor writes successfully to the storage, e.g. PostgreSQL.
; Everything fine so far.

4. The network between the Clojure backend and the Datomic transactor goes down.
5. The invocation of d/transact throws an exception, and the backend returns HTTP status code 500.
6. The network between the Clojure backend and the Datomic transactor comes back up.

; Now what?

7. The client retries the HTTP request.
8. Backend issues an identical @(d/transact ...).
9. Now d/transact throws a cas exception.
10. Backend returns what?

; The client is now stuck.
; What should it do?
```

The aim of this library is to handle such scenarios.
Had `:dt/cas` (and not `:db/cas`) been used, the retry would have
been successful. 

## Usage

`:dt/cas` takes 5 arguments, one more than `:db/cas`:
1. The entity id or lookup ref.
2. The attribute to change.
3. The old value.
4. The new value.
5. The sha representing the essence of the transaction.

The first four arguments are identical to Datomic's cas.

The last argument, the sha, should represent the essence of the transaction. 
It will be used to check if the transaction has already been committed in
the event of a cas failure, i.e. if this transaction has a duplicate.

You may use `(dt/sha payload)` to generate a sha. `my-data` must obviously
be identical for transactions that should be considered identical.
