# [No more] double trouble

Handle duplicate Datomic transactions with ease.

## Installation

...

## 1-minute example

```clojure
(require '[com.github.ivarref.double-trouble :as dt])
(require '[datomic.api :as d])

(def conn (d/connect "..."))

; Setup:
@(d/transact conn dt/schema)

```
