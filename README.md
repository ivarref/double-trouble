# no-more-double-trouble

Handle duplicate Datomic transactions with ease.

## Installation

...

## 1-minute example

```clojure
(require '[com.github.ivarref.no-more-double-trouble :as nmdt])
(require '[datomic.api :as d])

(def conn (d/connect "..."))

; Setup no-more-double-trouble's schema:
@(d/transact conn nmdt/schema)

```
