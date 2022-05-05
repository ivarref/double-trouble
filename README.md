# no-double-trouble

Handle duplicate Datomic transactions with ease.

## Installation

...

## 1-minute example

```clojure
(require '[com.github.ivarref.no_double_trouble :as ndt])
(require '[datomic.api :as d])

(def conn (d/connect "..."))

; Setup no-double-trouble's schema
@(d/transact conn ndt/schema)

```
