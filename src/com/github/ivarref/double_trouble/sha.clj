(ns com.github.ivarref.double-trouble.sha
  (:require [clojure.walk :as walk]
            [clojure.edn :as edn]
            [clojure.string :as str])
  (:import (java.security MessageDigest)
           (java.nio.charset StandardCharsets)))

;; Code "borrowed" from
;; * http://www.holygoat.co.uk/blog/entry/2009-03-26-1
;; * http://www.rgagnon.com/javadetails/java-0416.html
;; * https://github.com/clj-commons/digest/blob/master/src/clj_commons/digest.clj
(defn digest [^"[B" message]
  (let [^MessageDigest algo (MessageDigest/getInstance "sha-1")]
    (.reset algo)
    (.update algo message)
    (let [size (* 2 (.getDigestLength algo))
          sig (.toString (BigInteger. 1 (.digest algo)) 16)
          padding (str/join (repeat (- size (count sig)) "0"))]
      (str padding sig))))

(defn deterministic-order [x]
  (cond (map? x)
        (into (sorted-map) x)

        (set? x)
        (into (sorted-set) x)

        :else
        x))

(defn pr-str-safe [x]
  (let [printed (pr-str x)]
    (if (= x (edn/read-string printed))
      printed
      (throw (ex-info (str "Input not properly pr-str-able") {:input x})))))

(defn deterministic-str [m]
  (binding [*print-dup* false
            *print-meta* false
            *print-readably* true
            *print-length* nil
            *print-level* nil
            *print-namespace-maps* false]
    (pr-str-safe (walk/prewalk deterministic-order m))))

(defn sha-1 [m]
  (digest (.getBytes ^String (deterministic-str m) StandardCharsets/UTF_8)))
