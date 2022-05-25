(ns com.github.ivarref.cas-should-resolve-tempid-string
  (:require [clojure.test :as test :refer [deftest is]]
            [com.github.ivarref.log-init :as log-init]
            [datomic.api :as d]))

(log-init/init-logging!
  [[#{"datomic.*" "com.datomic.*" "org.apache.*"} :warn]
   [#{"*"} :info]])

(def ^:dynamic *conn* nil)

(def test-schema
  [#:db{:ident :e/id, :cardinality :db.cardinality/one, :valueType :db.type/string :unique :db.unique/identity}
   #:db{:ident :e/info, :cardinality :db.cardinality/one, :valueType :db.type/string}
   #:db{:ident :e/version, :cardinality :db.cardinality/one, :valueType :db.type/long}])

(defn with-new-conn [f]
  (let [conn (let [uri (str "datomic:mem://test-" (random-uuid))]
               (d/delete-database uri)
               (d/create-database uri)
               (d/connect uri))]
    (try
      @(d/transact conn test-schema)
      (binding [*conn* conn]
        (f))
      (finally
        (d/release conn)))))

(test/use-fixtures :each with-new-conn)

(deftest cas-resolves-explicit-tempid
  (let [temp-id (d/tempid :db.part/user)
        {:keys [db-after]} @(d/transact *conn* [[:db/cas temp-id :e/version nil 1]
                                                {:db/id temp-id :e/id "a" :e/info "a"}])]
    (is (= #:e{:version 1} (d/pull db-after [:e/version] [:e/id "a"])))))


(deftest this-should-throw-but-does-not
  @(d/transact *conn* [{:e/id "a" :e/version 1}])
  (let [tempid (d/tempid :db.part/user)
        {:keys [db-after]} @(d/transact *conn* [[:db/cas tempid :e/version nil 2]
                                                {:db/id tempid :e/id "a" :e/info "a"}])]
    ; :e/version should not be 2, but it is:
    (is (= 2 (:e/version (d/pull db-after [:e/version] [:e/id "a"]))))))

; Disable for now:
#_(deftest cas-should-resolve-tempid-string
      @(d/transact *conn* [[:db/cas "b" :e/version nil 1]
                           {:db/id "b" :e/id "b" :e/info "b"}]))

; Full stacktrace when using com.datomic/datomic-pro {:mvn/version "1.0.6397"}:
;
; ERROR in (cas-should-resolve-tempid-string) (promise.clj:10)
;Uncaught exception, not in assertion.
;expected: nil
;  actual: java.util.concurrent.ExecutionException: java.lang.IllegalArgumentException: :db.error/not-a-keyword Cannot interpret as a keyword: b, no leading :
; at datomic.promise$throw_executionexception_if_throwable.invokeStatic (promise.clj:10)
;    datomic.promise$throw_executionexception_if_throwable.invoke (promise.clj:6)
;    datomic.promise$settable_future$reify__7837.deref (promise.clj:54)
;    clojure.core$deref.invokeStatic (core.clj:2337)
;    clojure.core$deref.invoke (core.clj:2323)
;    com.github.ivarref.cas_should_resolve_tempid_string$fn__1933.invokeStatic (cas_should_resolve_tempid_string.clj:32)
;    com.github.ivarref.cas_should_resolve_tempid_string/fn (cas_should_resolve_tempid_string.clj:32)
;    clojure.test$test_var$fn__9856.invoke (test.clj:717)
;    clojure.test$test_var.invokeStatic (test.clj:717)
;    clojure.test$test_var.invoke (test.clj:708)
;    user$eval1938$fn__1977$fn__1978$fn__1979.invoke (form-init14348657061350917771.clj:1)
;    com.github.ivarref.cas_should_resolve_tempid_string$with_new_conn.invokeStatic (cas_should_resolve_tempid_string.clj:20)
;    com.github.ivarref.cas_should_resolve_tempid_string$with_new_conn.invoke (cas_should_resolve_tempid_string.clj:12)
;    clojure.test$compose_fixtures$fn__9850$fn__9851.invoke (test.clj:694)
;    clojure.test$default_fixture.invokeStatic (test.clj:687)
;    clojure.test$default_fixture.invoke (test.clj:683)
;    clojure.test$compose_fixtures$fn__9850.invoke (test.clj:694)
;    user$eval1938$fn__1977$fn__1978.invoke (form-init14348657061350917771.clj:1)
;    clojure.test$default_fixture.invokeStatic (test.clj:687)
;    clojure.test$default_fixture.invoke (test.clj:683)
;    user$eval1938$fn__1977.invoke (form-init14348657061350917771.clj:1)
;    clojure.core$with_redefs_fn.invokeStatic (core.clj:7582)
;    clojure.core$with_redefs_fn.invoke (core.clj:7566)
;    user$eval1938.invokeStatic (form-init14348657061350917771.clj:1)
;    user$eval1938.invoke (form-init14348657061350917771.clj:1)
;    clojure.lang.Compiler.eval (Compiler.java:7194)
;    clojure.lang.Compiler.eval (Compiler.java:7149)
;    clojure.core$eval.invokeStatic (core.clj:3215)
;    clojure.core$eval.invoke (core.clj:3211)
;    nrepl.middleware.interruptible_eval$evaluate$fn__968$fn__969.invoke (interruptible_eval.clj:87)
;    clojure.lang.AFn.applyToHelper (AFn.java:152)
;    clojure.lang.AFn.applyTo (AFn.java:144)
;    clojure.core$apply.invokeStatic (core.clj:667)
;    clojure.core$with_bindings_STAR_.invokeStatic (core.clj:1990)
;    clojure.core$with_bindings_STAR_.doInvoke (core.clj:1990)
;    clojure.lang.RestFn.invoke (RestFn.java:425)
;    nrepl.middleware.interruptible_eval$evaluate$fn__968.invoke (interruptible_eval.clj:87)
;    clojure.main$repl$read_eval_print__9206$fn__9209.invoke (main.clj:437)
;    clojure.main$repl$read_eval_print__9206.invoke (main.clj:437)
;    clojure.main$repl$fn__9215.invoke (main.clj:458)
;    clojure.main$repl.invokeStatic (main.clj:458)
;    clojure.main$repl.doInvoke (main.clj:368)
;    clojure.lang.RestFn.invoke (RestFn.java:1523)
;    nrepl.middleware.interruptible_eval$evaluate.invokeStatic (interruptible_eval.clj:84)
;    nrepl.middleware.interruptible_eval$evaluate.invoke (interruptible_eval.clj:56)
;    nrepl.middleware.interruptible_eval$interruptible_eval$fn__999$fn__1003.invoke (interruptible_eval.clj:152)
;    clojure.lang.AFn.run (AFn.java:22)
;    nrepl.middleware.session$session_exec$main_loop__1067$fn__1071.invoke (session.clj:202)
;    nrepl.middleware.session$session_exec$main_loop__1067.invoke (session.clj:201)
;    clojure.lang.AFn.run (AFn.java:22)
;    java.lang.Thread.run (Thread.java:833)
;Caused by: datomic.impl.Exceptions$IllegalArgumentExceptionInfo: :db.error/not-a-keyword Cannot interpret as a keyword: b, no leading :
;{:cognitect.anomalies/category :cognitect.anomalies/incorrect, :cognitect.anomalies/message "Cannot interpret as a keyword: b, no leading :", :db/error :db.error/not-a-keyword}
; at datomic.error$arg.invokeStatic (error.clj:79)
;    datomic.error$arg.invoke (error.clj:74)
;    datomic.error$arg.invokeStatic (error.clj:77)
;    datomic.error$arg.invoke (error.clj:74)
;    datomic.db$fn__1915.invokeStatic (db.clj:300)
;    datomic.db/fn (db.clj:296)
;    clojure.lang.AFn.applyToHelper (AFn.java:154)
;    clojure.lang.AFn.applyTo (AFn.java:144)
;    clojure.core$apply.invokeStatic (core.clj:667)
;    clojure.core$memoize$fn__6946.doInvoke (core.clj:6388)
;    clojure.lang.RestFn.invoke (RestFn.java:408)
;    datomic.db$to_kw.invokeStatic (db.clj:312)
;    datomic.db$to_kw.invoke (db.clj:306)
;    datomic.db.Db.idOf (db.clj:2332)
;    datomic.db$extended_resolve_id.invokeStatic (db.clj:612)
;    datomic.db$extended_resolve_id.invoke (db.clj:606)
;    datomic.db$resolve_id.invokeStatic (db.clj:621)
;    datomic.db$resolve_id.invoke (db.clj:614)
;    datomic.db$datoms.invokeStatic (db.clj:1046)
;    datomic.db$datoms.invoke (db.clj:1033)
;    datomic.builtins$compare_and_swap.invokeStatic (builtins.clj:80)
;    datomic.builtins$compare_and_swap.invoke (builtins.clj:67)
;    user$eval5880$fn__5881.invoke (form-init14348657061350917771.clj:1)
;    clojure.lang.AFn.applyToHelper (AFn.java:171)
;    clojure.lang.AFn.applyTo (AFn.java:144)
;    clojure.core$apply.invokeStatic (core.clj:669)
;    clojure.core$apply.invoke (core.clj:662)
;    datomic.db.ProcessExpander.inject (db.clj:3368)
;    datomic.db.ProcessInpoint.inject (db.clj:3121)
;    datomic.db$with_tx$inject_all__3298$fn__3299.invoke (db.clj:3499)
;    clojure.lang.PersistentVector.reduce (PersistentVector.java:343)
;    clojure.core$reduce.invokeStatic (core.clj:6885)
;    clojure.core$reduce.invoke (core.clj:6868)
;    datomic.db$with_tx$inject_all__3298.invoke (db.clj:3499)
;    datomic.db$with_tx.invokeStatic (db.clj:3503)
;    datomic.db$with_tx.invoke (db.clj:3492)
;    datomic.peer.LocalConnection/fn (peer.clj:547)
;    datomic.peer.LocalConnection.transactAsync (peer.clj:547)
;    datomic.peer.LocalConnection.transact (peer.clj:539)
;    datomic.api$transact.invokeStatic (api.clj:107)
;    datomic.api$transact.invoke (api.clj:105)
;    com.github.ivarref.cas_should_resolve_tempid_string$fn__1933.invokeStatic (cas_should_resolve_tempid_string.clj:33)
;    com.github.ivarref.cas_should_resolve_tempid_string/fn (cas_should_resolve_tempid_string.clj:32)
;    clojure.test$test_var$fn__9856.invoke (test.clj:717)
;    clojure.test$test_var.invokeStatic (test.clj:717)
;    clojure.test$test_var.invoke (test.clj:708)
;    user$eval1938$fn__1977$fn__1978$fn__1979.invoke (form-init14348657061350917771.clj:1)
;    com.github.ivarref.cas_should_resolve_tempid_string$with_new_conn.invokeStatic (cas_should_resolve_tempid_string.clj:20)
;    com.github.ivarref.cas_should_resolve_tempid_string$with_new_conn.invoke (cas_should_resolve_tempid_string.clj:12)
;    clojure.test$compose_fixtures$fn__9850$fn__9851.invoke (test.clj:694)
;    clojure.test$default_fixture.invokeStatic (test.clj:687)
;    clojure.test$default_fixture.invoke (test.clj:683)
;    clojure.test$compose_fixtures$fn__9850.invoke (test.clj:694)
;    user$eval1938$fn__1977$fn__1978.invoke (form-init14348657061350917771.clj:1)
;    clojure.test$default_fixture.invokeStatic (test.clj:687)
;    clojure.test$default_fixture.invoke (test.clj:683)
;    user$eval1938$fn__1977.invoke (form-init14348657061350917771.clj:1)
;    clojure.core$with_redefs_fn.invokeStatic (core.clj:7582)
;    clojure.core$with_redefs_fn.invoke (core.clj:7566)
;    user$eval1938.invokeStatic (form-init14348657061350917771.clj:1)
;    user$eval1938.invoke (form-init14348657061350917771.clj:1)
;    clojure.lang.Compiler.eval (Compiler.java:7194)
;    clojure.lang.Compiler.eval (Compiler.java:7149)
;    clojure.core$eval.invokeStatic (core.clj:3215)
;    clojure.core$eval.invoke (core.clj:3211)
;    nrepl.middleware.interruptible_eval$evaluate$fn__968$fn__969.invoke (interruptible_eval.clj:87)
;    clojure.lang.AFn.applyToHelper (AFn.java:152)
;    clojure.lang.AFn.applyTo (AFn.java:144)
;    clojure.core$apply.invokeStatic (core.clj:667)
;    clojure.core$with_bindings_STAR_.invokeStatic (core.clj:1990)
;    clojure.core$with_bindings_STAR_.doInvoke (core.clj:1990)
;    clojure.lang.RestFn.invoke (RestFn.java:425)
;    nrepl.middleware.interruptible_eval$evaluate$fn__968.invoke (interruptible_eval.clj:87)
;    clojure.main$repl$read_eval_print__9206$fn__9209.invoke (main.clj:437)
;    clojure.main$repl$read_eval_print__9206.invoke (main.clj:437)
;    clojure.main$repl$fn__9215.invoke (main.clj:458)
;    clojure.main$repl.invokeStatic (main.clj:458)
;    clojure.main$repl.doInvoke (main.clj:368)
;    clojure.lang.RestFn.invoke (RestFn.java:1523)
;    nrepl.middleware.interruptible_eval$evaluate.invokeStatic (interruptible_eval.clj:84)
;    nrepl.middleware.interruptible_eval$evaluate.invoke (interruptible_eval.clj:56)
;    nrepl.middleware.interruptible_eval$interruptible_eval$fn__999$fn__1003.invoke (interruptible_eval.clj:152)
;    clojure.lang.AFn.run (AFn.java:22)
;    nrepl.middleware.session$session_exec$main_loop__1067$fn__1071.invoke (session.clj:202)
;    nrepl.middleware.session$session_exec$main_loop__1067.invoke (session.clj:201)
;    clojure.lang.AFn.run (AFn.java:22)
;    java.lang.Thread.run (Thread.java:833)
;Ran 1 test containing 1 assertion.
;0 failures, 1 error.

(deftest nil-test
  @(d/transact *conn* [{:e/id "a" :e/info "1"}])
  @(d/transact *conn* [[:db/cas [:e/id "a"] :e/version nil 1]]))

(deftest nil-test-2
  @(d/transact *conn* [{:e/id "a" :e/info "1"}])
  (is (= ":db.error/cas-failed Compare failed: 2 " (err-msg @(d/transact *conn* [[:db/cas [:e/id "a"] :e/version 2 1]])))))
