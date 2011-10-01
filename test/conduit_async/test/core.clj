(ns conduit-async.test.core
  (:use [clojure.test]
        [conduit.core]
        [conduit-async.core])
  (:import (java.lang Thread)))

(deftest test-handle-message
         (let [tp (a-comp
                    (tap (a-arr #(println :x %)))
                    (a-arr inc))
               reply (promise)
               c1 (promise)
               c2 (promise)]
           (deliver c1 reply)
           (is (= ":x 3\n"
                  (with-out-str (handle-message tp {:value 3
                                                    :continuation c1}))))
           (is (= [4] @reply))
           (deliver c2 nil)
           (is (= ":x 8\n"
                  (with-out-str (handle-message tp {:value 8
                                                    :continuation c2}))))))

(deftest test-message-handler
         (let [tp (a-loop
                    (a-arr (fn [[c x]]
                             (+ c x)))
                    0)
               c1 (promise)
               c2 (promise)
               c3 (promise)
               r1 (promise)
               r2 (promise)
               queue (ref [{:value 7 :continuation c1}
                           {:value 3 :continuation c2}
                           {:value 9 :continuation c3}])
               thread (atom (Thread. identity))
               closed? (atom false)]
           (future (message-handler {:queue queue
                                     :thread thread
                                     :handlers (ref {})
                                     :p tp
                                     :closed? closed?}))
           (deliver c1 r1)
           (is (= [7] @r1))
           (deliver c2 nil)
           (deliver c3 r2)
           (is (= [19] @r2))
           (is (not (nil? @thread)))
           (swap! closed? (constantly true))
           (Thread/sleep 1200)
           (is (nil? @thread))))

(deftest test-enqueue-msg
         (deref
           (future
             (let [tp (a-loop
                        (a-arr (fn [[c x]]
                                 (+ c x)))
                        0)
                   c1 (promise)
                   c2 (promise)
                   c3 (promise)
                   r1 (promise)
                   r2 (promise)
                   queue (ref [])
                   thread (atom nil)
                   closed? (atom false)
                   args {:queue queue
                         :thread thread
                         :p tp
                         :closed? closed?}]
               (enqueue-msg args {:value 7 :continuation c1})
               (is (not (nil? @thread)))
               (deliver c1 r1)
               (is (= [7] @r1))
               (enqueue-msg args {:value 3 :continuation c2})
               (enqueue-msg args {:value 9 :continuation c3})
               (deliver c2 nil)
               (deliver c3 r2)
               (is (= [19] @r2))
               (swap! closed? (constantly true))
               (Thread/sleep 1200)
               (is (nil? @thread))))))

(deftest test-a-async
         (deref
           (future
             (let [tp (a-async
                        (a-loop
                          (a-arr (fn [[c x]]
                                   (+ c x)))
                          0))
                   thread (:thread (meta tp))]
               (is (nil? @thread))
               (is (= [3] (wait-for-reply tp 3)))
               (is (not (nil? @thread)))
               (enqueue tp 6)
               (is (= [17] (wait-for-reply tp 8)))
               (close tp)
               (is (= [] (wait-for-reply tp 8)))
               (Thread/sleep 1200)
               (is (= [] (wait-for-reply tp 8)))
               (is (nil? @thread))))))

(run-tests)
