(ns conduit-asynch.test.core
  (:use [clojure.test]
        [conduit.core]
        [conduit-asynch.core])
  (:import (java.lang Thread)))

(deftest test-handle-message
         (let [tp (a-comp
                    (tap (a-arr #(println :x %)))
                    (a-arr inc))
               reply (promise)]
           (is (= ":x 3\n"
                  (with-out-str (handle-message tp {:value 3 :reply reply}))))
           (is (= [4] @reply))
           (is (= ":x 8\n"
                  (with-out-str (handle-message tp {:value 8}))))))

(deftest test-message-handler
         (let [tp (a-loop
                    (a-arr (fn [[c x]]
                             (+ c x)))
                    0)
               r1 (promise)
               r2 (promise)
               queue (ref [{:value 7 :reply r1}
                           {:value 3}
                           {:value 9 :reply r2}])
               thread (atom (Thread. identity))
               closed? (atom false)]
           (future (message-handler {:queue queue
                                     :thread thread
                                     :p tp
                                     :closed? closed?}))
           (is (= [7] @r1))
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
                   r1 (promise)
                   r2 (promise)
                   queue (ref [])
                   thread (atom nil)
                   closed? (atom false)
                   args {:queue queue
                         :thread thread
                         :p tp
                         :closed? closed?}]
               (enqueue-msg args {:value 7 :reply r1})
               (is (not (nil? @thread)))
               (is (= [7] @r1))
               (enqueue-msg args {:value 3})
               (enqueue-msg args {:value 9 :reply r2})
               (is (= [19] @r2))
               (swap! closed? (constantly true))
               (Thread/sleep 1200)
               (is (nil? @thread))))))

(deftest test-a-asynch
         (deref
           (future
             (let [tp (a-asynch
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
