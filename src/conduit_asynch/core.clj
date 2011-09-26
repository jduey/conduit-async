(ns conduit-asynch.core
  (:use [conduit.core :only [abort-c]])
  (:import (java.lang Thread)))

(defn handle-message [p msg]
  (let [[new-p c] (p (:value msg))]
    (if-let [reply (:reply msg)]
      (deliver reply (c identity))
      (c nil))
    new-p))

(defn message-handler [{:keys [queue thread closed? p] :as args}]
  (let [msgs (dosync
               (let [msgs @queue]
                 (ref-set queue [])
                 msgs))]
    (if (or (seq msgs)
            (not (deref closed?)))
      (if (empty? msgs)
        (do
          ; make this thread wait on a notification
          (Thread/sleep 1000)
          (recur args))
        (let [new-p (reduce handle-message p msgs)]
          (recur (assoc args :p new-p))))
      (swap! thread (constantly nil)))))

(defn enqueue-msg [{:keys [queue thread closed? p] :as args} msg]
  (let [thread-obj @thread]
    (if (nil? thread-obj)
      (let [new-thread (Thread. #(message-handler args))]
        (when (compare-and-set! thread nil new-thread)
          (.start new-thread)))
      ; notify waiting thread
      #_(.notify thread-obj)))
    (dosync
      (alter queue conj msg)))

(defn a-asynch [p]
  (let [msg-queue (ref [])
        handlers (ref #{})
        closed? (atom false)
        thread (atom nil)
        args {:queue msg-queue
              :closed? closed?
              :thread thread
              :handlers handlers
              :p p}]
    (with-meta
      (fn curr-fn [x]
        (if @closed?
          [curr-fn abort-c]
          [curr-fn (fn [c]
                     (if (nil? c)
                       (enqueue-msg args {:value x})
                       (let [reply (promise)]
                         (enqueue-msg args {:value x :reply reply})
                         (c @reply))))]))
      (-> (meta p)
        (select-keys [:created-by :args])
        (assoc :handlers handlers
               :msg-queue msg-queue
               :thread thread
               :closed? closed?)))))

(defn close [p]
  (swap! (:closed? (meta p)) (constantly true)))

