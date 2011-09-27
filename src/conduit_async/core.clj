(ns conduit-async.core
  (:use [conduit.core :only [abort-c]])
  (:import (java.lang Thread)))

(defn handle-message [handlers p {:keys [value continuation]}]
  (when handlers
    (let [_ (prn :handlers @handlers)
          new-handlers (reduce (fn [hs [tag h]]
                                 (prn :hs hs :tag tag :h h)
                                 (let [[new-h c] (h value)]
                                   (prn :new-h new-h :c c)
                                   (c nil)
                                   (if new-h
                                     (assoc hs tag new-h)
                                     hs)))
                               {}
                               @handlers)]
      (dosync (ref-set handlers new-handlers))))
  (if (not= p [])
    (let [[new-p c] (p value)]
      (if-let [reply @continuation]
        (deliver reply (c identity))
        (c nil))
      new-p)
    p))

(defn message-handler [{:keys [queue thread closed? handlers p] :as args}]
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
        (let [new-p (reduce (partial handle-message handlers) p msgs)]
          (recur (assoc args :p new-p))))
      (swap! thread (constantly nil)))))

(defn enqueue-msg [{:keys [queue thread] :as args} msg]
  (let [thread-obj @thread]
    (if (nil? thread-obj)
      (let [new-thread (Thread. #(message-handler args))]
        (when (compare-and-set! thread nil new-thread)
          (.start new-thread)))
      ; notify waiting thread
      #_(.notify thread-obj)))
    (dosync
      (alter queue conj msg)))

(defn a-async [& [p]]
  (let [p (or p [])
        msg-queue (ref [])
        handlers (ref {})
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
          (let [continuation (promise)]
            (enqueue-msg args {:value x :continuation continuation})
            [curr-fn (fn [c]
                         (if (nil? c)
                           (deliver continuation nil)
                           (let [reply (promise)]
                             (deliver continuation reply)
                             (c @reply))))])))
      (-> (meta p)
        (select-keys [:created-by :args])
        (assoc :handlers handlers
               :msg-queue msg-queue
               :thread thread
               :closed? closed?)))))

(defn close [p]
  (swap! (:closed? (meta p)) (constantly true)))

(defn fork [p handler]
  (dosync
    (alter (:handlers (meta p)) assoc handler handler)))

(defn receive [p f]
  (let [handler (fn [x]
                  (f x)
                  [nil abort-c])]
    (dosync
      (alter (:handlers (meta p)) assoc f handler)))) 

(defn receive-all [p f]
  (let [handler (fn curr-fn [x]
                  (f x)
                  [curr-fn abort-c])]
    (dosync
      (alter (:handlers (meta p)) assoc f handler))))

(defn cancel-callback [p handler]
  (dosync
      (alter (:handlers (meta p)) dissoc handler)))
