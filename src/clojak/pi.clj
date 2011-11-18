(ns clojak.pi
  (:use clojak.core)
  (:import [java.util.concurrent CountDownLatch]))

(defrecord Calculate [])
(defrecord Work      [start num-elements])
(defrecord Result    [value])

(defn calculate-pi-for [start num-elements]
  (reduce 
    (fn [^double acc ^long i]
      (+ acc (/ (* 4.0 (- 1 (* (mod i 2) 2))) (+ (* 2 i) 1))))
    0.0
    (range (* start num-elements) (- (* (inc start) num-elements) 1))))

(defactor worker [] []
  (onReceive [work]
    (let [result (calculate-pi-for (:start work) (:num-elements work))]
      (.. this getContext (replyUnsafe (Result. result))))))

(defactor-aot clojak.pi.Worker
  :on-receive (fn [this work]
                (let [result (calculate-pi-for (:start work) (:num-elements work))]
                  (.. this getContext (replyUnsafe (Result. result))))))

; Non-AOT
;(defn make-workers [n]
  ;(doall (map (fn [_] (make-actor worker)) (range n))))

; AOT
(defn make-workers [n]
  (doall (map (fn [_]  (make-actor clojak.pi.Worker)) (range n))))

(defactor master [router num-messages num-elements latch]
  [pi          (atom 0.0)
   num-results (atom 0)
   start       (atom 0)]

  (onReceive [message] 
    (cond 
      (instance? Calculate message)
        (do
          (doseq [start (range num-messages)]
            (send-one-way router (Work. start num-elements) this))
          (broadcast router poison-pill)
          (send-one-way router poison-pill))
      (instance? Result message)
        (do
          (swap! pi + (:value message))
          (swap! num-results inc)
          (when (= @num-results num-messages)
            (.. this getContext stop)))
      :else
        (throw (IllegalArgumentException. (str "Unknown message [" message "]")))))

  (preStart [] 
    (reset! start (System/currentTimeMillis)))

  (postStop []
    (println (format "\n\tPi estimate: \t\t %s\n\tCalculation time: \t %s millis"
                      @pi (- (System/currentTimeMillis) @start)))
    (.countDown latch)))

(defn calculate [num-workers num-elements num-messages]
  (let [workers (make-workers num-workers)
        router  (make-actor (load-balancing-router workers))
        latch   (CountDownLatch. 1)
        master  (make-actor master router num-messages num-elements latch)]
    (send-one-way master (Calculate.))
    (.await latch)))

(defn -main [& args]
  (calculate 4 10000 10000))

