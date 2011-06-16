(ns clojak.core
  (:import [akka.actor Actors UntypedActor UntypedActorFactory]
           [akka.routing Routing$Broadcast CyclicIterator UntypedLoadBalancer]))

(defmacro defactor [name args bindings & body]
  `(defn ~name ~args
    (let ~bindings
      (proxy [UntypedActorFactory] []
        (create []
          (proxy [UntypedActor] []
            ~@body))))))

(defn make-actor [actor & args] 
  (let [actor (if (fn? actor) (apply actor args) actor)]
    (doto (Actors/actorOf actor) .start)))

(defn load-balancing-router [workers]
  (proxy [UntypedActorFactory] []
    (create [] 
      (let [itr (CyclicIterator. workers)]
        (proxy [UntypedLoadBalancer] []
          (seq [] itr))))))

(defn send-one-way 
  ([router message]
    (doto router
      (.sendOneWay message)))
  ([router message from]
    (doto router
      (.sendOneWay message (.getContext from)))))

(defn broadcast
  [router message]
  (send-one-way router (Routing$Broadcast. message)))

(def poison-pill (Actors/poisonPill))

