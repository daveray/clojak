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

(defmacro defactor-aot
  [name & {:keys [on-receive]
      :or {on-receive '(fn [this msg])}}]
  `(do
    (gen-class
      :name ~(or name (ns-name *ns*))
      :extends akka.actor.UntypedActor 
      :prefix "-clojak-actor-")

    (defn ~'-clojak-actor-onReceive [^akka.actor.UntypedActor this# msg#]
      (~on-receive this# msg#))))

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

