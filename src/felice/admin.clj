(ns felice.admin
  (:require [clojure.walk :refer [stringify-keys]])
  (:import org.apache.kafka.clients.admin.AdminClient
           org.apache.kafka.common.Node))


(defn admin-client
  "Instanciate an `AdminClient` from properties"
  {:added "3.2.0-1.7"}
  ^AdminClient
  ([props]
   (let [props* (-> (stringify-keys props)
                    (dissoc :key.deserializer :value.deserializer :topics))
         kac (. AdminClient (create ^java.util.Map props*))]
     kac)))


(defn admin-close
  "Manual helper to close `AdminClient` outside the
   `with-open` context"
  {:added "3.2.0-1.7"}
  ([^AdminClient ac]
   (.close ac)))


(defn describe-cluster
  "Describe the current cluster"
  {:added "3.2.0-1.7"}
  ([^AdminClient ac]
   (letfn [(coerce-node [^Node node]
             (cond-> {:host (.host node)
                      :port (.port node)
                      :id (.id node)}
               (.hasRack node) (assoc :rack (.rack node))))]
     (let [desc (.describeCluster ac)
           aop (.authorizedOperations desc)
           cluster-id (.clusterId desc)]
       {:cluster-id @cluster-id
        :authorized-operation @aop
        :controller-node (coerce-node (deref (.controller desc)))
        :nodes (map coerce-node (deref (.nodes desc)))}))))


(defn list-topics
  "List topics for the current `AdminClient` connection"
  {:added "3.2.0-1.7"}
  ([^AdminClient ac]
   (some->> (.listTopics ac)
            (.names)
            deref)))


(defn list-consumer-groups
  "List the consumer groups for the current `AdminClient`
   connection"
  {:added "3.2.0-1.7"}
  ([^AdminClient ac]
   (some->> (.listConsumerGroups ac)
            (.all)
            deref
            (map (fn [o]
                   {:group-id (.groupId o)
                    :is-simple-consumer-group (.isSimpleConsumerGroup o)})))))


(defn list-consumer-groups-offsets
  "List consumer group offsets, if no group id specified,
   compute for all the group-id well-known in the current
   cluster connection."
  {:added "3.2.0-1.7"}
  ([^AdminClient ac group-id]
   (some->> (.listConsumerGroupOffsets ac group-id)
            (.partitionsToOffsetAndMetadata)
            deref))
  ([^AdminClient ac]
   (let [all-group-ids* (map :group-id (list-consumer-groups ac))]
     (doall
      (keep (partial list-consumer-groups-offsets ac) all-group-ids*)))))
