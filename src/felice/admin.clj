(ns felice.admin
  (:require [clojure.walk :refer [stringify-keys]]
            [clojure.spec.alpha :as s]
            [clojure.string :as str])
  (:import org.apache.kafka.clients.admin.AdminClient
           org.apache.kafka.clients.admin.NewTopic
           org.apache.kafka.common.config.TopicConfig
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


(defn- safely-resolve-field [class f]
  (try (.get (.getField class f) nil) (catch Exception _ nil)))
(defn- static-field->props
  "From a configuration map, try to resolve static class
   field and populate a"
  [m class]
  (reduce
   (fn [acc p]
     (let [[k v]
           (map (fn [e]
                  (->> (str/replace (name e) "." "_")
                       (str/upper-case))) p)
           k? (safely-resolve-field class k)
           v? (or (safely-resolve-field class v) v)]
       (if k? (assoc acc k? v?) acc)))
   (sorted-map)
   m))


(defn- mk-topic-instance
  ^NewTopic
  [^String topic-name partition-count replication-factor props]
  (let [topic* (NewTopic. topic-name (int partition-count) (short replication-factor))]
    (when-not (empty? props)
      (.configs topic*
                (static-field->props props TopicConfig)))
    topic*))

(defn- submit-topic-creation-request
  [^AdminClient ac topic-instances]
  (some->>
      (.createTopics ac topic-instances)
      (.values)
      (map (fn [[k f]]
             (try
               (.get f)
               {:topic k
                :status :kafka.topic/created}
               (catch java.util.concurrent.ExecutionException e
                 {:topic k
                  :message (.getMessage e)
                  :status :kafka.topic/error}))))))

(defn create-topic
  "Create a new topic"
  {:added "3.2.0-1.7"}
  ([^AdminClient ac ^String topic-name partition-count replication-factor]
   (create-topic ac topic-name partition-count replication-factor {}))
  ([^AdminClient ac ^String topic-name partition-count replication-factor ^java.util.Map props]
   (let [topic* (mk-topic-instance topic-name partition-count replication-factor props)]
     (first (submit-topic-creation-request ac [topic*])))))

(s/def :kafka.topic/name string?)
(s/def :kafka.topic/partition-count int?)
(s/def :kafka.topic/replication-factor int?)
(s/def :kafka.topic/props map?)
(s/def ::kafka-topic (s/keys :req-un [:kafka.topic/name
                                      :kafka.topic/partition-count
                                      :kafka.topic/replication-factor
                                      :kafka.topic/props]))
(s/def ::kafka-topics (s/coll-of ::kafka-topic))

(defn create-topics
  "Create new topics from list of objects"
  {:added "3.2.0-1.7"}
  ([^AdminClient ac topics]
   (if (s/valid? ::kafka-topics topics)
     (let [topics* (->> topics
                        (mapv (fn [{:keys [name partition-count replication-factor props] :as t}]
                                (mk-topic-instance name partition-count replication-factor props))))]
       (submit-topic-creation-request ac topics*))
     (throw (ex-info "Bad Topics spec" (s/explain-data ::kafka-topic topics))))))

(defn delete-topics
  "Delete a topic set list"
  {:added "3.2.0-1.7"}
  ([^AdminClient ac topics]
   (->> (.deleteTopics ac topics)
        (.values)
        (map (fn [[k f]]
               (try
                 (.get f)
                 {:topic k
                  :status :kafka.topic/deleted}
                 (catch java.util.concurrent.ExecutionException e
                   {:topic k
                    :message (.getMessage e)
                    :status :kafka.topic/error})))))))


(defn delete-topic
  "Delete a topic"
  {:added "3.2.0-1.7"}
  ([^AdminClient ac topic-name]
   (first (delete-topics ac #{topic-name}))))
