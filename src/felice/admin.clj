(ns felice.admin
  (:require [clojure.walk :refer [stringify-keys]]
            [clojure.spec.alpha :as s]
            [clojure.string :as str])
  (:import org.apache.kafka.clients.admin.AdminClient
           org.apache.kafka.clients.admin.NewTopic
           org.apache.kafka.common.config.TopicConfig
           org.apache.kafka.common.Node
           org.apache.kafka.common.TopicPartitionInfo
           org.apache.kafka.clients.consumer.OffsetAndMetadata))

(defn- ->node
  [^Node node]
  (cond-> {:host (.host node)
           :port (.port node)
           :id (.id node)}
    (.hasRack node) (assoc :rack (.rack node))))

(defn- ->topic-partition
  [^TopicPartitionInfo partition]
  {:isr (map ->node (.isr partition))
   :leader (->node (.leader partition))
   :replicas (map ->node (.replicas partition))})

(defn- ->offset-metadata
  [^OffsetAndMetadata offset-metadata]
  {:metadata (.metadata offset-metadata)
   :offset (.offset offset-metadata)})

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
  "Close the Admin client and release all associated resources.

   The close operation has a grace period during which current operations
   will be allowed to complete, specified by the given duration.
   New operations will not be accepted during the grace period.

   Once the grace period is over, all operations that have not yet been
   completed will be aborted with a TimeoutException."
  {:added "3.2.0-1.7"}
  ([^AdminClient ac]
   (.close ac)))


(defn describe-cluster
  "Get information about the nodes in the cluster,
   using the default options."
  {:added "3.2.0-1.7"}
  ([^AdminClient ac]
   (let [desc (.describeCluster ac)
         aop (.authorizedOperations desc)
         cluster-id (.clusterId desc)]
     {:cluster-id @cluster-id
      :authorized-operation @aop
      :controller-node (->node (deref (.controller desc)))
      :nodes (map ->node (deref (.nodes desc)))})))


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
            (.get)
            (map (fn [o]
                   {:group-id (.groupId o)
                    :is-simple-consumer-group (.isSimpleConsumerGroup o)
                    :state (keyword (.orElse (.state o) "unknown"))})))))


(defn list-consumer-groups-offsets
  "List consumer group offsets, if no group id specified,
   compute for all the group-id well-known in the current
   cluster connection."
  {:added "3.2.0-1.7"}
  ([^AdminClient ac]
   (let [all-group-ids* (map :group-id (list-consumer-groups ac))]
     (doall
      (mapcat (partial list-consumer-groups-offsets ac) all-group-ids*))))
  ([^AdminClient ac group-id]
   (some->> (.listConsumerGroupOffsets ac group-id)
            (.partitionsToOffsetAndMetadata)
            (.get)
            (map (fn [[t m]]
                   {:topic-name (.topic t)
                    :partition (str t)
                    :metadata (->offset-metadata m)}))
            (group-by :topic-name)
            (map (fn [[t m]]
                   {:topic t
                    :offsets m}))
            (map (fn [to]
                   {:group-id group-id
                    :topics to})))))


(defn list-consumer-groups-offsets-sum
  "Sum consumer group offset over all partitions"
  ([^AdminClient ac]
   (let [all-group-ids* (map :group-id (list-consumer-groups ac))]
     (into {} (keep (partial list-consumer-groups-offsets-sum ac) all-group-ids*))))
  ([^AdminClient ac group-id]
   (let [consumer-group* (list-consumer-groups-offsets ac group-id)]
     (when-not (empty? consumer-group*)
       {group-id (->> consumer-group*
                  (map (fn [{:keys [topics]}]
                         (let [{:keys [topic offsets]} topics]
                           {:topic topic
                            :sum (apply + (keep #(get-in % [:metadata :offset]) offsets))}))))}))))


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
  "Create new topics from list of objects

  This operation is not transactional so it may succeed for some
  topics while fail for others. "
  {:added "3.2.0-1.7"}
  ([^AdminClient ac topics]
   (if (s/valid? ::kafka-topics topics)
     (let [topics* (->> topics
                        (mapv (fn [{:keys [name partition-count replication-factor props] :as t}]
                                (mk-topic-instance name partition-count replication-factor props))))]
       (submit-topic-creation-request ac topics*))
     (throw (ex-info "Bad Topics spec" (s/explain-data ::kafka-topic topics))))))

(defn delete-topics
  "Delete a topic set list

  This operation is not transactional so it may succeed for some topics
  while fail for others. "
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


(defn describe-topics
  "Describe some topics in the cluster.

   If not topic list provided, describe all the topics in
   the cluster."
  {:added "3.2.0-1.7"}
  ([^AdminClient ac]
   (let [topics* (list-topics ac)]
     (describe-topics ac topics*)))
  ([^AdminClient ac topic-list]
   (let [op (.allTopicNames (.describeTopics ac topic-list))]
     (->> (into {} (.get op))
          (reduce (fn [acc [name o]]
                    (assoc acc name
                           {:uuid       (str (.topicId o))
                            :partitions (map ->topic-partition (.partitions o))}))
                  (sorted-map))))))


(defn describe-topic
  "Describe a topic."
  {:added "3.2.0-1.7"}
  ([^AdminClient ac topic]
   (first (describe-topic ac #{topic}))))


(defn admin-metrics
  "Get the metrics kept by the adminClient"
  {:added "3.2.0-1.7"}
  ([^AdminClient ac]
   (some->> (.metrics ac)
            (map #(.getValue %))
            (map (fn [o]
                   (let [name (.metricName o)]
                     {:name (.name name)
                      :group (.group name)
                      :description (.description name)
                      :tags (.tags name)
                      :value (.metricValue o)}))))))
