(ns felice.admin
  (:require [clojure.walk :refer [stringify-keys]]
            [clojure.spec.alpha :as s]
            [clojure.string :as str])
  (:import org.apache.kafka.clients.admin.AdminClient
           org.apache.kafka.clients.admin.NewTopic
           org.apache.kafka.common.config.TopicConfig
           org.apache.kafka.common.Node
           org.apache.kafka.common.TopicPartition
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
   (if (s/valid? (s/coll-of :kafka.topic/name) topics)
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
                      :status :kafka.topic/error})))))
     (throw (ex-info "Bad Topics spec" (s/explain-data (s/coll-of :kafka.topic/name) topics))))))


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
      (keep (partial list-consumer-groups-offsets ac) all-group-ids*))))
  ([^AdminClient ac group-id]
   (let [per-topic-offsets
         (some->> (.listConsumerGroupOffsets ac group-id)
                  (.partitionsToOffsetAndMetadata)
                  (.get)
                  (map (fn [[t m]]
                         {:topic-name (.topic t)
                          :partition-name (str t)
                          :partition t
                          :metadata (->offset-metadata m)}))
                  (group-by :topic-name)
                  (not-empty))]
     (when (some? per-topic-offsets)
       {:group-id group-id
        :topics (into {} (map (fn [[t v]]
                                [t (map #(dissoc % :topic-name) v)])
                              per-topic-offsets))}))))


(defn sum-consumer-groups-offsets
  "Sum consumer group offset over all partitions"
  {:added "3.2.0-1.7"}
  ([^AdminClient ac]
   (let [all-group-ids* (map :group-id (list-consumer-groups ac))]
     (into {} (keep (partial sum-consumer-groups-offsets ac) all-group-ids*))))
  ([^AdminClient ac group-id]
   (let [consumer-group* (list-consumer-groups-offsets ac group-id)]
     (when-not (empty? consumer-group*)
       {group-id (->> consumer-group*
                      :topics
                      (map (fn [[topic offsets]]
                             {:topic topic
                              :sum (apply + (map #(get-in % [:metadata :offset])
                                                 offsets))})))}))))


(defn set-consumer-group-topic-offset
  "Alters offsets for the specified group of a specific topic.

   Once alter completed, list the current offset

   Yield nil if no link found between consummers & topic
  "
  {:added "3.2.0-1.7"}
  ([^AdminClient ac ^String group-id ^String topic offset]
   (let [target-offset (OffsetAndMetadata. (long offset))
         old-om (.get (.partitionsToOffsetAndMetadata
                       (.listConsumerGroupOffsets ac group-id)))
         updated-offsets (into {}
                               (keep (fn [[t]]
                                       (when (= topic (.topic t))
                                         {t target-offset}))
                                     old-om))]
     (when-not (empty? updated-offsets)
       (loop [op (.all (.alterConsumerGroupOffsets ac group-id updated-offsets))]
         (if (.isDone op)
           {:group-id group-id
            :topic topic
            :offsets (some->> (list-consumer-groups-offsets ac group-id)
                              :topics
                              (filter (fn [[t]] (= t topic)))
                              first
                              second)}
           (recur op)))))))


(defn delete-consumer-groups
  "Delete consumer groups from the cluster with the default options."
  {:added "3.2.0-1.7"}
  ([^AdminClient ac groups]
   (->> (.deleteConsumerGroups ac groups)
        .deletedGroups
        (map (fn [[group status]]
               (try
                 (.get status)
                 {:group-id group
                  :status :kafka.consumer-group/deleted}
                 (catch Exception e
                   {:group-id group
                    :status :kafka.consumer-group/error
                    :message (.getMessage e)})))))))


(defn delete-consumer-group
  "Delete one consumer group from the cluster
   with the default options."
  {:added "3.2.0-1.7"}
  ([^AdminClient ac group-id]
   (first (delete-consumer-groups ac [group-id]))))

(s/def ::kafka-partition #(instance? TopicPartition %))
(s/def ::kafka-partitions (s/coll-of ::kafka-partition))

(defn delete-consumer-group-offsets
  "Delete committed offsets for a set of partitions in a consumer group.

  NOTE: This will succeed at the partition level only if the group is not
  actively subscribed to the corresponding topic."
  {:added "3.2.0-1.7"}
  ([^AdminClient ac ^String group-id]
   (when-let [partitions* (some->> (list-consumer-groups-offsets ac group-id)
                                   :topics
                                   (mapcat second)
                                   (keep :partition))]
     (delete-consumer-group-offsets ac group-id partitions*)))
  ([^AdminClient ac ^String group-id  partitions]
   (if (s/valid? ::kafka-partitions partitions)
     (let [op (->> (.deleteConsumerGroupOffsets ac group-id (set partitions)))]
       (->> partitions
            (map (fn [^TopicPartition o]
                   (try
                     (.get (.partitionResult op o))
                     {:partition-offset (.toString o)
                      :status :kafka.consumer-group-offset/deleted}
                     (catch Exception e
                       {:partition-offset (.toString o)
                        :status :kafka.consumer-group-offset/error
                        :message (.getMessage e)}))))))
     (throw (ex-info "Bad Partition spec" (s/explain-data ::kafka-partitions partitions))))))
