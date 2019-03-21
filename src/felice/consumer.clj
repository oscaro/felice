(ns felice.consumer
  (:require [clojure.walk :as walk]
            [felice.serialization :refer [deserializer]])
  (:import [org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecords ConsumerRecord]
           [org.apache.kafka.clients.consumer OffsetAndMetadata]
           [org.apache.kafka.common TopicPartition Metric]
           org.apache.kafka.common.errors.WakeupException
           java.time.Duration))

(defn commit-sync [^KafkaConsumer consumer]
  (.commitSync consumer))

(defn commit-message-offset [^KafkaConsumer consumer {:keys [partition topic offset]}]
  (let [commit-point (long (inc offset))]
    (.commitSync consumer {(TopicPartition. topic partition)
                           (OffsetAndMetadata. commit-point)})))

(defn metric->map [^Metric metric]
  (let [metric-name (.metricName metric)]
    {:name        (.name metric-name)
     :tags        (.tags metric-name)
     :group       (.group metric-name)
     :description (.description metric-name)
     :value       (.metricValue metric)}))

(defn metrics [^KafkaConsumer consumer]
  (map (fn [m] (metric->map (.getValue m))) (.metrics consumer)))

(defn partitions-for [^KafkaConsumer consumer])

(defn subscription [^KafkaConsumer consumer] (.subscription consumer))

(defn subscribe [^KafkaConsumer consumer & topics]
  (.subscribe consumer (concat (subscription consumer) topics))
  consumer)

(defn unsubscribe [^KafkaConsumer consumer]
  "Unsubscribe from topics currently subscribed"
  (.unsubscribe consumer)
  consumer)

(defn position [^KafkaConsumer consumer topic partition])

(defn poll [^KafkaConsumer consumer timeout]
  "Fetch data for the topics or partitions specified using one of the subscribe/assign APIs. This method returns immediately if there are records available. Otherwise, it will await the timeout ms. If the timeout expires, an empty record set will be returned."
  (.poll consumer (Duration/ofMillis timeout)))

(defn wakeup [^KafkaConsumer consumer]
  "Wakeup the consumer."
  (.wakeup consumer))


(defn topic-partition->map [^TopicPartition topic-partition]
  "converts a TopicPartition object to a clojure map with :topic and :partition"
  {:partition (.partition topic-partition)
   :topic     (.topic topic-partition)})

(defn consumer-record->map
  [^ConsumerRecord record]
  {:key            (.key record)
   :offset         (.offset record)
   :partition      (.partition record)
   :timestamp      (.timestamp record)
   :timestamp-type (.name (.timestampType record))
   :headers        (.toArray (.headers record))
   :topic          (.topic record)
   :value          (.value record)})

(defn consumer-records->all-records-old
  [^ConsumerRecords records]
  (->> (.partitions records)
       (mapcat (fn [^TopicPartition tp]
                 (map consumer-record->map (.records records (.topic tp)))))))

(defn consumer-records->all-records
  [^ConsumerRecords records]
  (map consumer-record->map (iterator-seq (.iterator records))))


(defn consumer-records->records-by-topic
  [^ConsumerRecords records]
  (let [topics (map (comp :topic topic-partition->map) (.partitions records))]
    (->> topics
         (map (fn[topic] [topic (map consumer-record->map (.records records topic))]))
         (into {}))))

(defn consumer-records->records-by-partition
  [^ConsumerRecords records])

(defn poll-and-process [^KafkaConsumer consumer timeout process]
  (let [records (-> (poll consumer timeout)
                    (consumer-records->all-records))]
    (doseq [record records]
      (process record))))

(defn poll-loop
  "
### Parameters

         `consumer`: consumer context
     `poll-timeout`: ms between two events poll
`process-record-fn`: function to call with each record polled
      `auto-close?`: close the consumer on exit

### Returns

`stop-fn`: callback function to stop the loop
"
  [consumer poll-timeout process-record-fn auto-close?]
  (let [continue?  (atom true)
        completion (future
                     (try
                       (while @continue?
                         (try
                           (poll-and-process consumer poll-timeout process-record-fn)
                           (catch WakeupException _)))
                       :ok
                       (catch Throwable t t)
                       (finally
                         (when auto-close?
                           (.close consumer)))))]
    (fn []
      (reset! continue? false)
      (deref completion))))



(defn consumer
  "create a consumer"
  ([conf]
    (KafkaConsumer. (walk/stringify-keys conf)))
  ([conf key-deserializer value-deserializer]
    (KafkaConsumer. (walk/stringify-keys conf)
                    (deserializer key-deserializer)
                    (deserializer value-deserializer)))
  ([conf key-deserializer value-deserializer topics]
    (apply subscribe (consumer conf key-deserializer value-deserializer) topics)))


(defn close!
  ([^KafkaConsumer consumer])
  ([^KafkaConsumer consumer timeout]))
