;; A thin layer on top of java [KafkaConsumer](https://kafka.apache.org/21/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html)

(ns felice.consumer
  (:require [clojure.walk :as walk]
            [felice.serialization :refer [deserializer]])
  (:import [org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecords ConsumerRecord]
           [org.apache.kafka.clients.consumer OffsetAndMetadata]
           [org.apache.kafka.common TopicPartition Metric]
           org.apache.kafka.common.errors.WakeupException
           java.time.Duration))

;; ## COMMIT FUNCTIONS

(defn commit-sync
  "Commit offsets returned on the last `poll()` for all the subscribed list of topics and partitions.

  consumer must be a KafkaConsumer object"
  [^KafkaConsumer consumer]
  (.commitSync consumer))

(defn commit-message-offset
  "Commit a specific record

  consumer must be a KafkaConsumer object

  record must be a map with :partition :topic and :offset"
  [^KafkaConsumer consumer {:keys [partition topic offset] :as record}]
  (let [commit-point (long (inc offset))]
    (.commitSync consumer {(TopicPartition. topic partition)
                           (OffsetAndMetadata. commit-point)})))

(defn ^:no-doc metric->map [^Metric metric]
  (let [metric-name (.metricName metric)]
    {:name        (.name metric-name)
     :tags        (.tags metric-name)
     :group       (.group metric-name)
     :description (.description metric-name)
     :value       (.metricValue metric)}))

(defn metrics
  "returns a list of mtrics mapkept by the consumer"
  [^KafkaConsumer consumer]
  (map (fn [m] (metric->map (.getValue m))) (.metrics consumer)))

(defn ^:no-doc partitions-for [^KafkaConsumer consumer])

(defn subscription
  "returns the set of currenctly subscribed topics"
  [^KafkaConsumer consumer]
  (.subscription consumer))

(defn subscribe
  "subscribe the consumer to one or more topics
  automaticly resubscribes previous subscriptions
  returns the consumer"
  [^KafkaConsumer consumer & topics]
  (.subscribe consumer (concat (subscription consumer) topics))
  consumer)

(defn unsubscribe
  "Unsubscribe from all topics currently subscribed
  returns the consumer"
  [^KafkaConsumer consumer]
  (.unsubscribe consumer)
  consumer)

(defn ^:no-doc position [^KafkaConsumer consumer topic partition])

(defn poll
  "Fetch data for the topics or partitions specified using one of the subscribe/assign APIs. This method returns immediately if there are records available. Otherwise, it will await the timeout ms. If the timeout expires, an empty record set will be returned."
  [^KafkaConsumer consumer timeout]
  (.poll consumer (Duration/ofMillis timeout)))

(defn wakeup
  "Wakeup the consumer. This method is thread-safe and is useful in particular to abort a long poll.
  The thread which is blocking in an operation will throw WakeupException. If no thread is blocking in a method which can throw WakeupException, the next call to such a method will raise it instead."
  [^KafkaConsumer consumer]
  (.wakeup consumer))


(defn topic-partition->map
  "converts a TopicPartition object to a clojure map containing :topic and :partition"
  [^TopicPartition topic-partition]
  {:partition (.partition topic-partition)
   :topic     (.topic topic-partition)})

(defn consumer-record->map
  "transforms a ConsumerRecord to a clojure map containing :key :value :offset :topic :partition :timestamp :timestamp-type and :header "
  [^ConsumerRecord record]
  {:key            (.key record)
   :offset         (.offset record)
   :partition      (.partition record)
   :timestamp      (.timestamp record)
   :timestamp-type (.name (.timestampType record))
   :headers        (.toArray (.headers record))
   :topic          (.topic record)
   :value          (.value record)})

(defn poll->all-records
  "takes the return off a poll (see ConsumerRecords)
  returns a lazy seq of records as clojure maps"
  [^ConsumerRecords records]
  (map consumer-record->map (iterator-seq (.iterator records))))


(defn poll->records-by-topic
  "takes the return of a poll (see ConsumerRecords)
  returns a map {topic records-seq}"
  [^ConsumerRecords records]
  (let [topics (map (comp :topic topic-partition->map) (.partitions records))]
    (->> topics
         (map (fn[topic] [topic (map consumer-record->map (.records records topic))]))
         (into {}))))

(defn ^:no-doc poll->records-by-partition
  [^ConsumerRecords records])

(defn poll-and-process
  "Poll records and run process-fn on each of them (presumably for side effects)"
  [^KafkaConsumer consumer timeout process-fn]
  (let [records (-> (poll consumer timeout)
                    (poll->all-records))]
    (doseq [record records]
      (process-fn record))))

(defn poll-loop
  "Start a consumer loop, calling a callback for each record, and returning a funcion
to stop the loop.

### Parameters
             consumer: consumer context
    process-record-fn: function to call with each record polled
              options: {:poll-timeout 2000 ; duration of a polling without events (ms)
                        :auto-close?  false; close the consumer on exit
                        :on-error-fn  (fn [ex] ...); called on exception

### Returns
              stop-fn: callback function to stop the loop"
  [consumer
   process-record-fn
   & [{:keys [poll-timeout auto-close? on-error-fn]
       :or {poll-timeout 2000
            auto-close? false}}]]
  (let [continue?  (atom true)
        completion (future
                     (try
                       (while @continue?
                         (try
                           (poll-and-process consumer poll-timeout process-record-fn)
                           (catch WakeupException _)
                           (catch Throwable t
                             (if on-error-fn (on-error-fn t))
                                             (throw t))))
                       :ok
                       (catch Throwable t t)
                       (finally
                         (when auto-close?
                           (.close consumer)))))]
    (fn []
      (reset! continue? false)
      (deref completion))))



(defn consumer
  "create a consumer

  conf is a map {:keyword value} (for all  possibilities see https://kafka.apache.org/documentation/#consumerconfigs)
  
  key and value deserializer can be one of :long :string :t+json :t+mpack
  with the 1 argument arity, :key.deserializer and :value.deserializer must be provided in conf

  you can optionaly provide a list of topics to subscribe to"
  ([conf]
    (KafkaConsumer. (walk/stringify-keys conf)))
  ([conf key-deserializer value-deserializer]
    (KafkaConsumer. (walk/stringify-keys conf)
                    (deserializer key-deserializer)
                    (deserializer value-deserializer)))
  ([conf key-deserializer value-deserializer topics]
    (apply subscribe (consumer conf key-deserializer value-deserializer) topics)))


(defn close!
  "Tries to close the consumer cleanly within the specified timeout in ms (defaults to 30 secs).
  This method waits up to timeout for the consumer to complete pending commits and leave the group.
  If auto-commit is enabled, this will commit the current offsets if possible within the timeout.
  If the consumer is unable to complete offset commits and gracefully leave the group before the timeout expires, the consumer is force closed."
  ([^KafkaConsumer consumer]         (.close consumer))
  ([^KafkaConsumer consumer timeout] (.close consumer (Duration/ofMillis timeout))))
