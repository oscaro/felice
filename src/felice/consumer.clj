;; A thin layer on top of java [KafkaConsumer](https://kafka.apache.org/21/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html)

(ns felice.consumer
  (:require [clojure.walk :as walk]
            [felice.serialization :refer [deserializer]])
  (:import [org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecords ConsumerRecord]
           [org.apache.kafka.clients.consumer OffsetAndMetadata]
           [org.apache.kafka.common TopicPartition Metric]
           org.apache.kafka.common.errors.WakeupException
           java.time.Duration))


(def CONF-COERCERS {:auto.commit.interval.ms   int
                    :connections.max.idle.ms   int
                    :default.api.timeout.ms    int
                    :fetch.max.bytes           int
                    :fetch.max.wait.ms         int
                    :fetch.min.bytes           int
                    :heartbeat.interval.ms     int
                    :max.partition.fetch.bytes int
                    :max.poll.interval.ms      int
                    :max.poll.records          int
                    :metrics.num.samples       int
                    :receive.buffer.bytes      int
                    :request.timeout.ms        int
                    :send.buffer.bytes         int
                    :session.timeout.ms        int
                    :sasl.login.refresh.buffer.seconds     short
                    :sasl.login.refresh.min.period.seconds short})


(defn- coerce-consumer-config
  [cfg]
  (->> cfg
       (map (fn [[k v]]
              (let [coerce-fn (get CONF-COERCERS (keyword k))
                    v* (if (and v coerce-fn) (coerce-fn v) v)]
                [k v*])))
       (into {})))


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

(defn ^:no-doc position [^KafkaConsumer consumer topic partition]
  (.position consumer (TopicPartition. topic partition)))

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
  [^KafkaConsumer consumer timeout process-fn commit-policy]
  (let [records (-> (poll consumer timeout)
                    (poll->all-records))]
    (doseq [record records]
      (process-fn record)
      (when (= :record commit-policy)
        (commit-message-offset consumer record)))
    (when (= :poll commit-policy)
      (commit-sync consumer))))


(defn consumer
  "create a consumer

  conf is a map {:keyword value} (for all  possibilities see https://kafka.apache.org/documentation/#consumerconfigs)

  key and value deserializer can be one of :long :string :t+json :t+mpack
  with the 1 argument arity, :key.deserializer and :value.deserializer must be provided in conf

  you can optionaly provide a list of topics to subscribe to"
  ([conf]
    (let [kd (deserializer (:key.deserializer conf))
          vd (deserializer (:value.deserializer conf))
          conf* (-> conf
                    (dissoc :key.deserializer :value.deserializer :topics)
                    coerce-consumer-config
                    walk/stringify-keys)
          kc (KafkaConsumer. conf* kd vd)]
      (when-let [topics (:topics conf)]
        (apply subscribe kc topics))
      kc))
  ([conf topics]
   (consumer (assoc conf :topics topics)))
  ([conf key-deserializer value-deserializer]
    (consumer (assoc conf :key.deserializer key-deserializer
                          :value.deserializer value-deserializer)))
  ([conf key-deserializer value-deserializer topics]
    (consumer (assoc conf :topics topics) key-deserializer value-deserializer)))



(defn close!
  "Tries to close the consumer cleanly within the specified timeout in ms (defaults to 30 secs).
  This method waits up to timeout for the consumer to complete pending commits and leave the group.
  If auto-commit is enabled, this will commit the current offsets if possible within the timeout.
  If the consumer is unable to complete offset commits and gracefully leave the group before the timeout expires, the consumer is force closed."
  ([^KafkaConsumer consumer]         (.close consumer))
  ([^KafkaConsumer consumer timeout] (.close consumer (Duration/ofMillis timeout))))


(defn poll-loop*
  [consumer
   process-record-fn
   {:keys [poll-timeout on-error-fn commit-policy close-timeout-ms]
    :or {poll-timeout 2000 close-timeout-ms 5000}}]
  (let [continue?  (atom true)
        completion (future
                     (try
                       (while @continue?
                         (try
                           (poll-and-process consumer poll-timeout process-record-fn commit-policy)
                           (catch WakeupException _)
                           (catch Throwable t
                             (if on-error-fn (on-error-fn t))
                             (throw t))))
                       :stopped
                       (catch Throwable t t)
                       (finally
                         (close! consumer (or close-timeout-ms Long/MAX_VALUE)))))]
    (fn
      ([]
       (reset! continue? false)
       (deref completion))
      ([timeout]
       (deref completion timeout :polling)))))

(defn poll-loop
  "Start a consumer loop, calling a callback for each record, and returning a function
  to stop the loop.

### Parameters
             consumer: consumer config (see consumer)
    process-record-fn: function to call with each record polled
              options: {:poll-timeout 2000 ; duration of a polling without events (ms)
                        :on-error-fn  (fn [ex] ...); called on exception
                        :commit-policy :never ; #{:never :poll :record}}
#### commit policy
* :never  : does nothing (use it if you enabled client auto commit)
* :poll   : commit last read offset after processing all the items of a poll
* :record : commit the offset of every processed record

  if you want to commit messages yourself, set commit policy to `:never` and use `commit-message-offset` or `commit-sync`

### Returns
              stop-fn: callback function to stop the loop"
  ([consumer-conf process-record-fn] (poll-loop consumer-conf process-record-fn {}))
  ([consumer-conf process-record-fn opts]
   (let [consumer (consumer consumer-conf)]
     (poll-loop* consumer process-record-fn opts))))

(defn poll-loops* [consumer-conf process-record-fn topics opts threads]
  (for [n (range threads)
        :let [consumer (consumer consumer-conf topics)]]
    (poll-loop* consumer process-record-fn opts)))

(defn poll-loops
  "Start consumer loops, calling a callback for each record, and returning a function
  to stop the loops.

### Parameters
             consumer: consumer config (see consumer)
    process-record-fn: function to call with each record polled
               topics: topics you want to subscribe to
              options: {:poll-timeout 2000 ; duration of a polling without events (ms)
                        :on-error-fn  (fn [ex] ...); called on exception
                        :commit-policy :never ; #{:never :poll :record}
                        :threads-by-topic 1 ; number of spawned consumers for each topic
                        :threads 1 ; number of spawned consumers}
#### commit policy
* :never  : does nothing (use it if you enabled client auto commit)
* :poll   : commit last read offset after processing all the items of a poll
* :record : commit the offset of every processed record

  if you want to commit messages yourself, set commit policy to `:never` and use `commit-message-offset` or `commit-sync`

#### Multi-threading
  You can set either :threads-by-topic or :threads option (if both are set, :threads-by-topic will win)
  * :threads          : spawn N threads total (each thread listening all registered topic)
  * :threads-by-topic : spawn N threads for each registered topic
  * you can also provide a map {:topic :threads} instead of a list of topics

### Returns
              stop-fn: callback function to stop the loop"
  ([comsumer-conf process-record-fn] (poll-loops consumer-conf process-record-fn {}))
  ([consumer-conf process-record-fn {:as opts}]
   (if-let [topics (:topics consumer-conf)]
     (poll-loops consumer-conf process-record-fn topics opts)
     (throw (ex-info "topics configuration is missing"
                     {:consumer-configuration consumer-conf
                      :info "you must specify a (list of )topic(s) either in the consumer config or using the 4 params arity of 'poll-loops'"}))))
  ([consumer-conf process-record-fn topics {:keys [threads threads-by-topic] :as opts}]
   (let [loops
         (doall
          (if (and threads (nil? threads-by-topic))
            (poll-loops* consumer-conf process-record-fn topics opts threads)
            (flatten (for [topic topics]
                       (let [[topic threads] (if (map-entry? topic)
                                               topic
                                               [topic (or threads-by-topic 1)])]
                         (poll-loops* consumer-conf process-record-fn [topic] opts threads))))))]
     (fn
       ([]        (doall (for [loop loops] (loop))))
       ([timeout] (doall (for [loop loops] (loop timeout))))))))
