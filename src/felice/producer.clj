(ns felice.producer
  (:require [clojure.walk :as walk]
            [felice.serialization :refer [serializer]])
  (:import java.util.concurrent.TimeUnit
           org.apache.kafka.clients.producer.KafkaProducer
           org.apache.kafka.clients.producer.ProducerRecord))

(defn flush!
  "Invoking this method makes all buffered records immediately available to send (even if linger.ms is greater than 0) and blocks on the completion of the requests associated with these records."
  [^KafkaProducer producer]
  (.flush producer))

(defn metrics
  "Get the full set of internal metrics maintained by the producer."
  [^KafkaProducer producer]
  (.metrics producer))

(defn partitions-for
  "Get the partition metadata for the given topic."
  [^KafkaProducer producer topic]
  (.partitionsFor producer topic))

(defn ^:no-doc producer-record
  "transforms a clojure map to a ProducerRecord object"
  [{:keys [topic key value partition timestamp headers]}]
  (ProducerRecord. topic partition timestamp key value headers))

(defn ^:no-doc send-record!
  "sends a ProducerRecord with an optional callback when the send has been acknowledged."
  ([^KafkaProducer producer ^ProducerRecord record]          (.send producer (producer-record record)))
  ([^KafkaProducer producer ^ProducerRecord record callback] (.send producer (producer-record record) callback)))

(defn ->record
  "creates a record map given a topic, a value and an optional key"
  ([topic value]     {:topic topic          :value value})
  ([topic key value] {:topic topic :key key :value value}))

(defn send!
  "send a record"
  ([^KafkaProducer producer topic value]     (send-record! producer (->record topic value)))
  ([^KafkaProducer producer topic key value] (send-record! producer (->record topic key value)))
  ([^KafkaProducer producer record-map]      (send-record! producer record-map)))

(defn producer
  "create a producer

  conf is a map {:keyword value} (for all  possibilities see https://kafka.apache.org/documentation/#producerconfigs)

key and value serializer can be one of :long :string :t+json :t+mpack
with the 1 argument arity, :key.deserializer and :value.deserializer must be provided in conf"
  ([conf]                                 (KafkaProducer. (walk/stringify-keys conf)))
  ([conf key-serializer value-serializer] (KafkaProducer. (walk/stringify-keys conf)
                                                          (serializer key-serializer)
                                                          (serializer value-serializer))))

(defn close!
  "This method waits up to timeout ms for the producer to complete the sending of all incomplete requests.
  If the producer is unable to complete all requests before the timeout expires, this method will fail any unsent and unacknowledged records immediately.
  Calling close with no timeout is equivalent to close(Long.MAX_VALUE, TimeUnit.MILLISECONDS)"
  ([^KafkaProducer producer]         (.close producer))
  ([^KafkaProducer producer timeout] (.close producer timeout TimeUnit/MILLISECONDS)))
