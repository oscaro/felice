(ns felice.producer
  (:require [clojure.walk :as walk]
            [felice.serialization :refer [serializer]])
  (:import java.util.concurrent.TimeUnit
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord Callback]))

(defn flush!
  "Invoking this method makes all buffered records immediately available
  to send (even if linger.ms is greater than 0) and blocks on the
  completion of the requests associated with these records."
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
  "Transforms a clojure map to a ProducerRecord object"
  [{:keys [topic key value partition timestamp headers]}]
  (ProducerRecord. topic partition timestamp key value headers))

(defn ^:no-doc send-record!
  "Sends a ProducerRecord with an optional callback when the
   send has been acknowledged."
  ([^KafkaProducer producer ^ProducerRecord record]
   (.send producer (producer-record record)))
  ([^KafkaProducer producer ^ProducerRecord record callback]
   (.send producer
          (producer-record record)
          (reify Callback
            (onCompletion [this metadata exception]
              (callback (or exception metadata)))))))

(defn ->record
  "creates a record map given a topic, a value and an optional key"
  ([topic value]     {:topic topic          :value value})
  ([topic key value] {:topic topic :key key :value value}))

(defn send!
  "asynchronously send a record"
  ([^KafkaProducer producer topic value]     (send-record! producer (->record topic value)))
  ([^KafkaProducer producer topic key value] (send-record! producer (->record topic key value)))
  ([^KafkaProducer producer record-map]      (send-record! producer record-map)))

(defn send-with-callback!
  "Asynchronously send a record triggering the given callback when the send has
  been acknowledged.
  Note that callbacks will generally execute in the I/O thread of the producer
  and so should be reasonably fast or they will delay the sending of messages
  from other threads.

  If you want to execute blocking or computationally expensive callbacks it
  is recommended to use your own Executor in the callback body to parallelize
  processing."
  ([^KafkaProducer producer topic value cb]     (send-record! producer (->record topic value) cb))
  ([^KafkaProducer producer topic key value cb] (send-record! producer (->record topic key value) cb))
  ([^KafkaProducer producer record-map cb]      (send-record! producer record-map cb)))

(defn send!!
  "Synchronously send a record - wait until acknowledged"
  ([^KafkaProducer producer topic value]     (send!! producer (->record topic value)))
  ([^KafkaProducer producer topic key value] (send!! producer (->record topic key value)))
  ([^KafkaProducer producer record-map] (.get (send! producer record-map))))

(defn producer
  "Create a producer

   `conf` is a map {:keyword value}
   See https://kafka.apache.org/documentation/#producerconfigs for all possibilities

   key and value serializer can be one of keys defined in `felice.serializer` namespace
   with the 1 argument arity, :key.serializer and :value.serializer must be provided in conf"
  ([conf]
   (KafkaProducer. (walk/stringify-keys (dissoc conf :key.serializer :value.serializer))
                   (serializer (:key.serializer conf))
                   (serializer (:value.serializer conf))))
  ([conf key-serializer value-serializer]
   (producer (assoc conf :key.serializer key-serializer
                    :value.serializer value-serializer))))

(defn close!
  "This method waits up to timeout ms for the producer to complete the
  sending of all incomplete requests.

  If the producer is unable to complete all requests before the timeout
  expires, this method will fail any unsent and unacknowledged records immediately.

  Calling close with no timeout is equivalent to close(Long.MAX_VALUE, TimeUnit.MILLISECONDS)"
  ([^KafkaProducer producer]         (.close producer))
  ([^KafkaProducer producer timeout] (.close producer timeout TimeUnit/MILLISECONDS)))
