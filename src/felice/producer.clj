(ns felice.producer
  (:require [clojure.walk :as walk]
            [felice.serialization :refer [serializer]])
  (:import java.util.concurrent.TimeUnit
           org.apache.kafka.clients.producer.KafkaProducer
           org.apache.kafka.clients.producer.ProducerRecord))

(defn flush!  [^KafkaProducer producer] (.flush producer))
(defn metrics [^KafkaProducer producer] (.metrics producer))

(defn partitions-for [^KafkaProducer producer topic] (.partitionsFor producer topic))

(defn producer-record [{:keys [topic key value partition timestamp headers]}]
  (ProducerRecord. topic partition timestamp key value headers))

(defn send-record!
  ([^KafkaProducer producer ^ProducerRecord record]          (.send producer (producer-record record)))
  ([^KafkaProducer producer ^ProducerRecord record callback] (.send producer (producer-record record) callback)))

(defn ->record
  ([topic value]     {:topic topic          :value value})
  ([topic key value] {:topic topic :key key :value value}))

(defn send!
  ([^KafkaProducer producer topic value]     (send-record! producer (->record topic value)))
  ([^KafkaProducer producer topic key value] (send-record! producer (->record topic key value)))
  ([^KafkaProducer producer record-map]      (send-record! producer record-map)))

(defn producer
  "create a producer"
  ([conf]                                 (KafkaProducer. (walk/stringify-keys conf)))
  ([conf key-serializer value-serializer] (KafkaProducer. (walk/stringify-keys conf)
                                                          (serializer key-serializer)
                                                          (serializer value-serializer))))

(defn close!
  ([^KafkaProducer producer]         (.close producer))
  ([^KafkaProducer producer timeout] (.close producer timeout TimeUnit/MILLISECONDS)))
