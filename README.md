# FELICE

Felice is client library for [Apache Kafka](http://kafka.apache.org) in Clojure.

## De/Serializers

|Available key/value de/serializers|
+---------------------+------------+
| String              | `:string`  |
| Json                | `:json`    |
| Transit MessagePack | `:t+mpack` |
| Transit Json        | `:t+json`  |

## Producing records
```clojure
(require '[felice.producer :as fp])

;see http://kafka.apache.org/documentation/#producerconfigs
(let [producer (fp/producer {:bootstrap.servers "localhost:9092" ;required
                             :key.serializer :string             ;required
                             :value.serializer :t+json           ;required
                             :close.timeout.ms Long/MAX_VALUE}
  (fp/send! producer "topic1" "value")
  (fp/send! producer "topic2" "key" "value")
  (fp/send! producer {:topic "topic3" :key "key" :value "value"})
  (fp/close! producer))
```

## Consuming records

```clojure
(require '[felice.consumer :as fc])

(defn print-record [{:keys [topic partition offset timestamp key value]}]
   (println (format "Record %s(%d) %d @%d - Key = %s, Value = %s"
                    topic partition offset timestamp key value)))

;see http://kafka.apache.org/documentation/#producerconfigs
(let [consumer-cfg {:bootstrap.servers "localhost:9092" ;required
                    :group.id "my-group"                ;required
                    :auto.offset.reset "latest"         ;or "earliest", used at first startup only
                    :key.serializer    :string          ;required
                    :value.serializer  :json            ;required
                    :enable.auto.commit true
                    :auto.commit.interval.ms 5000       ;delay between auto commits
                    :topics #{"topic1" "topic2"}        ;auto subscribes at startup
                    :close.timeout.ms Long/MAX_VALUE}
      consumer (fc/consumer consumer-cfg)]

;; subscribe can take multiple topics at once
  (fc/subscribe consumer "topic1" "topic2")
;; and keep your previous subscriptions
  (fc/subscribe consumer "topic3")

;; basic polling
  (let [cr (fc/poll consumer 100)
        records (fc/consumer-records->all-records cr)]
    (doseq [record records] (print-record record))
    (fc/close! consumer))

;; poll-loop
  (let [stop-fn (fc/poll-loop consumer-cfg print-record)]
    (Thread/sleep 10000)
    (stop-fn))

```