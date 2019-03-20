# FELICE

Felice is client library for [Apache Kafka](http://kafka.apache.org) in Clojure.

## Producing records
```clojure
(require '[felice.producer :as fp])

(let [producer (fp/producer {:bootstrap.servers "localhost:9092"} :string :string)]
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

(let [consumer (fc/consumer {:bootstrap.servers "localhost:9092"
                             :group.id "my-group"
                             :auto.offset.reset "earliest"
                             :enable.auto.commit false}
                            :string :string)]
                            
;; subscribe can take multiple topics at once
  (fc/subscribe consumer "topic1" "topic2")
;; and keep your previous subscriptions 
  (fc/subscribe consumer "topic3")

;; basic polling
  (let [cr (fc/poll consumer 100)
        records (fc/consumer-records->all-records cr)]
    (doseq [record records] (print-record record)))
    
;; poll-loop 
  (let [loop (fc/poll-loop consumer 100 print-record true)]
    (reset! (:continue? loop) false)
    (deref (:completion loop)))
    
  (fc/close! consumer))
```