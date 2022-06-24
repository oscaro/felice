<p align="center">
	<img src="https://raw.githubusercontent.com/oscaro/felice/master/.github/image.png"/>
</p>

# Felice [![Clojars Project](https://img.shields.io/clojars/v/com.oscaro/felice.svg)](https://clojars.org/com.oscaro/felice) [![Clojure CI](https://github.com/oscaro/felice/actions/workflows/clojure.yml/badge.svg?branch=master)](https://github.com/oscaro/felice/actions/workflows/clojure.yml)


Felice is client library for [Apache Kafka](http://kafka.apache.org) in Clojure. 

Built with simplicity it mind, it support by default JSON, Transit & Nippy (Fast | LZ4) and
provide also custom Serializer / Deserializer mechanism

> Note: the version contains in a first time the [Kafka Client](https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients) then the felice version, separated by hyphen.

## De/Serializers

| TYPE                | KEYWORD      |
|---------------------|--------------|
| String              | `:string`    |
| Json                | `:json`      |
| Json safe           | `:json-safe` |
| Transit MessagePack | `:t+mpack`   |
| Transit Json        | `:t+json`    |
| Nippy Fast          | `:nippy+fast`|
| Nippy LZ4           | `:nippy+lz4` |

Beware that any exception during the deserialization process (eg: malformed json) will be thrown by the poll call. 
This may result in a silent dead poll-loop. 

Using a safe deserializer is an option, it will return a map containing the raw value (as a string) 
and a `:felice.serialization/error` key containing the exception instead of the deserialized value of the record.

## Producing records

```clojure
(require '[felice.producer :as fp])

;see http://kafka.apache.org/documentation/#producerconfigs
(let [producer (fp/producer {:bootstrap.servers "localhost:9092" ;required
                             :key.serializer :string             ;required
                             :value.serializer :t+json           ;required
                             :close.timeout.ms Long/MAX_VALUE}
  ;; publish without key
  (fp/send! producer "topic1" "value")
  ;; publish with a key
  (fp/send! producer "topic2" "key" "value")
  ;; map syntax
  (fp/send! producer {:topic "topic3" :key "key" :value "value"})
  ;; remember to close you producer
  (fp/close! producer))
```

## Consuming records

```clojure
(require '[felice.consumer :as fc])

(defn print-record [{:keys [topic partition offset timestamp key value]}]
   (println (format "Record %s(%d) %d @%d - Key = %s, Value = %s"
                    topic partition offset timestamp key value)))

;see http://kafka.apache.org/documentation/#producerconfigs
(def consumer-cfg {:bootstrap.servers "localhost:9092" ;required
                    :group.id "my-group"               ;required
                    :auto.offset.reset "latest"        ;or "earliest", used at first startup only
                    :key.serializer    :string         ;required
                    :value.serializer  :json           ;required
                    :enable.auto.commit true
                    :auto.commit.interval.ms 5000      ;delay between auto commits
                    :topics #{"topic1" "topic2"}       ;auto subscribes at startup
                    :close.timeout.ms Long/MAX_VALUE})
```

### Do it yourself

```clojure
(let [consumer (fc/consumer consumer-cfg)]

;; subscribe can take multiple topics at once
  (fc/subscribe consumer "topic1" "topic2")
;; and keep your previous subscriptions
  (fc/subscribe consumer "topic3")

;; basic polling
  (let [cr (fc/poll consumer 100)
        records (fc/consumer-records->all-records cr)]
    (doseq [record records] (print-record record))
    (fc/close! consumer))
```

### Using `poll-loop` or `poll-loops`

```clojure
;; poll-loop
  (let [stop-fn (fc/poll-loop consumer-cfg print-record)]
    ;; consumes in a future thread
    ;; call the returned fn when you want to stop polling
    (stop-fn))
;; poll-loops
  (let [stop-fn (fc/poll-loops consumer-cfg print-record {:threads-by-topic 4})]
    ;; spawns 4 consumers by registered topic, each in its own future thread
    ;; call the returned fn when you want to stop polling
    (stop-fn))
```

#### Commit policy

* `:never`   does nothing (use it if you enabled client auto commit)
* `:poll`    commit last read offset after processing all the items of a poll
* `:record`  commit the offset of every processed record

#### Multi-threading

You can set either :threads-by-topic or :threads option (if both are set, :threads-by-topic will win)
* `:threads`           spawn N threads total (each thread listening all registered topic)
* `:threads-by-topic`  spawn N threads for each registered topic
* you can also provide a map {:topic :threads} instead of a list of topics

#### Examples

```clojure
;; Commit after each record processed spawning 8 threads (4 for topic1 and 4 for topic2)
(fc/poll-loops consumer-cfg print-record ["topic1" "topic2"] {:commit-policy :record :threads-by-topic 4})
;; No committing 1 thread for topic1 and 4 for topic2
(fc/poll-loops consumer-cfg print-record {"topic1" 1 "topic2" 4} {:commit-policy :never})
```

#### Partitionning

The partitionner used by a producer can be set using this [configuration key](https://kafka.apache.org/documentation/#producerconfigs_partitioner.class).

See [default](https://github.com/a0x8o/kafka/blob/master/clients/src/main/java/org/apache/kafka/clients/producer/internals/DefaultPartitioner.java#L65-L71) partitionner implementation

There is a comment at the bottom of the felice.producer namespace mimiking the default partitionner if you want to see the result for some keys.

```clojure
;; default partitionner
(import 'org.apache.kafka.common.utils.Utils)
(defn partition-from-bytes [partition-count bytes]
  (mod (Utils/toPositive (Utils/murmur2 bytes)) partition-count))
(defn partition-from-string [partition-count string]
  (partition-from-bytes partition-count (.getBytes string)))
```

## License

Copyright © 2018 - 2022 Oscaro

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
