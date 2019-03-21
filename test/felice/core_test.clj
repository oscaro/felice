(ns felice.core-test
  (:require [clojure.test :refer :all]
            [clojure.java.shell :refer [sh]]
            [clojure.string :refer [trim-newline split]]

            [felice.consumer :as consumer]
            [felice.producer :as producer]))

(defn start-docker-kafka []
  (println "starting kafka...")
  (let [{:keys [exit out err] :as r} (sh "docker" "run" "-d"
                                         "-p" "2181:2181" "-p" "9092:9092"
                                         "--env" "ADVERTISED_HOST=172.17.0.1"
                                         "--env" "ADVERTISED_PORT=9092"
                                         "spotify/kafka")
        cid (if (not= 0 exit)
              (throw (ex-info "failed to start docker container" r))
              (trim-newline out))]
    (println "kafka started")
    cid
    ))

(defn close-docker-kafka [container]
  (println "testing done, killing kafka...")
  (sh "docker" "kill" container)
  (sh "docker" "rm"   container)
  (println "kafka killed, bye"))

(defn create-topic [topic]
  (sh
   "dev-resources/kafka-topics.sh" "--create" "--zookeeper" "localhost:2181" "--replication-factor" "3" "--partitions" "3" "--topic" (str topic)))

(defn docker-kafka [f]
  (let [container (start-docker-kafka)]
    (f)
    (close-docker-kafka container)))

(use-fixtures :once docker-kafka)

(deftest client
  (testing "produce and consume strings"
    (let [producer (producer/producer {:bootstrap.servers "localhost:9092"} :string :string)
          consumer (consumer/consumer {:bootstrap.servers "localhost:9092" :group.id "test-1"
                                       :auto.offset.reset "earliest"} :string :string)
          topic "topic"]
      (create-topic topic)
      (consumer/subscribe consumer topic)

      (producer/send! producer topic "value")
      (producer/send! producer topic "key" "value")
      (producer/send! producer {:topic topic :key "key" :value "value"})
      (producer/flush! producer)
      (println (consumer/subscription consumer))
      (let [consumer-records (consumer/poll consumer 100000)
            records (consumer/consumer-records->all-records consumer-records)]
        (is (not (.isEmpty consumer-records)) "we have polled something")
        (is (= 3 (count records)))
        (is (= "value" (:value (first records))))
        )
      (producer/close! producer)
      (consumer/close! consumer)
      ))

  (testing "produce and consume json"
    (let [producer (producer/producer {:bootstrap.servers "localhost:9092"} :string :t+json)
          consumer (consumer/consumer {:bootstrap.servers "localhost:9092" :group.id "test-2"
                                       :auto.offset.reset "earliest"} :string :t+json)
          topic "topic2"
          message {:string "string" :long (long 42) :double (double 4.2) :bool false :nil nil
                   :date (java.util.Date.)}]

      (create-topic topic)
      (consumer/subscribe consumer topic)

      (producer/send! producer topic message)
      (producer/flush! producer)
      (let [consumer-records (consumer/poll consumer 60000)
            records (consumer/consumer-records->all-records consumer-records)
            record (first records)]
        (is (not (.isEmpty consumer-records)) "we have polled something")
        (is (= 1 (count records)))
        (is record)
        (is (= message (:value record)))
        )
      (producer/close! producer)
      (consumer/close! consumer)))

  (testing "produce and consume msgpack"
    (let [producer (producer/producer {:bootstrap.servers "localhost:9092"} :string :t+mpack)
          consumer (consumer/consumer {:bootstrap.servers "localhost:9092" :group.id "test-3"
                                       :auto.offset.reset "earliest"} :string :t+mpack)
          topic "topic3"
          message {:string "string" :long (long 42) :double (double 4.2)
                   :bool false :nil nil :date (java.util.Date.)}]

      (create-topic topic)
      (consumer/subscribe consumer topic)

      (producer/send! producer topic message)
      (producer/flush! producer)
      (let [consumer-records (consumer/poll consumer 60000)
            records (consumer/consumer-records->all-records consumer-records)
            record (first records)]
        (is (not (.isEmpty consumer-records)) "we have polled something")
        (is (= 1 (count records)))
        (is record)
        (is (= message (:value record)))
        )
      (producer/close! producer)
      (consumer/close! consumer)))

  (testing "poll-loop"
    (let [producer (producer/producer {:bootstrap.servers "localhost:9092"} :string :string)
          consumer (consumer/consumer {:bootstrap.servers "localhost:9092" :group.id "test-4"
                                       :auto.offset.reset "earliest"} :string :string)
          topic "topic4"]
      (create-topic topic)
      (consumer/subscribe consumer topic)
      (let [counter (atom 0)
            stop-fn (consumer/poll-loop consumer (fn [_] (swap! counter inc)) {:auto-close? true})]
        (producer/send! producer topic "first")
        (Thread/sleep 1000)
        (is (= 1 @counter))
        (producer/send! producer topic "second")
        (Thread/sleep 1000)
        (is (= 2 @counter))
        (stop-fn))
      (producer/close! producer)))
  )

;(deftest serialization (testing "no serialization"))


