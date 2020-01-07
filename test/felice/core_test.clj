(ns felice.core-test
  (:require [clojure.test :refer :all]
            [clojure.java.shell :refer [sh]]
            [clojure.string :refer [trim-newline split]]
            [clojure.core.async :as async]

            [felice.consumer :as consumer]
            [felice.producer :as producer]
            [felice.async    :as fa]))

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
   "dev-resources/kafka-topics.sh" "--create" "--zookeeper" "0.0.0.0:2181" "--replication-factor" "3" "--partitions" "3" "--topic" (str topic)))

(defn docker-kafka [f]
  (let [container (start-docker-kafka)]
    (f)
    (close-docker-kafka container)))

;(use-fixtures :once docker-kafka)

(deftest client
  (testing "produce and consume strings"
    (let [producer (producer/producer {:bootstrap.servers "localhost:9092"} :string :string)
          consumer (consumer/consumer {:bootstrap.servers "localhost:9092" 
                                       :group.id "test-1"
                                       :max.poll.records 100
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
            records (consumer/poll->all-records consumer-records)]
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
            records (consumer/poll->all-records consumer-records)
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
            records (consumer/poll->all-records consumer-records)
            record (first records)]
        (is (not (.isEmpty consumer-records)) "we have polled something")
        (is (= 1 (count records)))
        (is record)
        (is (= message (:value record)))
        )
      (producer/close! producer)
      (consumer/close! consumer)))

  (testing "poll-loop"
    (let [topic "topic4"
          key-fmt :string
          val-fmt :json
          producer (producer/producer {:bootstrap.servers "localhost:9092"} key-fmt val-fmt)
          consumer-cfg {:bootstrap.servers "localhost:9092"
                        :group.id "test-4"
                        :auto.offset.reset "earliest"
                        :enable.auto.commit false
                        :key.deserializer key-fmt
                        :value.deserializer val-fmt
                        :topics #{topic}}]
      (create-topic topic)
      (let [counter (atom 0)
            process-fn (fn [{:keys [topic partition offset timestamp key value]}]
                          (swap! counter + (:zob value)))
            stop-fn (consumer/poll-loop consumer-cfg process-fn {:auto-close? true})]
        (producer/send! producer topic {:zob 42})
        (Thread/sleep 1000)
        (is (= 42 @counter))
        (producer/send! producer topic {:zob 66})
        (Thread/sleep 1000)
        (is (= (+ 42 66) @counter))
        (stop-fn))

      (testing "commit by poll"
        (let [topic "topic-41"
              last-record (atom nil)
              consumer-cfg (assoc consumer-cfg :topics #{topic})
              process-fn (fn [r] (reset! last-record r))]
          (create-topic topic)
          (let [producing (future (dotimes [i 10] (Thread/sleep 1000) (producer/send! producer topic {:zob 0})))
                stop-fn (consumer/poll-loop consumer-cfg process-fn {:auto-close? true :commit-policy :poll})]
            (Thread/sleep 3000)
            (stop-fn)
            (deref producing)
            (let [consumer (consumer/consumer consumer-cfg)
                  {offset-commited :offset} @last-record
                  {offset-read :offset} (first (consumer/poll->all-records (consumer/poll consumer 1000)))]
              (is (= offset-commited (dec offset-read)))
              (consumer/close! consumer)))))
      (producer/close! producer))))

(deftest async
  (testing "poll-chan"
    (let [producer (producer/producer {:bootstrap.servers "localhost:9092"} :long :long)
          topic "topic5"]
      (println "CREATE TOPIC...")
      (create-topic topic)
      (println "TOPIC CREATED")
      (let [records (async/chan 100)
            consumer (fa/consumer {:bootstrap.servers "localhost:9092" :group.id "test-5"
                                   :auto.offset.reset "earliest"
                                   :enable.auto.commit false}
                                  :long :long
                                  records topic)]
        (println "PRODUCING...")
        (doseq [i (range 10)]
          (producer/send! producer topic i i))
        (println "PRODUCED")
        (is (= 0 (:key (async/<!! records))))
        (fa/commit-message-offset consumer (async/<!! records))
        (fa/close! consumer))

      (let [records (async/chan 100)
            consumer (fa/consumer {:bootstrap.servers "localhost:9092" :group.id "test-5"
                                         :auto.offset.reset "earliest"
                                         :enable.auto.commit false}
                                        :long :long
                                        records topic)]
        (is (= 2 (:key (async/<!! records))))

        (fa/close! consumer))
      (producer/close! producer)))


  (testing "poll-chans"
    (let [producer (producer/producer {:bootstrap.servers "localhost:9092"} :long :long)
          topics (map #(str "pc-" %) (range 5))]
      (doseq [topic topics] (create-topic topic))
      (let [chans (into {} (map (fn [topic] [topic (async/chan 100)]) topics))
            consumer (fa/consumer {:bootstrap.servers "localhost:9092" :group.id "test-5"
                                   :auto.offset.reset "earliest"
                                   :enable.auto.commit false}
                                  :long :long
                                  chans)
            stats (atom {})]
        (dotimes [i 5] (producer/send! producer "pc-0" 0 0))
        (dotimes [i 4] (producer/send! producer "pc-1" 1 1))
        (dotimes [i 3] (producer/send! producer "pc-2" 2 2))
        (dotimes [i 2] (producer/send! producer "pc-3" 3 3))
        (producer/send! producer "pc-4" 4 4)

        (doseq [[topic chan] chans]
          (async/go-loop [item (async/<! chan)]
            (when item
              (swap! stats update topic (fnil inc 0))
              (recur (async/<! chan)))))
        (fa/close! consumer)
        (is (= {"pc-0" 5 "pc-1" 4 "pc-2" 3 "pc-3" 2 "pc-4" 1}
               @stats)))
      (producer/close! producer))))

