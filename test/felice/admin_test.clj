(ns felice.admin-test
  (:require [felice.admin :as admin]
            [felice.consumer :as consumer]
            [felice.producer :as producer]
            [clojure.test :refer :all]))

(deftest admin-test
  (testing "admin methods"
    (let [admin-client (admin/admin-client {:bootstrap.servers "localhost:9092"})
          topic "foobar-baz"
          topic-bis "quux"]

      ;; Assert for deletion on empty topic
      (doall
       (for [x (admin/list-topics admin-client)]
         (is (= {:topic x :status :kafka.topic/deleted}
                (admin/delete-topic admin-client x)))))
      (is (= #{} (admin/list-topics admin-client)))

      (is (= {:topic topic :status :kafka.topic/created} (admin/create-topic admin-client topic 1 1)))

      (is (thrown? Exception
                   (admin/create-topics admin-client {:nttttttame topic-bis
                                                      :partition-count 1})))
      (is (= [{:topic topic-bis :status :kafka.topic/created}]
             (admin/create-topics admin-client [{:name topic-bis
                                                 :partition-count 1
                                                 :replication-factor 1
                                                 :props {}}])))
      (is (= #{topic topic-bis} (admin/list-topics admin-client)))
      (doall
       (for [x (admin/list-topics admin-client)]
         (is (= {:topic x :status :kafka.topic/deleted}
                (admin/delete-topic admin-client x)))))
      ;; Describe Cluster
      (is (= #{:cluster-id :authorized-operation :controller-node :nodes} (set (keys (admin/describe-cluster admin-client)))))
      (admin/admin-close admin-client))))

(deftest consumer-state-tests
  (testing "consumer state tests"
    (let [group-id "test-1"
          admin-client (admin/admin-client {:bootstrap.servers "localhost:9092"})
          producer (producer/producer {:bootstrap.servers "localhost:9092"} :string :string)
          consumer (consumer/consumer {:bootstrap.servers "localhost:9092"
                                       :group.id group-id
                                       :max.poll.records 100
                                       :auto.offset.reset "earliest"} :string :string)
          topic "quux-baz"]

      ;; Assert for deletion on empty topic
      (doall
       (for [x (admin/list-topics admin-client)]
         (is (= {:topic x :status :kafka.topic/deleted}
                (admin/delete-topic admin-client x)))))

      (is (= #{} (admin/list-topics admin-client)))

      (is (= {:topic topic :status :kafka.topic/created} (admin/create-topic admin-client topic 1 1)))
      ;; => 3msg sent
      (consumer/subscribe consumer topic)

      (producer/send! producer topic "value")
      (producer/send! producer topic "key" "value")
      (producer/send! producer {:topic topic :key "key" :value "value"})
      (producer/flush! producer)

      (let [consumer-records (consumer/poll consumer 100000)
            records (consumer/poll->all-records consumer-records)]
        (is (not (.isEmpty consumer-records)) "we have polled something")
        (is (= 3 (count records)))
        (is (= "value" (:value (first records)))))

      (consumer/close! consumer)

      ;;Reset all the offset to beginning
      (is (= [0]
             (map #(get-in % [:metadata :offset]) (:offsets (admin/set-consumer-group-topic-offset admin-client group-id topic 0)))))

      (def consumer-2 (consumer/consumer {:bootstrap.servers "localhost:9092"
                                          :group.id group-id
                                          :max.poll.records 100
                                          :auto.offset.reset "earliest"} :string :string))

      (consumer/subscribe consumer-2 topic)

;; Let's replay test then
      (let [consumer-records (consumer/poll consumer-2 100000)
            records (consumer/poll->all-records consumer-records)]
        (is (not (.isEmpty consumer-records)) "we have polled something")
        (is (= 3 (count records)))
        (is (= "value" (:value (first records)))))

      (consumer/close! consumer-2)

      (is (= 3
             (:offset (:metadata (first (get-in (admin/list-consumer-groups-offsets admin-client group-id)
                                                [:topics topic]))))))
      (is (= {"test-1" [{:topic "quux-baz", :sum 3}]}
             (admin/sum-consumer-groups-offsets admin-client)))

      ;; Edges

      (is (nil? (admin/sum-consumer-groups-offsets admin-client "broken-group-id")))

      (admin/delete-topic admin-client topic)
      (producer/close! producer)
      (admin/admin-close admin-client))))
