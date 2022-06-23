(ns felice.serdes-test
  (:require  [clojure.test :refer :all]
             [felice.consumer :as consumer]
             [felice.producer :as producer]
             [felice.core-test :refer [create-topic]]
             [clojure.string :as str]))

(deftest nippy-serdes-test
  (doseq [format #{:nippy+fast :nippy+lz4}]
    (testing (format "produce and consume %s payload" (name format))
      (let [producer (producer/producer {:bootstrap.servers "localhost:9092"} format format)
            consumer (consumer/consumer {:bootstrap.servers "localhost:9092"
                                         :group.id "test-1"
                                         :max.poll.records 100
                                         :auto.offset.reset "earliest"} format format)
            topic "nippy-test"]
        (create-topic topic)
        (consumer/subscribe consumer topic)

        (producer/send! producer topic {:foo format})
        (producer/send! producer topic :my-key {:baz :quux})
        (producer/send! producer {:topic topic
                                  :key :alpha
                                  :value {:bravo :charlie}})

        (producer/flush! producer)

        (let [consumer-records (consumer/poll consumer 100000)
              records (consumer/poll->all-records consumer-records)]
          (is (not (.isEmpty consumer-records)) "we have polled something")
          (is (= 3 (count records)))
          (is (= {:foo format} (:value (first records)))))

        (producer/close! producer)
        (consumer/close! consumer)))))
