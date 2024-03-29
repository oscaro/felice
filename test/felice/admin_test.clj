(ns felice.admin-test
  (:require [felice.admin :as admin]
            [clojure.test :refer :all]))

(deftest admin
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
