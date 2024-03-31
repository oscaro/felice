(ns user
  (:require [felice.consumer :as fc]
            [felice.producer :as fp]
            [felice.admin    :as fa]
            [clojure.reflect :as r]
            [clojure.pprint  :refer [print-table]])
  (:import ))


(def broker "localhost:9092")

;;; Local Client

(defn dump-object-methods [o]
  (print-table (:members (r/reflect o))))

(comment

  (def admin-client (fa/admin-client {:bootstrap.servers broker}))
  (fa/create-topic admin-client "bar" 1 1 )

  (def producer (fp/producer {:bootstrap.servers broker
                              :key.serializer :string
                              :value.serializer :json}))
  (def consumer (fc/consumer {:bootstrap.servers broker
                              :key.deserializer :string
                              :value.deserializer :json
                              :group.id "elfs-99"}))

  (fc/subscribe consumer "foo")

  (let [cr (fc/poll consumer 100)]
    (println "TOT" (.count cr))
    (fc/commit-sync consumer))

  (fp/send! producer "foo" {:foo :bar})
  (fa/list-topics admin-client)

  (clojure.pprint/pprint (fa/describe-topics admin-client))
 
  (fa/list-consumer-groups admin-client)
  (clojure.pprint/pprint (fa/list-consumer-groups-offsets admin-client))
  (fa/list-consumer-groups-offsets-sum admin-client "fooee")
  )
