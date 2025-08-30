(ns user
  (:require [felice.consumer :as fc]
            [unilog.config :as ul]
            [clojure.tools.namespace.repl :refer :all]))

(comment

  (ul/start-logging! (merge ul/default-configuration
                            {:overrides {"org.apache.kafka.common" :warn
                                         "org.apache.kafka" :warn}}))

  (def consumer-builder
    (partial fc/consumer
             {:bootstrap.servers "localhost:9092"
              :group.id "my-group"
              :auto.offset.reset "latest"
              :key.deserializer  :string
              :value.deserializer :json
              :enable.auto.commit true
              :max.poll.records 1
              :topics #{"topic-2"}}))

  (def poll-loop
    (fc/poll-loop-ng* consumer-builder
                      (fn [payload]
                        (println payload)
                        (Thread/sleep 1000000))
                      {}))

  ((:stop-fn poll-loop))

  )
