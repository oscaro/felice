(ns user
  (:require [felice.consumer :as fc]
            [unilog.config :as ul]
            [clojure.tools.namespace.repl :refer :all]))

(comment

  (ul/start-logging! (merge ul/default-configuration
                            {:overrides {"org.apache.kafka.common" :warn
                                         "org.apache.kafka" :warn}}))

  (def consumer-configuration
    {:bootstrap.servers "localhost:9092"
     :group.id "my-group"
     :auto.offset.reset "latest"
     :key.deserializer  :string
     :value.deserializer :json
     :enable.auto.commit true
     :max.poll.records 1
     :topics #{"topic-2"}})

  (def handler-fn
    (fn [x]
      (clojure.pprint/pprint x)))

  (def poll-loop
    (fc/poll-loop-ng consumer-configuration
                     handler-fn
                     {}))

  ((:stop-fn poll-loop))

  (let [{:keys [suspend!]} poll-loop]
    (suspend!))

  (let [{:keys [resume!]} poll-loop]
    (resume!))
  
  (let [{:keys [stop-fn]} poll-loop]
    (stop-fn))

  )
