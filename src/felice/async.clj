(ns felice.async
  (:require [felice.consumer :as consumer])
  (:import org.apache.kafka.common.errors.WakeupException))

(def async-enabled?
  (try
    (require '[clojure.core.async :as async])
    true
    (catch Throwable _ false)))

(defn- consumer-control [consumer [type data]]
  (try
    (condp = type
      :commit-message (consumer/commit-message-offset consumer data)
      (str "unknown control type" type))
    (catch WakeupException _)))

(defn commit-message-offset [consumer message]
  (println (async/>!! (:control-chan consumer)
                      [:commit-message message]))
            
  (consumer/wakeup (:consumer consumer)))

(defn poll-chan
  [consumer poll-timeout records-chan control-chan]
  (future
    (try
      (while (not (clojure.core.async.impl.protocols/closed? records-chan))
        (async/onto-chan records-chan
                         (consumer/consumer-records->all-records
                          (try (consumer/poll consumer poll-timeout)
                               (catch WakeupException _)))
                         false)
        ;; process consumer controls
        (loop []
          (let [res (async/alt!!
                      control-chan ([control]
                                    (if control
                                      (consumer-control consumer control)
                                      :closed))
                      (async/timeout 100) :timeout)]
            (when-not (#{:timeout :closed} res)
              (recur)))))
      :ok
      (catch Throwable t t)
      (finally
        (.close consumer)))))

(defn consumer [conf key-deserializer value-deserializer chan & topics]
  {:pre [async-enabled?]}
  (let [consumer (consumer/consumer conf key-deserializer value-deserializer)
        control-chan (async/chan 100)]
    (apply consumer/subscribe consumer topics)
    {:consumer consumer
     :records-chan chan
     :control-chan control-chan
     :poll-chan (poll-chan consumer 2000 chan control-chan)}))


(defn close! [{:keys [records-chan control-chan poll-chan] :as consumer}]
  (async/close! records-chan)
  (async/close! control-chan)
  (deref poll-chan))
