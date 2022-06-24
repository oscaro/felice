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
                         (consumer/poll->all-records
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

(defn poll-chans
  [consumer poll-timeout topic->chan control-chan]
  (let [continue? (atom true)
        completion
        (future
          (try
            (while @continue?
              (let [poll (try (consumer/poll consumer poll-timeout)
                              (catch WakeupException _))
                    records-by-topic (consumer/poll->records-by-topic poll)]
                (doseq [[topic records] records-by-topic]
                  (async/onto-chan (topic->chan topic) records false)))
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
              (.close consumer))))]
    (fn close! []
      (reset! continue? false)
      (deref completion))))

(defn consumer
  ([conf key-deserializer value-deserializer topic->chan]
   {:pre [async-enabled?]}
   (let [consumer (consumer/consumer conf key-deserializer value-deserializer)
         control-chan (async/chan 100)]
     (apply consumer/subscribe consumer (keys topic->chan))
     (let [close-polling! (poll-chans consumer 2000 topic->chan control-chan)]
       {:consumer     consumer
        :chans        topic->chan
        :control-chan control-chan
        :close! (fn []
                  (close-polling!)
                  (async/close! control-chan)
                  (doseq [chan (vals topic->chan)]
                    (async/close! chan)))})))
  ([conf key-deserializer value-deserializer records-chan & topics]
   {:pre [async-enabled?]}
   (let [consumer (consumer/consumer conf key-deserializer value-deserializer)
         control-chan (async/chan 100)]
     (apply consumer/subscribe consumer topics)
     (let [polling (poll-chan consumer 2000 records-chan control-chan)]
       {:consumer consumer
        :records-chan records-chan
        :control-chan control-chan
        :close!  (fn []
                   (async/close! records-chan)
                   (async/close! control-chan)
                   (deref polling))}))))

(defn close! [{:keys [close!] :as consumer}] (close!))
