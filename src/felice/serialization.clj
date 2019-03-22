(ns ^:no-doc felice.serialization
  (:require [cognitect.transit :as transit])
  (:import [org.apache.kafka.common.serialization Serializer       Deserializer
                                                  LongSerializer   LongDeserializer
                                                  StringSerializer StringDeserializer]
           [java.io ByteArrayInputStream ByteArrayOutputStream]))

(defn transit-serializer [type]
  (reify
    Serializer
    (close [this])
    (configure [this config is-key?])
    (serialize [this topic payload]
      (let [out (ByteArrayOutputStream.)
            writer (transit/writer out type)]
        (transit/write writer payload)
        (.toByteArray out)))))

(defn transit-deserializer [type]
    (reify
      Deserializer
      (close [this])
      (configure [this config is-key?])
      (deserialize [this topic payload]
        (let [in (ByteArrayInputStream. payload)
              reader (transit/reader in type)]
          (transit/read reader)))))

(def serializers {:long   (fn [] (LongSerializer.))
                  :string (fn [] (StringSerializer.))
                  :t+json   (partial transit-serializer :json)
                  :t+mpack  (partial transit-serializer :msgpack)})

(def deserializers {:long    (fn [] (LongDeserializer.))
                    :string  (fn [] (StringDeserializer.))
                    :t+json  (partial transit-deserializer :json)
                    :t+mpack (partial transit-deserializer :msgpack)})

(defn ^serializer serializer [s]
  (if (keyword? s)
    (if-let [serializer (serializers s)]
      (serializer)
      (throw (ex-info "failed to initialize kafka serializer"
                      {:cause   (str "unknown serializer alias " s)
                       :allowed (keys serializers)})))
    s))

(defn ^deserializer deserializer [d]
  (if (keyword? d)
    (if-let [deserializer (deserializers d)]
      (deserializer)
      (throw (ex-info "failed to initialize kafka deserializer"
                      {:cause   (str "unknown deserializer alias " d)
                       :allowed (keys deserializers)})))
    d))
