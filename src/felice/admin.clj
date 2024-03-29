(ns felice.admin
  (:import org.apache.kafka.clients.admin.AdminClient))


(defn admin-client
  "Instanciate an `AdminClient` from properties"
  {:added "3.2.0-1.7"}
  ^AdminClient
  ([props]
   (let [props* (dissoc props :key.deserializer :value.deserializer :topics)
         kac (. AdminClient (create ^java.util.Map props*))]
     kac)))


(def client (admin-client {"bootstrap.servers" "localhost:9092"}))
