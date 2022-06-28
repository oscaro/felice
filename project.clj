(defproject com.oscaro/felice "3.2.0-1.6-SNAPSHOT"
  :description "Felice is client library for Apache Kafka in Clojure"
  :url "https://gitlab.oscaroad.com/it-dev/felice"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins [[lein-marginalia "0.9.1"]]
  :deploy-repositories [["snapshots" {:url "https://repo.clojars.org"
                                      :username :env/clojars_username
                                      :password :env/clojars_password
                                      :sign-releases false}]
                        ["releases"  {:url "https://repo.clojars.org"
                                      :username :env/clojars_username
                                      :password :env/clojars_password
                                      :sign-releases false}]]
  :dependencies [[org.clojure/clojure            "1.10.1"]
                 [org.apache.kafka/kafka-clients "3.2.0"]
                 [com.cognitect/transit-clj      "0.8.319" :exclusions [com.fasterxml.jackson.core/jackson-core]]
                 [metosin/jsonista               "0.2.5"]
                 [com.taoensso/nippy             "3.1.3"]]
  :profiles {:dev {:dependencies [[org.slf4j/slf4j-jdk14  "1.7.29" :exclusions [org.slf4j/slf4j-api]]
                                  [org.clojure/core.async "0.5.527"]
                                  [spootnik/unilog        "0.7.30"]]
                   :source-paths ["dev"]}}
   :repl-options {:init-ns user})
