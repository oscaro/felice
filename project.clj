(defproject com.oscaro/felice "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.apache.kafka/kafka-clients "2.1.0"]
                 [com.cognitect/transit-clj "0.8.313"]]
  :profiles {:dev {:dependencies [[org.slf4j/slf4j-jdk14 "1.7.25"]]}})
