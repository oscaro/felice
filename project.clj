(defproject com.oscaro/felice "3.2.0-1.5-SNAPSHOT"
  :description "Felice is client library for Apache Kafka in Clojure"
  :url "https://gitlab.oscaroad.com/it-dev/felice"
  :repositories [["oscaro-releases"  {:url "https://artifactory.oscaroad.com/artifactory/libs-release-local"}]
                 ["oscaro-snapshots" {:url "https://artifactory.oscaroad.com/artifactory/libs-snapshot-local"}]
                 ["oscaro-remote"    {:url "https://artifactory.oscaroad.com/artifactory/remote-repos"}]]
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins [[lein-codox "0.10.6"]]
  :codox {:output-path "public"
          :source-uri "https://gitlab.oscaroad.com/it-dev/felice/blob/{git-commit}/{filepath}#L{line}"}
  :dependencies [[org.clojure/clojure            "1.10.1"]
                 [org.apache.kafka/kafka-clients "3.2.0"]
                 [com.cognitect/transit-clj      "0.8.319" :exclusions [com.fasterxml.jackson.core/jackson-core]]
                 [metosin/jsonista               "0.2.5"]
                 [com.taoensso/nippy             "3.1.1"]]
  :profiles {:dev {:dependencies [[org.slf4j/slf4j-jdk14 "1.7.29" :exclusions [org.slf4j/slf4j-api]]
                                  [org.clojure/core.async "0.5.527"]]}})
