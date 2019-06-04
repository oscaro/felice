(defproject com.oscaro/felice "0.1.0"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :repositories [["oscaro-releases"  {:url "https://artifactory.oscaroad.com/artifactory/libs-release-local"}]
                 ["oscaro-snapshots" {:url "https://artifactory.oscaroad.com/artifactory/libs-snapshot-local"}]
                 ["oscaro-remote"    {:url "https://artifactory.oscaroad.com/artifactory/remote-repos"}]]
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins [[lein-codox "0.10.6"]]
  :codox {:output-path "public"
          :source-uri "https://gitlab.oscaroad.com/it-dev/felice/blob/{git-commit}/{filepath}#L{line}"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.apache.kafka/kafka-clients "2.1.0"]
                 [com.cognitect/transit-clj "0.8.313"]
                 [metosin/jsonista "0.2.2"]]
  :profiles {:dev {:dependencies [[org.slf4j/slf4j-jdk14 "1.7.25"]
                                  [org.clojure/core.async "0.4.490"]]}})
