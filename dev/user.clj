(ns user
  (:require [felice.consumer :as fc]
            [felice.producer :as fp]
            [felice.admin    :as fa]
            [clojure.reflect :as r]
            [clojure.pprint  :refer [print-table]]))

(defn- dump-object-methods [o]
  (print-table (:members (r/reflect o))))

(def broker "localhost:9092")
