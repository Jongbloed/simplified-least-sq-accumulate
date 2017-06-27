(defproject scct "0.0.1-SNAPSHOT"
  :description "Spark Kaggle Clojure Thing"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [yieldbot/flambo "0.8.0"]
                 [clj-time/clj-time "0.13.0"]
                 [org.apache.spark/spark-core_2.11 "2.1.1"]]
  :javac-options ["-target" "1.6" "-source" "1.6" "-Xlint:-options"]
  :aot [scct.core]
  :main scct.core
  :profiles
    {:provided
      {:dependencies
       [[org.apache.spark/spark-core_2.11 "2.1.1"]]}})
    ; :dev {:aot [scct.core]}})
