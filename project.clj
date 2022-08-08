(defproject covid-warehouse "0.1.0-SNAPSHOT"
  :description "data warehouse exercise for analyzing covid-19 data"
  :url "https://github.com/jflinchbaugh/covid-warehouse"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :plugins []
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/data.csv "1.0.1"]
                 [com.taoensso/timbre "5.2.1"]
                 [clojure.java-time "0.3.3"]
                 [hiccup "1.0.5"]
                 [cheshire "5.11.0"]
                 [org.clj-commons/digest "1.4.100"]
                 [com.xtdb/xtdb-core "1.21.0.1"]
                 [com.xtdb/xtdb-rocksdb "1.21.0.1"]
                 [org.slf4j/slf4j-nop "1.7.36"]]
  :main covid-warehouse.core
  :target-path "target/%s"
  :jvm-opts ["-server" "-XX:MaxRAMPercentage=75" "-XX:MinRAMPercentage=75"]
  :profiles {:uberjar {:aot :all}})
