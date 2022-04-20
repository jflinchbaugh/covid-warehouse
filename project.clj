(defproject covid-warehouse "0.1.0-SNAPSHOT"
  :description "data warehouse exercise for analyzing covid-19 data"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :plugins []
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/data.csv "1.0.1"]
                 [com.taoensso/timbre "5.2.1"] 
                 [clojure.java-time "0.3.3"]
                 [hiccup "1.0.5"]
                 [com.github.seancorfield/next.jdbc "1.2.780"]
                 [com.h2database/h2 "2.1.212"]
                 [com.layerware/hugsql-adapter-next-jdbc "0.5.3"]
                 [com.layerware/hugsql-core "0.5.3"]
                 [cheshire "5.10.2"]
                 [org.clj-commons/digest "1.4.100"]
                 [org.postgresql/postgresql "42.3.4"]]
  :main covid-warehouse.core
  :target-path "target/%s"
  :jvm-opts ["-server" "-XX:MaxRAMPercentage=75" "-XX:MinRAMPercentage=75"]
  :profiles {:uberjar {:aot :all}})
