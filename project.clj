(defproject covid-warehouse "0.1.0-SNAPSHOT"
  :description "data warehouse exercise for analyzing covid-19 data"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/data.csv "1.0.0"]
                 [clojure.java-time "0.3.2"]
                 [hiccup "1.0.5"]
                 [seancorfield/next.jdbc "1.1.610"]
                 [com.h2database/h2 "1.4.200"]
                 [com.layerware/hugsql-adapter-next-jdbc "0.5.1"]
                 [com.layerware/hugsql-core "0.5.1"]]
  :main ^:skip-aot covid-warehouse.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
