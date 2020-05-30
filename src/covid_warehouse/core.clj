(ns covid-warehouse.core
  (:gen-class)
  (:require [java-time :as t]
            [clojure.string :as str]
            [covid-warehouse.db-warehouse :refer :all]
            [covid-warehouse.db-queries :refer :all]))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))

(comment
  (stage-data! ds "/home/john/workspace/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports")

  (create-dims! ds)

  (load-dim-location! ds)

  (load-dim-date! ds)

  (dim-locations ds)

  (dim-dates ds)

  (->>
   (cases-by-window ds "US" "Pennsylvania" (t/local-date) 7)
   (map (comp prn vals)))

  (->>
   (series-by-county ds "US" "Pennsylvania" "Lancaster")
   (map (comp prn vals)))

  (->>
    (deaths-by-country ds)
    (pmap vals)
    (map prn))

  nil)
