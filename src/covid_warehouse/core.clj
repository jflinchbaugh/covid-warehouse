(ns covid-warehouse.core
  (:gen-class)
  (:require [java-time :as t]
            [clojure.string :as str]
            [covid-warehouse.db-warehouse :refer :all]
            [covid-warehouse.db-queries :refer :all]))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]

  (println "staging data")
  (stage-data! ds "/home/john/workspace/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports")

  (println "loading dimensions")
  (create-dims! ds)
  (load-dim-location! ds)
  (load-dim-date! ds)

  (println "loading facts")
  (create-fact-day! ds)
  (load-fact-day! ds)

  (println "querying for lancaster")
  (->>
    (dw-series-by-county ds "US" "Pennsylvania" "Lancaster")
    (map
      (comp
        println
        (partial str/join " ")
        (juxt
          :DIM_DATE/YEAR
          :DIM_DATE/MONTH
          :DIM_DATE/DAY_OF_MONTH
          :DIM_LOCATION/COUNTRY
          :DIM_LOCATION/STATE
          :DIM_LOCATION/COUNTY
          :FACT_DAY/CASE_CHANGE
          :FACT_DAY/DEATH_CHANGE
          :FACT_DAY/RECOVERY_CHANGE)))
    doall)

  (println "totals")
  (->>
    (dw-sums-by-county ds "US" "Pennsylvania" "Lancaster")
    (map (comp println (partial str/join " ") vals))
    doall))


(comment
  (-main)

  (stage-data! ds "/home/john/workspace/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports")

  (create-dims! ds)

  (load-dim-location! ds)

  (load-dim-date! ds)

  (dim-locations ds)

  (dim-dates ds)

  (create-fact-day! ds)

  (load-fact-day! ds)

  (take 20 (fact-days ds))

  (take 20 (map :COVID_DAY/DATE (staged-data ds)))

  (dim->lookup (map (partial take 2) (dim-dates ds)))

  (covid-complete ds)

  (t/local-date (t/java-date) "UTC")

  (->>
   (cases-by-window ds "US" "Pennsylvania" (t/local-date) 14)
   (map (comp prn vals)))

  (->>
   (dw-series-by-county ds "US" "Pennsylvania" "Lancaster")
   (map
     (comp
       prn
       (juxt
         :DIM_DATE/YEAR
         :DIM_DATE/MONTH
         :DIM_DATE/DAY_OF_MONTH
         :DIM_LOCATION/COUNTRY
         :DIM_LOCATION/STATE
         :DIM_LOCATION/COUNTY
         :FACT_DAY/CASE_CHANGE
         :FACT_DAY/DEATH_CHANGE))))

  (->>
    (deaths-by-country ds)
    (pmap vals)
    (map prn))

  nil)
