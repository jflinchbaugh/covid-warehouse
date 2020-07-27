(ns covid-warehouse.core
  (:gen-class)
  (:require [clojure.string :as str]
            [covid-warehouse.db :refer :all]
            [covid-warehouse.writer :refer :all]
            [java-time :as t]
            [next.jdbc :as jdbc]
            [clojure.java.io :as io]))

(defn dw-series [ds country state county]
  (cond
    (nil? county)
    (->>
     (dw-series-by-state ds country state)
     doall)
    :else
    (->>
     (dw-series-by-county ds country state county)
     doall)))

(def print-day
  (comp
   println
   (partial str/join " ")
   (juxt :date :country :state :county :case-change :death-change :recovery-change)))

(defn -main
  [action & args]

  (jdbc/with-transaction [con ds]
    (cond
      (= "load" action)
      (do
        (create-stage! con)
        (stage-data!
         con
         (first args))

        (create-dims! con)
        (load-dim-location! con)
        (load-dim-date! con)

        (drop-fact-day! con)
        (create-fact-day! con)
        (load-fact-day! con))
      (= "query" action)
      (let [[country state county] args
            series (map shorten-keys (dw-series con country state county))]
        (doall (map print-day series))
        (spit (str "output/" country "-" state "-" county ".html") (report series))
        (->>
        (cond
          (nil? county) (dw-sums-by-state con country state)
          :else (dw-sums-by-county con country state county))
          (map (comp println (partial str/join " ") vals))
           doall)))))

(comment
  (-main "load" "/home/john/workspace/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports")

  (-main "query" "US" "Pennsylvania")

  (-main "query" "US" "Pennsylvania" "Lancaster")

  (let [[country state county] ["US" "Pennsylvania" "Lancaster"]
        con ds
        series (map shorten-keys (dw-series con country state county))]
    (report series))

  nil)
