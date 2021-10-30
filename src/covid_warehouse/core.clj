(ns covid-warehouse.core
  (:gen-class)
  (:require [clojure.string :as str]
            [covid-warehouse.db :refer :all]
            [covid-warehouse.writer :refer :all]
            [covid-warehouse.timer :refer :all]
            [java-time :as t]
            [next.jdbc :as jdbc]
            [clojure.java.io :as io]
            [clojure.pprint :as pp]))

(defn dw-series [ds country state county]
  (doall
   (cond
     (and (nil? state) (nil? county))
     (dw-series-by-country
      ds
      {:country country})

     (nil? county)
     (dw-series-by-state
      ds
      {:country country :state state})

     :else
     (dw-series-by-county
      ds
      {:country country :state state :county county}))))

(defn load-db [con path]
  (timer "load data"
         (do
           (timer "create staging table"
             (create-stage! con))
           (timer "stage data"
                  (stage-data!
                   con
                   path))

           (timer "create dimension tables"
             (create-dims! con))
           (timer "load locations"
                  (load-dim-location! con))
           (timer "load dates"
                  (load-dim-date! con))

           (timer "create fact table"
             (do
               (drop-fact-day! con)
               (create-fact-day! con)))
           (timer "load facts"
                  (load-fact-day! con)))))

(defn roll-history [days coll]
  (->> coll
       (partition-all days 1)
       (map
        (fn [d]
          (let [deaths (map :death-change d)
                cases (map :case-change d)
                recoveries (map :recovery-change d)]
            (merge
             (first d)
             {:death-change-history (int (mean deaths))
              :case-change-history (int (mean cases))
              :recovery-change-history (int (mean recoveries))}))))))

(defn query [con dest args]
  (timer (str "query " args)
         (let [[country state county] args
               series (roll-history 7 (map shorten-keys (dw-series con country state county)))]
           (spit
            (str dest "/" (html-file-name (file-name country state county)))
            (report series))
           (spit
            (str dest "/" (json-file-name (file-name country state county)))
            (report-json series)))))

(defn all-places
  "list all the places we care to see"
  [con]
  (timer "all places"
         (sort
          (apply concat
                 (pcalls
                  #(timer "counties"
                          (map (juxt :country :state :county)
                               (distinct-counties-by-state-country
                                con
                                {:country "US" :state "Pennsylvania"})))
                  #(timer "us states"
                          (map (juxt :country :state)
                               (distinct-states-by-country con {:country "US"})))
                  #(timer "canada provinces"
                          (map (juxt :country :state)
                               (distinct-states-by-country con {:country "Canada"})))
                  #(timer "countries"
                          (-> [["US"]
                               ["India"]
                               ["Israel"]
                               ["Canada"]
                               ["Greece"]
                               ["Mexico"]
                               ["Italy"]
                               ["France"]
                               ["United Kingdom"]
                               ["Brazil"]
                               ["Japan"]
                               ["China"]
                               ["Korea, South"]
                               ["New Zealand"]])))))))

(defn copy-file [src dest]
  (io/copy (io/input-stream (io/resource src)) (io/file dest)))

(defn copy-resources [dest]
  (copy-file "web/htaccess" (str dest "/.htaccess"))
  (copy-file "web/style.css" (str dest "/style.css")))

(defn publish-all [ds dest]
  (let [all-places (all-places ds)]
    (timer "all reports"
           (doall
             (pmap (partial query ds dest) all-places)))
    (timer "writing index.html"
      (spit
        (str dest "/index.html")
        (index-html all-places)))
    (timer "writing index.json"
      (spit
        (str dest "/index.json")
        (index-json all-places))))
  (timer "copy resources"
    (copy-resources dest)))

(defn usage-message []
  (println "
Usage:
lein run all <input-dir> <output-dir>
lein run load <input-dir>
lein query <output-dir> 'US' 'Pennsylvania'
"))

(defn counts [ds]
  {:facts (first (vals (count-facts ds)))
   :dates (first (vals (count-dates ds)))
   :locations (first (vals (count-locations ds)))
   :stage (first (vals (count-stage ds)))})

(defn -main
  [& args]

  (let [[action & args] args]
    (case action
      "load"
      (jdbc/with-transaction [con ds]
        (load-db con (first args)))

      "query"
      (query ds (first args) (rest args))

      "publish-all"
      (publish-all ds (first args))

      "all"
      (do
        (jdbc/with-transaction [con ds]
          (load-db con (first args)))
        (pp/pprint (counts ds))
        (publish-all ds (second args)))

      (usage-message))))

(comment
  (-main "load" "/home/john/workspace/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports")

  (-main "load" "test/files")

  (-main "all" "/home/john/workspace/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports" "output")

  (-main "query" "output" "US" "Pennsylvania")

  (-main "query" "output" "US" "New York")

  (-main "query" "output" "US" "Alabama")

  (-main "query" "output" "Afghanistan")

  (-main "query" "output" "US" "Pennsylvania" "Lancaster")

  (let [[country state county] ["US" "Pennsylvania" "Lancaster"]
        con ds
        series (map shorten-keys (dw-series con country state county))]
    (report series))

  (diff-queries
   dw-series-by-county
   dw-series-by-county
   {:country "US" :state "Pennsylvania" :county "York"}
   (juxt :date :case_change))

  (jdbc/execute! ds ["select distinct country, state from dim_location"])

  (map (comp vals) (jdbc/execute! ds ["
    select l.country as country, l.state as state, l.county as county, sum(death_change) as deaths
    from fact_day f
    join dim_location l on l.location_key = f.location_key
    where l.country = 'US'
    and l.state = 'New York'
    group by l.country, l.state, l.county
    having deaths > 0
    order by deaths desc"]))

  (map (comp (partial conj []) :country) (distinct-countries ds))

  (map (comp (partial conj []) (juxt :country :state)) (distinct-states-by-country ds {:country "US"}))

  (distinct-states-by-country ds {:country "US"})

  (first (vals (count-facts ds)))

  nil)
