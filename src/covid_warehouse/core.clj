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

(defn load-db [ds path]
  (timer "load data"
         (with-open [con (jdbc/get-connection ds)]
           (timer "create staging tables"
                  (create-stage! con))
           (timer "load checksums"
                  (stage-checksums! con path))
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
                  (create-facts! con))
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

(defn query [ds dest args]
  (timer (str "query " args)
         (with-open [con (jdbc/get-connection ds)]
           (let [[country state county] args
                 series (roll-history
                         7
                         (map
                          shorten-keys
                          (dw-series con country state county)))]
             (spit
              (str dest "/" (html-file-name (file-name country state county)))
              (report series))
             (spit
              (str dest "/" (json-file-name (file-name country state county)))
              (report-json series))))))

(defn sql-date-last-week
  "provide a sql date for a week ago for cutoff dates"
  []
  (t/sql-date (t/adjust (t/local-date) t/minus (t/days 7))))

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
                                {:cutoff-date (sql-date-last-week)
                                 :country "US"
                                 :state "Pennsylvania"})))
                  #(timer "us states"
                          (map (juxt :country :state)
                               (distinct-states-by-country
                                con
                                {:cutoff-date (sql-date-last-week)
                                 :country "US"})))
                  #(timer "canada provinces"
                          (map (juxt :country :state)
                               (distinct-states-by-country
                                con
                                {:cutoff-date (sql-date-last-week)
                                 :country "Canada"})))
                  #(timer "countries"
                          (-> [["US"]
                               ["India"]
                               ["Israel"]
                               ["Canada"]
                               ["Mexico"]
                               ["Italy"]
                               ["France"]
                               ["United Kingdom"]
                               ["Brazil"]
                               ["Japan"]
                               ["China"]
                               ["Korea, South"]
                               ["New Zealand"]
                               ["South Africa"]
                               ["Germany"]])))))))

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
  (with-open [con (jdbc/get-connection ds)]
    {:facts (first (vals (count-facts con)))
     :dates (first (vals (count-dates con)))
     :locations (first (vals (count-locations con)))
     :stage (first (vals (count-stage con)))}))

(defn -main
  [& args]

  (let [[action & args] args]
    (case action
      "load"
      (load-db ds (first args))

      "query"
      (query ds (first args) (rest args))

      "publish-all"
      (publish-all ds (first args))

      "all"
      (do
        (load-db ds (first args))
        (pp/pprint (counts ds))
        (publish-all ds (second args)))

      (usage-message))))

(comment
  (-main "load"
         "/home/john/workspace/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports")

  (-main "all"
         "/home/john/workspace/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports"
         "output")

  (-main "query" "output" "US" "Pennsylvania")

  (-main "query" "output" "US" "Pennsylvania" "Lancaster")

  (jdbc/execute! ds ["select distinct \"country\", \"state\" from dim_location"])

  nil)
