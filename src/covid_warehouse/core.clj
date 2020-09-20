(ns covid-warehouse.core
  (:gen-class)
  (:require [clojure.string :as str]
            [covid-warehouse.db :refer :all]
            [covid-warehouse.writer :refer :all]
            [java-time :as t]
            [next.jdbc :as jdbc]
            [clojure.java.io :as io]))

(defn dw-series [ds country state county]
  (doall
   (cond
     (and (nil? state) (nil? county))
     (dw-rolling-series-by-country ds {:country country})

     (nil? county)
     (dw-rolling-series-by-state ds {:country country :state state})

     :else
     (dw-rolling-series-by-county ds {:country country :state state :county county}))))

(defmacro timer
  "Evaluates expr and prints the time it took.  Returns the value of expr."
  {:added "1.0"}
  [msg expr]
  `(let [start# (. System (nanoTime))
         ret# ~expr]
     (println (str ~msg ": " (/ (double (- (. System (nanoTime)) start#)) 1000000.0) " msecs"))
     ret#))

(defn load-db [con path]
  (timer "loading"
         (do
           (create-stage! con)
           (stage-data!
            con
            path)

           (create-dims! con)
           (load-dim-location! con)
           (load-dim-date! con)

           (drop-fact-day! con)
           (create-fact-day! con)
           (load-fact-day! con))))

(defn query [con args]
  (timer (str "query " args)
         (let [[country state county] args
               series (map shorten-keys (dw-series con country state county))]
           (spit
            (str "output/" (html-file-name (file-name country state county)))
            (report series)))))

(defn all-places
  "list all the places we care to see"
  [con]
  (timer "all-places"
         (sort
          (apply concat
                 (pcalls
                  #(timer "counties"
                          (map (juxt :country :state :county)
                               (distinct-counties-by-state-country
                                con
                                {:country "US" :state "Pennsylvania"})))
                  #(timer "states"
                          (map (juxt :country :state)
                               (distinct-states-by-country con {:country "US"})))
                  #(timer "countries"
                          (-> [["US"]
                               ["United Kingdom"]
                               ["Brazil"]
                               ["Japan"]
                               ["China"]
                               ["Korea, South"]
                               ["Hong Kong"]
                               ["New Zealand"]
                               ["France"]]))
                  #_(timer "countries"
                           (map (juxt :country)
                                (distinct-countries con))))))))

(defn copy-style []
  (io/copy (io/file (io/resource "web/style.css")) (io/file "output/style.css")))

(defn -main
  [action & args]

  (cond
    (= "load" action)
    (jdbc/with-transaction [con ds]
      (load-db con (first args)))
    (= "query" action)
    (query ds args)
    (= "all" action)
    (do
      (jdbc/with-transaction [con ds]
        (load-db con (first args)))
      (let [all-places (all-places ds)]
        (timer "all reorts"
               (doall
                (pmap (partial query ds) all-places)))
        (spit
         "output/index.html"
         (index-file all-places)))
      (copy-style))))

(comment
  (-main "load" "/home/john/workspace/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports")

  (-main "all" "/home/john/workspace/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports")

  (-main "query" "US" "Pennsylvania")

  (-main "query" "US" "New York")

  (-main "query" "US" "Alabama")

  (-main "query" "Afghanistan")

  (-main "query" "US" "Pennsylvania" "Lancaster")

  (let [[country state county] ["US" "Pennsylvania" "Lancaster"]
        con ds
        series (map shorten-keys (dw-series con country state county))]
    (report series))

  (diff-queries
   dw-series-by-county
   dw-rolling-series-by-county
   {:country "US" :state "Pennsylvania" :county "York"}
   (juxt :date :case_change))

  (diff-queries
   dw-series-by-country
   dw-rolling-series-by-country
   {:country "US"}
   (juxt :date :case_change :death_change))

  (jdbc/execute! ds ["select distinct country, state from dim_location"])

  (map (comp (partial conj []) :country) (distinct-countries ds))

  (map (comp (partial conj []) (juxt :country :state)) (distinct-states-by-country ds {:country "US"}))

  (distinct-states-by-country ds {:country "US"})

  (timer "thing" (+ 1 1))

  nil)
