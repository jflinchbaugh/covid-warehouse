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

(defmacro timer
  "Evaluates expr and prints the time it took.  Returns the value of expr."
  {:added "1.0"}
  [msg expr]
  `(let [start# (. System (nanoTime))
         ret# ~expr]
     (println (str ~msg ": " (/ (double (- (. System (nanoTime)) start#)) 1000000.0) " msecs"))
     ret#))

(defn load-db [con path]
  (timer "load data"
         (do
           (create-stage! con)
           (timer "stage data"
                  (stage-data!
                   con
                   path))

           (create-dims! con)
           (timer "load locations"
                  (load-dim-location! con))
           (timer "load dates"
                  (load-dim-date! con))

           (drop-fact-day! con)
           (create-fact-day! con)
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

(defn query [con args]
  (timer (str "query " args)
         (let [[country state county] args
               series (roll-history 7 (map shorten-keys (dw-series con country state county)))]
           (spit
            (str "output/" (html-file-name (file-name country state county)))
            (report series))
           (spit
            (str "output/" (json-file-name (file-name country state county)))
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
  (io/copy (io/file (io/resource src)) (io/file dest)))

(defn copy-resources []
  (copy-file "web/.htaccess" "output/.htaccess")
  (copy-file "web/style.css" "output/style.css"))

(defn publish-all [ds]
  (let [all-places (all-places ds)]
    (timer "all reports"
           (doall
            (pmap (partial query ds) all-places)))
    (spit
     "output/index.html"
     (index-html all-places))
    (spit
     "output/index.json"
     (index-json all-places)))
  (copy-resources))

(defn usage-message []
  (println "
Usage:
lein run all <path>
lein run load <path>
lein query 'US' 'Pennsylvania'
"))

(defn -main
  [& args]

  (let [[action & args] args]
    (cond
      (= "load" action)
      (jdbc/with-transaction [con ds]
        (load-db con (first args)))
      (= "query" action)
      (query ds args)
      (= "publish-all" action)
      (publish-all ds)
      (= "all" action)
      (do
        (jdbc/with-transaction [con ds]
          (load-db con (first args)))
        (publish-all ds))
      :else
      (usage-message)))
  (create-stage! ds))

(comment
  (-main "load" "/home/john/workspace/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports")

  (-main "load" "test/files")

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

  nil)
