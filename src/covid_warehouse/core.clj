(ns covid-warehouse.core
  (:gen-class)
  (:require [clojure.string :as str]
            [covid-warehouse.writer :refer :all]
            [covid-warehouse.reader :refer :all]
            [covid-warehouse.timer :refer :all]
            [covid-warehouse.storage :refer :all]
            [taoensso.timbre :as l]
            [java-time :as t]
            [clojure.java.io :as io]
            [covid-warehouse.storage :as storage]
            [xtdb.api :as xt]))

(defn dw-series [node country state county]
  (doall
   (cond
     (and (nil? state) (nil? county))
     (get-dates-by-country node [country])

     (nil? county)
     (get-dates-by-state node [country state])

     :else
     (get-dates-by-county node [country state county]))))

(defn roll-history [days coll]
  (->>
   coll
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

(defn report [node dest args]
  (timer (str "  report " args)
         (let [[country state county] args
               series (roll-history
                        7
                        (dw-series node country state county))
               q-file-name (file-name country state county)]
           (spit
            (str dest "/" (html-file-name q-file-name))
            (report-html args series))
           (spit
            (str dest "/" (json-file-name q-file-name))
            (report-json args series)))))

(defn sql-date-last-week
  "provide a sql date for a week ago for cutoff dates"
  []
  (t/sql-date (t/adjust (t/local-date) t/minus (t/days 7))))

(def all-places
  "list all the places we care to see"
  (sort
    [
     ["US" "Pennsylvania" "York"]
     ["US" "Pennsylvania" "Lancaster"]
     ["US" "Pennsylvania" "Adams"]
     ["US" "Pennsylvania" "Allegheny"]
     ["US" "Pennsylvania" "Philadelphia"]
     ["US" "Pennsylvania" "Lebanon"]
     ["US" "Pennsylvania" "Dauphin"]
     ["US" "Pennsylvania"]
     ["US" "Delaware"]
     ["US" "Florida"]
     ["US" "New York"]
     ["US" "California"]
     ["US"]
     ["India"]
     ["Canada"]
     ["Mexico"]
     ["United Kingdom"]
     ["France"]
     ["Germany"]
     ["Japan"]
     ["China"]]))

(defn copy-file [src dest]
  (io/copy (io/input-stream (io/resource src)) (io/file dest)))

(defn copy-resources [dest]
  (copy-file "web/htaccess" (str dest "/.htaccess"))
  (copy-file "web/style.css" (str dest "/style.css")))

(defn publish-all [node dest]
  (timer "all reports"
    (doall
      (pmap (partial report node dest) all-places)))
  (spit
    (str dest "/index.html")
    (index-html all-places))
  (spit
    (str dest "/index.json")
    (index-json all-places))
  (copy-resources dest))

(defn usage-message []
  (println "
Usage:
lein run all <input-dir> <output-dir>
lein run load <input-dir>
lein report <output-dir> 'US' 'Pennsylvania'
"))

(defn stage-all-storage [node path]
  (timer "load"
         (->> path
              get-csv-files
              (pmap
               (fn [file-name]
                 (->>
                  file-name
                  (io/file path)
                  file->doc
                  (put-stage-day node))))
              count)))

(defn facts-storage [node]
  (timer "facts"
         (->> (get-stage-days node)
              (map (comp :places first))
              (reduce concat)
              latest-daily
              (sort-by table-keys)
              (group-by location-grouping)
              (pmap
               (fn
                 [[[country state county] v]]
                 {:country country
                  :state state
                  :county county
                  :dates (->>
                          v
                          (map
                           #(select-keys
                             %
                             [:date :cases :deaths :recoveries]))
                          (reduce calc-changes []))}))
              (pmap (partial put-place node))
              doall)))

(defn load-data [node input-path]
  (stage-all-storage node input-path)
  (facts-storage node))

(defn -main
  [& args]

  (timer "MAIN"
    (let [[action & args] args]
      (with-open [xtdb-node (start-xtdb!)]
        (case action
          "load"
          (load-data xtdb-node (first args))

          "report"
          (report xtdb-node (first args) (rest args))

          "publish-all"
          (publish-all xtdb-node (first args))

          "all"
          (do
            (load-data xtdb-node (first args))
            (publish-all xtdb-node (second args)))

          (usage-message)))
      )))

(comment

  (def xtdb-node (start-xtdb!))

  (timer "insert"
         (->>
          "/home/john/workspace/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports"
          get-data-from-files
          (timer " retrieving")
          (put-stage-doc xtdb-node)
          count))

  (timer "input"
         (->>
          (get-data-from-files "/home/john/workspace/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports")
          (group-by :date)
          (pmap (fn [[k v]] (assoc {} :date (str k) :places v)))
          (pmap (partial put-stage-day xtdb-node))
          doall))

  (timer "query"
         (get-stage-days xtdb-node))

  (timer "query"
         (count (get-stage-docs xtdb-node)))

  (-main "load"
         "/home/john/workspace/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports")

  (-main "all"
         "/home/john/workspace/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports"
         "output")

  (-main "report" "output" "US" "Pennsylvania")

  (-main "report" "output" "US" "Pennsylvania" "Allegheny")

  (report xtdb-node "output" ["US" "Pennsylvania" "Allegheny"])

  (report xtdb-node "output" ["US" "Florida"])

  (publish-all ds "output")

  ;; storage

  (stage-all-storage
   xtdb-node
   "/home/john/workspace/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports")

  (facts-storage xtdb-node)

  (get-stage-days xtdb-node)

  (get-places xtdb-node)

  (->>
   ["US" "Pennsylvania" "York"]
   (get-place xtdb-node)
   :dates)

  (->>
   ["US" "Pennsylvania"]
   (get-dates-by-state xtdb-node)
   (roll-history 7))

  nil)
