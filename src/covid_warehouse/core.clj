(ns covid-warehouse.core
  (:gen-class)
  (:require [clojure.string :as str]
            [covid-warehouse.writer :refer :all]
            [covid-warehouse.reader :refer :all]
            [covid-warehouse.timer :refer :all]
            [covid-warehouse.storage :refer :all]
            [clojure.java.io :as io]
            [taoensso.timbre :as l]
            [tablecloth.api :as tc]
            [xtdb.api :as xt]))

(l/merge-config! {:ns-filter #{"covid-warehouse.*"}})

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
     (report-json args series))))

(defn all-places
  "list all the places we care to see"
  [node]
  (sort
   (concat
    (timer "get PA counties"
           (map (juxt :country :state :county) (get-counties node "US" "Pennsylvania")))
    (timer "get US states"
           (map (juxt :country :state) (get-states node "US")))
    (timer "get countries"
           (map (juxt :country) (get-countries node))))))

(defn copy-file [src dest]
  (io/copy (io/input-stream (io/resource src)) (io/file dest)))

(defn copy-resources [dest]
  (copy-file "web/htaccess" (str dest "/.htaccess"))
  (copy-file "web/style.css" (str dest "/style.css")))

(defn publish-all [node dest]
  (let [places (all-places node)]
    (timer "all reports"
           (doall
            (pmap (partial report node dest) places)))
    (spit
     (str dest "/index.html")
     (index-html places))
    (spit
     (str dest "/index.json")
     (index-json places)))
  (copy-resources dest))

(defn usage-message []
  (println "
Usage:
lein run all <input-dir> <output-dir>
lein run load <input-dir>
lein run report <output-dir> <country> [state] [county]
lein run publish-all <output-dir>
lein run list-files
lein run history-file <file-name>
lein run history-place <country> <state> <county>
lein run delete-stage-days
"))

(defn stage-all-storage [node path]
  (timer "stage dates"
         (let [existing-checksums (get-stage-checksums node)
               files (get-csv-files path)
               changed-files (->>
                              files
                              (pmap (fn [f] (->> f (io/file path) file->checksum)))
                              (remove (partial contains? existing-checksums))
                              (pmap first))
               staged (pmap
                       (fn [file-name]
                         (->>
                          file-name
                          io/file
                          file->doc
                          (put-stage-day node)))
                       changed-files)]
           {:staged (count staged)
            :files (count files)})))

(def table-keys (juxt :country :state :county :date))

(defn latest-daily [col]
  (->> col
    (sort-by table-keys)
    (group-by table-keys)
    (reduce-kv
      (fn [m k v]
        (assoc m k (last v))) {})
    vals
    flatten))

(defn facts-storage [node]
  (timer "transform to facts"
         (->>
          (get-stage-days node)
          (map :places)
          (reduce concat)
          latest-daily
          (sort-by table-keys)
          (group-by location-grouping)
          (pmap
           (fn
             [[[country state county] v]]
             (let [dates (->>
                          v
                          (map
                           #(select-keys
                             %
                             [:date :cases :deaths :recoveries]))
                          (reduce calc-changes []))
                   date-count (count dates)]
               {:country country
                :state state
                :county county
                :dates dates
                :date-count date-count
                :date-earliest (->> dates first :date)
                :date-latest (->> dates last :date)
                :current? (> date-count 50)})))
          (pmap (partial put-place node))
          (count))))

(defn load-data [node input-path]
  (let [loaded (stage-all-storage node input-path)]
    (l/info (str "days changed: " (:staged loaded)))
    (l/info (str "files present: " (:files loaded))))
  (l/info (str "places: " (facts-storage node))))

(defn history-place [xtdb-node [country state county]]
  (l/info (str "history: " [country state county]))
  (doseq [h (xt/entity-history
             (xt/db xtdb-node)
             {:country country
              :state state
              :county county}
             :asc
             {:with-corrections? true
              :with-docs? false})]
    (l/info h)))

(defn history-file [xtdb-node file-name]
  (l/info (str "history: " file-name))
  (doseq
   [h (xt/entity-history
       (xt/db xtdb-node)
       file-name
       :asc
       {:with-corrections? true
        :with-docs? false})]
    (l/info h)))

(defn list-files [xtdb-node]
  (doseq
   [f (sort
       (map
         (comp keys->mem first)
        (xt/q
         (xt/db xtdb-node)
         '{:find [d]
           :where [[d :covid-warehouse.stage.day/type :stage]]})))]
    (l/info f)))

(defn -main
  [& args]

  (timer
   "MAIN"
   (do
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

           "history-place"
           (history-place xtdb-node args)

           "history-file"
           (history-file xtdb-node (first args))

           "list-files"
           (list-files xtdb-node)

           "delete-stage-days"
           (delete-stage-days xtdb-node)

           (usage-message))))
     (shutdown-agents))))

(comment

  (def xtdb-node (start-xtdb!))

  (stop-xtdb! xtdb-node)

  (-main "load" "input")

  (-main "all" "input" "output")

  (-main "report" "output" "US" "Pennsylvania")

  (-main "report" "output" "US" "Pennsylvania" "Allegheny")

  (-main "history-place" "US" "Pennsylvania" "Lancaster")

  (-main "history-file" "input/01-01-2021.csv")

  (-main "list-files")

  (report xtdb-node "output" ["US" "Pennsylvania" "Allegheny"])

  (report xtdb-node "output" ["US" "Florida"])

  (publish-all ds "output")

  ;; storage

  (stage-all-storage xtdb-node "input")

  (facts-storage xtdb-node)

  (get-stage-days xtdb-node)

  (get-stage-checksums xtdb-node)

  (get-places xtdb-node)

  (history-file xtdb-node "input/01-01-2022.csv")

  (history-place xtdb-node ["US" "Pennsylvania" "Lancaster"])

  (->>
   ["US" "Pennsylvania" "York"]
   (get-place xtdb-node)
   :dates)

  (->>
   ["US" "Pennsylvania"]
   (get-dates-by-state xtdb-node)
   (roll-history 7))

  (with-open [xtdb-node (start-xtdb!)]
    (xt/attribute-stats xtdb-node))

  (with-open [xtdb-node (start-xtdb!)]
    (xt/entity-history
     (xt/db xtdb-node)
     {:country "US" :state "Pennsylvania" :county "Lancaster"}
     :asc
     {:with-corrections? false
      :with-docs? true}))

  (->>
   (xt/q
    (xt/db xtdb-node)
    '{:find [d]
      :where [[d :type :fact]
              [d :country "US"]]})
   (map #(select-keys (first %) [:country :state]))
   distinct)

  (with-open [xtdb-node (start-xtdb!)]
    (xt/q
     (xt/db xtdb-node)
     '{:find [d]
       :where [[d :type :stage]]}))

  (->>
   (xt/q
    (xt/db xtdb-node)
    '{:find [country state county dates]
      :where [[d :type :fact]
              [d :country country]
              [d :state state]
              [d :county county]
              [d :dates dates]
              [(== country "US")]
              [(== state "Pennsylvania")]
              [(== county "Lancaster")]]
      :timeout 6000}))

  (->>
   (xt/q (xt/db xtdb-node)
         '{:find [(pull d [:country :state :county {:dates [*]}])]
           :where [[d :type :fact]
                   [d :country "US"]
                   [d :state "Pennsylvania"]
                   [d :county "Lancaster"]]
           :timeout 6000}))

  (all-places xtdb-node)

  (defmethod xtdb.query/aggregate 'sort-reverse [_]
    (fn
      ([] [])
      ([acc] (vec (reverse (sort acc))))
      ([acc x] (conj acc x))))

  nil)
