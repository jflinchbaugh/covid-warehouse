(ns covid-warehouse.reader
  (:require [covid-warehouse.timer :refer :all]
            [clojure.data.csv :as csv]
            [tick.core :as t]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clj-commons.digest :as digest]
            [tablecloth.api :as tc]))

(defn get-csv-files [path]
  (->> path
       io/file
       .list
       (filter (partial re-matches #".*\.csv"))))

(defn- convert-date-str [fmt-str s]
  (-> s
      (t/parse-date (t/formatter fmt-str))
      (t/at (t/time "00:00"))))

(defn- convert-date-time-str [fmt-str s]
  (t/parse-date-time s (t/formatter fmt-str)))

(defn parse-date [s]
  (->
   (cond
     (re-matches #"\d+/\d+/\d{4}" s)
     (convert-date-str "M/d/yyyy" s)

     (re-matches #"\d+/\d+/\d{2}" s)
     (convert-date-str "M/d/yy" s)

     (re-matches #"\d+/\d+/\d{2} \d+:\d+" s)
     (convert-date-time-str "M/d/yy H:m" s)

     (re-matches #"\d+/\d+/\d{4} \d+:\d+" s)
     (convert-date-time-str "M/d/yyyy H:m" s)

     (re-matches #"\d+-\d+-\d+T\d+:\d+:\d+" s)
     (convert-date-time-str "y-M-d'T'H:m:s" s)

     (re-matches #"\d+-\d+-\d+ \d+:\d+:\d+" s)
     (convert-date-time-str "y-M-d H:m:s" s)

     (re-matches #"\d+-\d+-\d+ \d+:\d+" s)
     (convert-date-time-str "y-M-d H:m" s)

     :else
     (throw (IllegalArgumentException. (str "Bad date: " s))))
   (t/<< (t/new-duration 1 :days))
   t/date))

(def location-grouping (juxt :country :state :county))

(def table-keys (juxt :country :state :county :date))

(defn calc-changes
  "given the list of days, calculate daily changes and add it to the list"
  [lst new]
  (let [prev (last lst)]
    (conj
     lst
     (merge
      new
      {:cases-change
       (-
        (or (:cases new) 0)
        (or (:cases prev) 0))
       :deaths-change
       (-
        (or (:deaths new) 0)
        (or (:deaths prev) 0))
       :recoveries-change
       (-
        (or (:recoveries new) 0)
        (or (:recoveries prev) 0))}))))

(defn latest-daily [col]
  (->> col
       (sort-by table-keys)
       (group-by table-keys)
       (reduce-kv
        (fn [m k v]
          (assoc m k (last v))) {})
       vals
       flatten))

(defn unify-country-name [c]
  (get {"UK" "United Kingdom"
        "Mainland China" "China"
        "Taiwan" "Taiwan*"
        "South Korea" "Korea, South"} c c) ; pass-through if not found
  )

(defn overlap-location? [r]
  (= ((juxt :country :state :county) r) ["US" "New York" "New York City"]))

(defn file->checksum [file]
  [(str file) (digest/md5 file)])

(def column-map
  {:admin2 :county
   :province_state :state
   :province/state :state
   :country_region :country
   :country/region :country
   :confirmed :cases
   :deaths :deaths
   :case-fatality_ratio :case-fatality-ratio
   :case_fatality_ratio :case-fatality-ratio
   :recovered :recoveries
   :incident_rate :incident-rate
   :incidence_rate :incident-rate
   :lat :latitude
   :long_ :longitude
   :last_update :date
   (keyword "last update") :date})

(defn cleanup [ds]
  (-> ds
    (tc/rename-columns column-map)
    (tc/drop-rows overlap-location?)
    (tc/update-columns
      {:date (partial pmap (comp str parse-date))
       :country (partial pmap unify-country-name)})
    (tc/select-columns
      [:county :state :country :date :cases :deaths :recoveries])
    (tc/replace-missing [:county :state :country] :value "")))

(defn file->doc [file]
  (let [places (-> file
                 (tc/dataset {:key-fn (comp keyword str/lower-case)})
                 cleanup
                 (tc/rows :as-maps)
                 seq)
        checksum (digest/md5 file)]
    {:file-name (str file)
     :checksum checksum
     :places places}))
