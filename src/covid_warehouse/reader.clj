(ns covid-warehouse.reader
  (:require [covid-warehouse.timer :refer :all]
            [clojure.data.csv :as csv]
            [java-time.api :as t]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clj-commons.digest :as digest]))

(defn read-6 [[n2 n3 n4 n5 n6 n7]]
  {:county ""
   :state n2
   :country n3
   :date n4
   :cases n5
   :deaths n6
   :recoveries n7})

(def read-8 read-6)

(defn read-12 [[_ n1 n2 n3 n4 _ _ n7 n8 n9]]
  {:county n1
   :state n2
   :country n3
   :date n4
   :cases n7
   :deaths n8
   :recoveries n9})

(def read-14 read-12)

(defn get-csv-files [path]
  (->> path
       io/file
       .list
       (filter (partial re-matches #".*\.csv"))))

(defn read-checksums [path]
  (->>
   path
   get-csv-files
   (map
    (fn [f] [f (->> f (io/file path) digest/md5)]))))

(defn read-csv [path]
  (->>
   path
   get-csv-files
   (map
    (comp
     rest
     csv/read-csv
     io/reader
     (partial io/file path)))
   (reduce concat)))

(defn cols->maps [line]
  ((case (count line)
     6 read-6
     8 read-8
     12 read-12
     14 read-14
     (throw
      (IllegalArgumentException. (str (count line) " is too many: " (seq line)))))
   line))

(defn parse-date [s]
  (t/adjust
   (cond
     (re-matches #"\d+/\d+/\d{4}" s) (t/local-date "M/d/yyyy" s)
     (re-matches #"\d+/\d+/\d{2}" s) (t/local-date "M/d/yy" s)
     (re-matches #"\d+/\d+/\d{2} \d+:\d+" s) (t/local-date "M/d/yy H:m" s)
     (re-matches #"\d+/\d+/\d{4} \d+:\d+" s) (t/local-date "M/d/yyyy H:m" s)
     (re-matches #"\d+-\d+-\d+T\d+:\d+:\d+" s) (t/local-date "y-M-d'T'H:m:s" s)
     (re-matches #"\d+-\d+-\d+ \d+:\d+:\d+" s) (t/local-date "y-M-d H:m:s" s)
     (re-matches #"\d+-\d+-\d+ \d+:\d+" s) (t/local-date "y-M-d H:m" s)
     :else (throw (IllegalArgumentException. (str "Bad date: " s))))
   t/minus (t/days 1)))

(defn fix-date [m] (update-in m [:date] parse-date))

(defn- parse-int [i] (when-not (str/blank? i) (int (Double/parseDouble i))))

(def location-grouping (juxt :country :state :county))

(def table-keys (juxt :country :state :county :date))

(defn fix-numbers [m]
  (-> m
      (update-in [:cases] parse-int)
      (update-in [:deaths] parse-int)
      (update-in [:recoveries] parse-int)))

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

(defn amend-changes
  "iterate the whole list and add change fields for each day based on previous"
  [col]
  (->> col
       (sort-by table-keys)
       (group-by location-grouping)
       (reduce-kv
        (fn [m k v]
          (assoc m k (reduce calc-changes [] v))) {})
       vals
       flatten))

(defn latest-daily [col]
  (->> col
       (sort-by table-keys)
       (group-by table-keys)
       (reduce-kv
        (fn [m k v]
          (assoc m k (last v))) {})
       vals
       flatten))

(defn unify-countries
  "unify historic names of countries. if nothing matches, then pass through."
  [m]
  (update-in
   m
   [:country]
   #(get {"UK" "United Kingdom"
          "Mainland China" "China"
          "Taiwan" "Taiwan*"
          "South Korea" "Korea, South"} % %))) ; pass-through if not found

(defn overlap-location? [r]
  (= ((juxt :country :state :county) r) ["US" "New York" "New York City"]))

(defn trim-all-fields
  [m]
  (map str/trim m))

(defn get-data-from-files [input-dir]
  (->>
   input-dir
   read-csv
   (remove overlap-location?)
   (pmap (comp unify-countries fix-numbers fix-date cols->maps trim-all-fields))
   latest-daily
   amend-changes))

(defn file->doc [file]
  (let [places (->>
                 file
                 io/reader
                 csv/read-csv
                 rest
                 (remove overlap-location?)
                 (pmap
                   (comp
                     unify-countries
                     fix-numbers
                     fix-date
                     cols->maps
                     trim-all-fields)))
        checksum (digest/md5 file)]
    {:file-name (str file)
     :checksum checksum
     :places places}))

(defn file->checksum [file]
    [(str file) (digest/md5 file)])

(comment

  (file->doc (io/file "input" "01-01-2022.csv"))


  nil)
