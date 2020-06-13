(ns covid-warehouse.reader
  (:require [clojure.data.csv :as csv]
            [java-time :as t]
            [clojure.java.io :as io]
            [clojure.string :as str]))

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

(defn read-csv [path]
  (->> path
       io/file
       .list
       (filter (partial re-matches #".*\.csv"))
       (pmap
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
  (cond
    (re-matches #"\d+/\d+/\d{4}" s) (t/local-date "M/d/yyyy" s)
    (re-matches #"\d+/\d+/\d{2}" s) (t/local-date "M/d/yy" s)
    (re-matches #"\d+/\d+/\d{2} \d+:\d+" s) (t/local-date "M/d/yy H:m" s)
    (re-matches #"\d+/\d+/\d{4} \d+:\d+" s) (t/local-date "M/d/yyyy H:m" s)
    (re-matches #"\d+-\d+-\d+T\d+:\d+:\d+" s) (t/local-date "y-M-d'T'H:m:s" s)
    (re-matches #"\d+-\d+-\d+ \d+:\d+:\d+" s) (t/local-date "y-M-d H:m:s" s)
    :else (throw (IllegalArgumentException. (str "Bad date: " s)))))

(defn fix-date [m] (update-in m [:date] parse-date))

(defn parse-int [i] (when-not (str/blank? i) (Integer/parseInt i)))

(defn fix-numbers [m]
  (-> m
      (update-in [:cases] parse-int)
      (update-in [:deaths] parse-int)
      (update-in [:recoveries] parse-int)))

(defn view
  ([f col]
   (let [tp (f col)]
     (doall (map println tp)) col))
  ([col]
   (view (partial take 50) col)))
