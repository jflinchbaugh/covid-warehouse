(ns covid-warehouse.db-warehouse
  (:require [java-time :as t]
            [clojure.string :as str]
            [next.jdbc :as jdbc]
            [covid-warehouse.reader :refer :all]))

(def ds (jdbc/get-datasource {:dbtype "h2:mem" :dbname "covid"}))

(def insert-values
  (juxt :date :country :state :county
        :cases :cases-change
        :deaths :deaths-change
        :recoveries :recoveries-change))

(defn drop-table! [ds]
  (jdbc/execute! ds ["drop table covid_day if exists"]))

(defn create-table! [ds]
  (drop-table! ds)
  (jdbc/execute! ds ["
create table covid_day (
  date timestamp,
  country varchar,
  state varchar,
  county varchar,
  case_total int,
  case_change int,
  death_total int,
  death_change int,
  recovery_total int,
  recovery_change int,
  primary key (date, country, state, county))"]))

(defn insert-day! [ds r]
  (jdbc/execute!
   ds
   (cons "
insert into covid_day (
  date,
  country,
  state,
  county,
  case_total,
  case_change,
  death_total,
  death_change,
  recovery_total,
  recovery_change
) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
         (insert-values r))))

(def location-grouping (juxt :country :state :county))

(def table-keys (juxt :country :state :county :date))

(defn calc-changes [lst new]
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

(defn ammend-changes [col]
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

(defn has-changes? [r]
  (not= [0 0 0] ((juxt :cases-change :deaths-change :recoveries-change) r)))

(defn stage-data! [ds input-dir]
  (create-table! ds)
  (->>
   input-dir
   read-csv
   (pmap #(pmap str/trim %))
   (pmap cols->maps)
   (pmap fix-date)
   (pmap fix-numbers)
   latest-daily
   ammend-changes
   (filter has-changes?)
   (pmap (partial insert-day! ds))
   doall
   count))

(defn uuid []
  (java.util.UUID/randomUUID))

(defn drop-dim-location! [ds]
  (jdbc/execute! ds ["drop table dim_location if exists"]))

(defn create-dim-location! [ds]
  (drop-dim-location! ds)
  (jdbc/execute!
   ds
   ["
create table dim_location (
  location_key uuid primary key,
  country varchar,
  state varchar,
  county varchar,
  unique (country, state, county))"]))

(defn insert-dim-location! [ds [country state county]]
  (jdbc/execute!
   ds
   ["
insert into dim_location (
  location_key,
  country,
  state,
  county
) values (?, ?, ?, ?)"
    (uuid)
    country
    state
    county]))

(defn dim-locations [ds]
  (->>
   (jdbc/execute!
    ds
    ["select location_key, country, state, county from dim_location"])
   (map vals)))

(defn na-fields
  "replace empty strings with N/A"
  [r]
  (pmap (fn [v] (if (and (string? v) (str/blank? v)) "N/A" v)) r))

(defn load-dim-location! [ds]
  (let [existing (->> ds
                      dim-locations
                      (map rest)
                      set)]
    (->>
     (jdbc/execute! ds ["select distinct country, state, county from covid_day"])
     (pmap vals)
     (pmap na-fields)
     (filter (complement existing))
     (pmap (partial insert-dim-location! ds))
     doall
     count)))

(defn drop-dim-date! [ds]
  (jdbc/execute! ds ["drop table dim_date if exists"]))

(defn create-dim-date! [ds]
  (drop-dim-date! ds)
  (jdbc/execute!
   ds
   ["
create table dim_date (
  date_key uuid primary key,
  date timestamp,
  unique (date))"]))

(defn insert-dim-date! [ds [date]]
  (jdbc/execute!
   ds
   ["
insert into dim_date (
  date_key,
  date
) values (?, ?)"
    (uuid)
    date]))

(defn dim-dates [ds]
  (->>
   (jdbc/execute!
    ds
    ["select date_key, date from dim_date"])
   (map vals)))

(defn load-dim-date! [ds]
  (let [existing (->> ds dim-dates (map rest) set)]
    (->>
     (jdbc/execute! ds ["select distinct date from covid_day"])
     (pmap vals)
     (pmap na-fields)
     (filter (complement existing))
     (pmap (partial insert-dim-date! ds))
     doall
     count)))

(defn create-dims! [ds]
  (create-dim-location! ds)
  (create-dim-date! ds))

;; facts

(defn drop-fact-day! [ds]
  (jdbc/execute! ds ["drop table fact_day if exists"]))

(defn create-fact-day! [ds]
  (drop-fact-day! ds)
  (jdbc/execute!
   ds
   ["
create table fact_day (
  date_key uuid
  , location_key uuid
  , case_change int
  , death_change int
  , recovery_change int
  , unique (date_key, location_key))"]))

(defn insert-fact-day!
  [ds
   [date-key location-key case-change death-change recovery-change]]
  (jdbc/execute!
   ds
   ["
insert into fact_day (
  date_key,
  location_key,
  case_change,
  death_change,
  recovery_change
) values (?, ?, ?, ?, ?)"
    date-key
    location-key
    case-change
    death-change
    recovery-change]))

(defn fact-days [ds]
  (->>
   (jdbc/execute!
    ds
    ["
select
  date_key
  , location_key
  , case_change
  , death_change
  , recovery_change
from
  fact_day"])
   (map vals)))

(defn dim->lookup [dim]
  (->> dim
       (reduce (fn [lookup row] (assoc lookup (rest row) (first row))) {})))

(defn vals->dims
  [date-lookup
   location-lookup
   [date country state county case-change death-change recovery-change]]
  [(date-lookup [date])
   (location-lookup [country state county])
   case-change
   death-change
   recovery-change])

(defn load-fact-day! [ds]
  (let [existing (->> ds
                      fact-days
                      set)
        date-lookup (dim->lookup (dim-dates ds))
        location-lookup (dim->lookup (dim-locations ds))]
    (->>
     (jdbc/execute! ds ["
select
  date,
  country,
  state,
  county,
  case_change,
  death_change,
  recovery_change
from covid_day"])
     (pmap vals)
     (pmap na-fields)
     (pmap (partial vals->dims date-lookup location-lookup))
     (filter (complement existing))
     (pmap (partial insert-fact-day! ds))
     doall
     count)))
