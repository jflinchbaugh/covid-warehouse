(ns covid-warehouse.db-warehouse
  (:require [java-time :as t]
            [clojure.string :as str]
            [next.jdbc :as jdbc]
            [next.jdbc.sql :as sql]
            [covid-warehouse.reader :refer :all]))

(def ds (jdbc/get-datasource {:dbtype "h2:mem" :dbname "covid"}))

(defn drop-table! [ds]
  (jdbc/execute! ds ["drop table covid_day if exists"]))

(defn create-table! [ds]
  (drop-table! ds)
  (jdbc/execute! ds ["
create table covid_day (
  date date,
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

(def stage-map
  {:date :date
   :country :country
   :state :state
   :county :county
   :cases :case_total
   :cases-change :case_change
   :deaths :death_total
   :deaths-change :death_change
   :recoveries :recovery_total
   :recoveries-change :recovery_change})

(defn rec->stage
  "turn a record map into sql map"
  [r]
  (into {} (map (fn [[k v]] [(stage-map k) v]) r)))

(defn insert-day! [ds r]
  (sql/insert! ds :covid_day (rec->stage r)))

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
  (sql/insert! ds
    :dim_location
    {:location_key (uuid)
     :country country
     :state state
     :county county}))

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
  date date,
  year int,
  month int,
  day_of_month int,
  day_of_week varchar,
  unique (date))"]))

(defn insert-dim-date! [ds [date]]
  (let [[year month day-of-month dow]
        (t/as
         (t/local-date-time date)
         :year
         :month-of-year
         :day-of-month
         :day-of-week)
        day-of-week
        (str/capitalize (.name (t/day-of-week dow)))]
    (jdbc/execute!
     ds
     ["
insert into dim_date (
  date_key
  , date
  , year
  , month
  , day_of_month
  , day_of_week
) values (?, ?, ?, ?, ?, ?)"
      (uuid)
      date
      year
      month
      day-of-month
      day-of-week])))

(defn dim-dates [ds]
  (->>
   (jdbc/execute!
    ds
    ["
select
  date_key
  , date
  , year
  , month
  , day_of_month
  , day_of_week
from
  dim_date
order by
  date"])
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

(defn staged-data [ds]
  (jdbc/execute! ds ["
select
  date,
  country,
  state,
  county,
  case_change,
  death_change,
  recovery_change
from
  covid_day
order by
  date"]))

(defn load-fact-day! [ds]
  (let [existing (->> ds
                      fact-days
                      set)
        date-lookup (->> (dim-dates ds) (map (partial take 2)) dim->lookup)

        location-lookup (dim->lookup (dim-locations ds))]
    (->>
     ds
     staged-data
     (pmap vals)
     (pmap na-fields)
     (pmap (partial vals->dims date-lookup location-lookup))
     (filter (complement existing))
     (pmap (partial insert-fact-day! ds))
     doall
     count)))
