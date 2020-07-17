(ns covid-warehouse.db
  (:require [java-time :as t]
            [clojure.string :as str]
            [next.jdbc :as jdbc]
            [next.jdbc.sql :as sql]
            [hugsql.core :as hugsql]
            [hugsql.adapter.next-jdbc :as adapter]
            [covid-warehouse.reader :refer :all]))

;; regular SQL functions
(hugsql/def-db-fns "db/covid-warehouse.sql"
  {:adapter (adapter/hugsql-adapter-next-jdbc)})

;; datasource
(def ds (jdbc/get-datasource {:dbtype "h2" :dbname "covid"}))

(defn create-stage! [ds]
  (drop-covid-day! ds)
  (create-covid-day! ds))

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
  (->>
   input-dir
   read-csv
   (pmap (comp fix-numbers fix-date cols->maps #(map str/trim %)))
   latest-daily
   ammend-changes
   (filter has-changes?)
   (map (partial insert-day! ds))
   doall
   count))

(defn uuid []
  (java.util.UUID/randomUUID))

(defn insert-dim-location! [ds [country state county]]
  (sql/insert!
   ds
   :dim_location
   {:location_key (uuid)
    :country country
    :state state
    :county county}))

(defn na-fields
  "replace empty strings with N/A"
  [r]
  (map (fn [v] (if (and (string? v) (str/blank? v)) "N/A" v)) r))

(defn load-dim-location! [ds]
  (let [existing (->> ds
                      dim-locations
                      (map rest)
                      set)]
    (->>
     (distinct-staged-locations ds)
     (map (comp na-fields vals))
     (filter (complement existing))
     (map (partial insert-dim-location! ds))
     doall
     count)))

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
    (sql/insert!
     ds
     :dim_date
     {:date_key (uuid)
      :date date
      :year year
      :month month
      :day_of_month day-of-month
      :day_of_week day-of-week})))

(defn load-dim-date! [ds]
  (let [existing (->> ds dim-dates (map rest) set)]
    (->>
     (distinct-staged-dates ds)
     (map (comp na-fields vals))
     (filter (complement existing))
     (map (partial insert-dim-date! ds))
     doall
     count)))

(defn create-dims! [ds]
  (drop-dim-location! ds)
  (create-dim-location! ds)
  (drop-dim-date! ds)
  (create-dim-date! ds))

;; facts


(defn insert-fact-day!
  [ds
   [date-key location-key case-change death-change recovery-change]]
  (sql/insert!
   ds
   :fact_day
   {:date_key date-key
    :location_key location-key
    :case_change case-change
    :death_change death-change
    :recovery_change recovery-change}))

(defn fact-days [ds]
  (map
   vals
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
  fact_day"])))

(defn dim->lookup [dim]
  (reduce
   (fn [lookup row] (assoc lookup (rest row) (first row)))
   {}
   dim))

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
        date-lookup (->>
                      (dim-dates ds)
                      (map (comp (partial take 2) vals))
                      dim->lookup)

        location-lookup (dim->lookup (map vals (dim-locations ds)))]
    (->>
     ds
     staged-data
     (map (comp (partial vals->dims date-lookup location-lookup) na-fields vals))
     (filter (complement existing))
     (map (partial insert-fact-day! ds))
     doall
     count)))

(defn days-ago [days date]
  (t/adjust date t/minus (t/days days)))

(defn cases-by-window [ds country state date days]
  (jdbc/execute!
   ds
   ["
select county, sum(case_change)
from covid_day
where date >= ?
and date <= ?
and country = ?
and state = ?
group by county
"
    (days-ago days date)
    date
    country
    state]))

(defn series-by-county [ds country state county]
  (jdbc/execute!
   ds
   ["
select
  date,
  case_total, case_change,
  death_total, death_change,
  recovery_total, recovery_change
from covid_day
where country = ?
and state = ?
and county = ?
order by date, country, state, county
"
    country
    state
    county]))

(defn deaths-by-state [ds]
  (->>
   (jdbc/execute!
    ds
    ["
select
  sum(death_change) as s,
  country,
  state
from covid_day
group by
  country,
  state
order by s"])))

(defn deaths-by-country [ds]
  (->>
   (jdbc/execute!
    ds
    ["
select
  sum(death_change) as s
  , country
from covid_day
group by country
order by s
"])))

(defn dw-series-by-county [ds country state county]
  (jdbc/execute!
   ds
   ["
select
  d.date
  , d.year
  , d.month
  , d.day_of_month
  , l.country
  , l.state
  , l.county
  , f.case_change
  , f.death_change
  , f.recovery_change
from fact_day f
join dim_date d
  on d.date_key = f.date_key
join dim_location l
  on l.location_key = f.location_key
where
  l.country = ?
  and l.state = ?
  and l.county = ?
order by
  d.date
"
    country
    state county]))

(defn dw-series-by-state [ds country state]
  (jdbc/execute!
    ds
    ["
select
  d.date
  , d.year
  , d.month
  , d.day_of_month
  , l.country
  , l.state
  , sum(f.case_change) as case_change
  , sum(f.death_change) as death_change
  , sum(f.recovery_change) as recovery_change
from fact_day f
join dim_date d
  on d.date_key = f.date_key
join dim_location l
  on l.location_key = f.location_key
where
  l.country = ?
  and l.state = ?
group by
  d.date
  , d.year
  , d.month
  , d.day_of_month
  , l.country
  , l.state
order by
  d.date
"
     country
     state]))

(defn dw-sums-by-county [ds country state county]
  (jdbc/execute!
   ds
   ["
select
  l.country
  , l.state
  , l.county
  , sum(f.case_change) as case_change
  , sum(f.death_change) as death_change
  , sum(f.recovery_change) as recovery_change
from fact_day f
join dim_date d
  on d.date_key = f.date_key
join dim_location l
  on l.location_key = f.location_key
where
  l.country = ?
  and l.state = ?
  and l.county = ?
group by
  l.country
  , l.state
  , l.county"
    country
    state
    county]))

(defn dw-sums-by-state [ds country state]
  (jdbc/execute!
    ds
    ["
select
  l.country
  , l.state
  , sum(f.case_change) as case_change
  , sum(f.death_change) as death_change
  , sum(f.recovery_change) as recovery_change
from fact_day f
join dim_date d
  on d.date_key = f.date_key
join dim_location l
  on l.location_key = f.location_key
where
  l.country = ?
  and l.state = ?
group by
  l.country
  , l.state"
     country
     state]))
