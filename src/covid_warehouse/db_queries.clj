(ns covid-warehouse.db-queries
  (:require
   [java-time :as t]
   [next.jdbc :as jdbc]
   [covid-warehouse.db-warehouse :refer :all]))

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

(defn covid-complete [ds]
  (jdbc/execute!
   ds
   ["
select
  d.date
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
"]))

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

(defn dw-sums-by-county [ds country state county]
  (jdbc/execute!
   ds
   ["
select
  l.country
  , l.state
  , l.county
  , sum(f.case_change)
  , sum(f.death_change)
  , sum(f.recovery_change)
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
