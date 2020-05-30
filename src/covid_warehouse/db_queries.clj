(ns covid-warehouse.db-queries
  (:require 
            [java-time :as t]
            [next.jdbc :as jdbc]
            [covid-warehouse.db-warehouse :refer :all]))

(defn days-ago [days date]
  (-> date (t/adjust t/minus (t/days days))))

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
    (days-ago 14 date)
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
order by s"])
    ))

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
"])
    ))
