-- :name drop-covid-day!
-- :command :execute
-- :result :raw
drop table covid_day if exists

-- :name create-covid-day!
-- :command :execute
-- :result :raw
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
  primary key (date, country, state, county)
)

-- :name drop-dim-location!
-- :command :execute
-- :result :raw
drop table dim_location if exists

-- :name create-dim-location!
-- :command :execute
-- :result :raw
create table dim_location (
  location_key uuid primary key,
  country varchar,
  state varchar,
  county varchar,
  unique (country, state, county))

-- :name drop-dim-date!
-- :command :execute
-- :result :raw
drop table dim_date if exists

-- :name create-dim-date!
-- :command :execute
-- :result :raw
create table dim_date (
  date_key uuid primary key,
  date date,
  year int,
  month int,
  day_of_month int,
  day_of_week varchar,
  unique (date))

-- :name drop-fact-day!
-- :command :execute
-- :result :raw
drop table fact_day if exists

-- :name create-fact-day!
-- :command :execute
-- :result :raw
create table fact_day (
  date_key uuid
  , location_key uuid
  , case_change int
  , death_change int
  , recovery_change int
  , unique (date_key, location_key))


-- :name dim-dates
-- :command :query
-- :result :many
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
  date

-- :name dim-locations
-- :command :query
-- :result :many
select
  location_key
  , country
  , state
  , county
from
  dim_location

-- :name distinct-staged-locations
-- :command :query
-- :result :many
select distinct
  country
  , state
  , county
from
  covid_day

-- :name distinct-staged-dates
-- :command :query
-- :result :many
select distinct
  date
from
  covid_day

-- :name staged-data
-- :command :query
-- :result :many
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
  date
