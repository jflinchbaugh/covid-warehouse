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
-- :doc drop the dim_location table
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
