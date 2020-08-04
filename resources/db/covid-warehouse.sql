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


-- :name dw-sums-by-country
-- :command :query
-- :result :many
select
  l.country
  , sum(f.case_change) as case_change
  , sum(f.death_change) as death_change
  , sum(f.recovery_change) as recovery_change
from fact_day f
join dim_date d
  on d.date_key = f.date_key
join dim_location l
  on l.location_key = f.location_key
where
  l.country = :country
group by
  l.country

-- :name dw-sums-by-state
-- :command :query
-- :result :many
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
  l.country = :country
  and l.state = :state
group by
  l.country
  , l.state


-- :name dw-sums-by-county
-- :command :query
-- :result :many
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
  l.country = :country
  and l.state = :state
  and l.county = :county
group by
  l.country
  , l.state
  , l.county


-- :name dw-series-by-country
-- :command :query
-- :result :many
select
  d.date
  , d.year
  , d.month
  , d.day_of_month
  , l.country
  , sum(f.case_change) as case_change
  , sum(f.death_change) as death_change
  , sum(f.recovery_change) as recovery_change
from fact_day f
join dim_date d
  on d.date_key = f.date_key
join dim_location l
  on l.location_key = f.location_key
where
  l.country = :country
group by
  d.date
  , d.year
  , d.month
  , d.day_of_month
  , l.country
order by
  d.date desc

-- :name dw-series-by-state
-- :command :query
-- :result :many
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
  l.country = :country
  and l.state = :state
group by
  d.date
  , d.year
  , d.month
  , d.day_of_month
  , l.country
  , l.state
order by
  d.date desc

-- :name dw-series-by-county
-- :command :query
-- :result :many
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
  l.country = :country
  and l.state = :state
  and l.county = :county
order by
  d.date desc
