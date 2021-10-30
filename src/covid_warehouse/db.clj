(ns covid-warehouse.db
  (:require [clojure.string :as str]
            [covid-warehouse.reader :refer :all]
            [covid-warehouse.timer :refer :all]
            [hugsql.adapter.next-jdbc :as adapter]
            [hugsql.core :as hugsql]
            [java-time :as t]
            [next.jdbc :as jdbc]
            [next.jdbc.sql :as sql]))

;; regular SQL functions
(hugsql/def-db-fns "db/covid-warehouse.sql"
  {:adapter (adapter/hugsql-adapter-next-jdbc)})

;; datasource
(defonce ds (jdbc/get-datasource
          {:jdbcUrl "jdbc:h2:file:./covid;MAX_COMPACT_TIME=120000"}))

(defn create-stage! [ds]
  (drop-covid-day-location-index! ds)
  (drop-covid-day! ds)
  (create-covid-day! ds)
  (create-covid-day-location-index! ds))

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

(defn insert-day!
  "insert the day into the staging database"
  [ds r]
  (sql/insert! ds :covid_day (rec->stage r)))

(def location-grouping (juxt :country :state :county))

(def table-keys (juxt :country :state :county :date))

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

(defn stage-data! [ds input-dir]
  (->>
   input-dir
   read-csv
   (remove overlap-location?)
   (pmap (comp unify-countries fix-numbers fix-date cols->maps trim-all-fields))
   latest-daily
   amend-changes
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
  (let [ldt (t/local-date-time date)
        [year month day-of-month dow]
        (t/as
         ldt
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
      :date (t/format "yyyy-MM-dd" ldt)
      :raw_date date
      :year year
      :month month
      :day_of_month day-of-month
      :day_of_week day-of-week})))

(defn pad-dates [cnt dates]
  (if (empty? dates)
    '()
    (let [earliest (->> dates (sort-by :date) first :date t/local-date-time)
          prev-days (->> (range (* -1 cnt) 0)
                         (map #(t/plus earliest (t/days %)))
                         (map t/sql-date)
                         (map #(-> {:date %})))]
      (concat prev-days dates))))

(defn load-dim-date! [ds]
  (let [existing (->> ds dim-dates (map rest) set)]
    (->>
     (distinct-staged-dates ds)
     (pad-dates 2)
     (map (comp na-fields vals))
     (filter (complement existing))
     (map (partial insert-dim-date! ds))
     doall
     count)))

(defn create-dims! [ds]
  (drop-dim-location-index! ds)
  (drop-dim-location! ds)
  (create-dim-location! ds)
  (create-dim-location-index! ds)
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
                      (map vals)
                      set)
        date-lookup (->>
                     ds
                     dim-dates
                     (map (juxt :date_key :raw_date))
                     dim->lookup)
        location-lookup (->>
                         ds
                         dim-locations
                         (map (juxt :location_key :country :state :county))
                         dim->lookup)]
    (->>
     ds
     staged-data
     (map (comp (partial vals->dims date-lookup location-lookup) na-fields vals))
     (filter (complement existing))
     (map (partial insert-fact-day! ds))
     doall
     count)))

(defn kebab [s] (str/replace s #"_" "-"))

(defn shorten-keys
  [m]
  (reduce-kv
   (fn [m k v] (assoc m ((comp keyword kebab str/lower-case name) k) v))
   {}
   m))

(defn diff-queries
  "diff results of 2 query functions"
  [q1 q2 params tf]
  (let [s (map tf (q1 ds params))
        r (map tf (q2 ds params))]
    (filter (fn [[sc rc]] (not= sc rc)) (partition 2 (interleave s r)))))
