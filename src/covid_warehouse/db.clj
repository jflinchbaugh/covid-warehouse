(ns covid-warehouse.db
  (:require [clojure.string :as str]
            [covid-warehouse.reader :refer :all]
            [hugsql.adapter.next-jdbc :as adapter]
            [hugsql.core :as hugsql]
            [java-time :as t]
            [next.jdbc :as jdbc]
            [next.jdbc.sql :as sql]
            [next.jdbc.quoted :refer [ansi]]))

;; regular SQL functions
(hugsql/def-db-fns "db/covid-warehouse.sql"
  {:adapter (adapter/hugsql-adapter-next-jdbc)})

;; datasource
(defonce ds (jdbc/get-datasource
               {:jdbcUrl
                (or
                 (System/getenv "COVID_DB_URL")
                 "jdbc:h2:file:./covid;MAX_COMPACT_TIME=45000")
                :user
                (or
                 (System/getenv "COVID_DB_USER")
                 nil)
                :password
                (or
                 (System/getenv "COVID_DB_PASSWORD")
                 nil)}))

(defn create-stage! [ds]
  (drop-input-file! ds)
  (create-input-file! ds)

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

(defn insert-days!
  "batch insert all the incoming staging records"
  [ds recs]
  (jdbc/execute-batch! ds
    "insert into covid_day (
       \"date\",
       \"country\",
       \"state\",
       \"county\",
       \"case_total\",
       \"case_change\",
       \"death_total\",
       \"death_change\",
       \"recovery_total\",
       \"recovery_change\"
    ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    (map
      (comp
        (juxt
          :date :country :state :county
          :case_total :case_change
          :death_total :death_change
          :recovery_total :recovery_change)
        rec->stage)
      recs)
    {}))

(defn insert-input-file! [ds [file-name checksum]]
  (sql/insert!
    ds
    :input_file
    {:file_name file-name
     :checksum checksum},
    {:column-fn ansi}))

(defn stage-checksums! [ds input-dir]
  (->>
    input-dir
    read-checksums
    (map (partial insert-input-file! ds))
    doall
    count))

(defn stage-data! [insert-fn input-dir]
  (->>
   input-dir
   get-data-from-files
   insert-fn
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
    :county county}
   {:column-fn ansi}))

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
        day-of-week (str/capitalize (.name (t/day-of-week dow)))]
    (sql/insert!
     ds
     :dim_date
     {:date_key (uuid)
      :date (t/format "yyyy-MM-dd" ldt)
      :raw_date date
      :year year
      :month month
      :day_of_month day-of-month
      :day_of_week day-of-week}
     {:column-fn ansi})))

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

(defn create-facts! [con]
  (drop-fact-day! con)
  (create-fact-day! con))

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
    :recovery_change recovery-change}
   {:column-fn ansi}))

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
