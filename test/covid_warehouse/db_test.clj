(ns covid-warehouse.db-test
  (:require [covid-warehouse.db :refer :all]
            [clojure.test :refer :all]
            [java-time :as t]
            [next.jdbc :as jdbc]
            [next.jdbc.sql :as sql]))

(deftest test-pad-dates
  (testing "pad-dates"
    (is (empty? (pad-dates 1 [])))
    (is (=
         [{:date (t/sql-date "2020-01-02")}]
         (pad-dates 0 [{:date (t/sql-date "2020-01-02")}])))
    (is (=
         [{:date (t/sql-date "2020-01-01")} {:date (t/sql-date "2020-01-02")}]
         (pad-dates 1 [{:date (t/sql-date "2020-01-02")}])))
    (is (=
         [{:date (t/sql-date "2020-01-01")}
          {:date (t/sql-date "2020-01-02")}
          {:date (t/sql-date "2020-01-03")}
          {:date (t/sql-date "2020-01-04")}]
         (pad-dates 2 [{:date (t/sql-date "2020-01-03")}
                       {:date (t/sql-date "2020-01-04")}])))
    (is (=
         [{:date (t/sql-date "2020-01-01")}
          {:date (t/sql-date "2020-01-02")}
          {:date (t/sql-date "2020-01-03")}]
         (pad-dates 2 [{:date (t/sql-date "2020-01-03")}])))))

(deftest test-shorten-keys
  (is (= {} (shorten-keys nil)))
  (is (= {:db-field "y"} (shorten-keys {:TABLE/DB_FIELD "y"}))))

(deftest test-dim->lookup
  (is (= {} (dim->lookup nil)))
  (is (= {[:b :c] :a
          [:e :f] :d} (dim->lookup [[:a :b :c] [:d :e :f]]))))

(deftest test-vals->dims
  (is (= [nil nil nil nil nil] (vals->dims {} {} nil)) "nil vals")
  (is (=
        [nil nil :cases :deaths :recoveries]
        (vals->dims
          {}
          {}
          [:date :country :state :county :cases :deaths :recoveries]))
    "values get copied along, but keys are nil when not found")
  (is (=
        [:dk :lk :cases :deaths :recoveries]
        (vals->dims
          {[:date] :dk}
          {[:country :state :county] :lk}
          [:date :country :state :county :cases :deaths :recoveries]))
    "values get copied along, but keys are nil when not found"))

(deftest test-overlap-location
  (is (overlap-location? {:country "US" :state "New York" :county "New York City"}))
  (is (not (overlap-location? {}))))

(deftest test-trim-all-fields
  (is (= [] (trim-all-fields nil)))
  (is (= ["x" "y"] (trim-all-fields [" x " " y "]))))

(deftest test-unify-countries
  (are [in out] (= {:x :y :country out} (unify-countries {:x :y :country in}))
    "x" "x"
    "UK" "United Kingdom"
    "Taiwan" "Taiwan*"
    "Mainland China" "China"
    "South Korea" "Korea, South")
  )

(deftest test-create-stage
  (with-open [con (jdbc/get-connection {:jdbcUrl "jdbc:h2:mem:covid"})]
    (is (create-stage! con))))

(deftest test-create-dims
  (with-open [con (jdbc/get-connection {:jdbcUrl "jdbc:h2:mem:covid"})]
    (is (create-dims! con))))

(deftest test-fact-day
  (with-open [con (jdbc/get-connection {:jdbcUrl "jdbc:h2:mem:covid"})]
    (is
      (do (drop-fact-day! con)
         (create-fact-day! con)))))

(deftest test-dim-date
  (with-open [con (jdbc/get-connection {:jdbcUrl "jdbc:h2:mem:covid"})]
    (let [_ (drop-dim-date! con)
          _ (create-dim-date! con)
          _ (insert-dim-date! con [(t/local-date "2020-01-02")])
          inserted-dates (dim-dates con)
          only-date (first inserted-dates)]
      (is (= 1 (count inserted-dates)) "there's 1 date in the dim")
      (testing "date dim has key and raw value set"
        (are [field] (field only-date)
          :date_key
          :raw_date))
      (testing "date dim in db has parsed values"
        (are [value field] (= value (field only-date))
          2020 :year
          1 :month
          2 :day_of_month
          "Thursday" :day_of_week
          )))))

(deftest test-dim-location
  (with-open [con (jdbc/get-connection {:jdbcUrl "jdbc:h2:mem:covid"})]
    (let [_ (drop-dim-location! con)
          _ (create-dim-location! con)
          _ (insert-dim-location! con ["US" "Pennsylvania" "Lancaster"])
          inserted-locations (dim-locations con)
          only-location (first inserted-locations)]
      (is (= 1 (count inserted-locations)) "there's 1 location in the dim")
      (testing "location dim has key set"
        (are [field] (field only-location)
          :location_key))
      (testing "location dim in db has parsed values"
        (are [value field] (= value (field only-location))
          "US" :country
          "Pennsylvania" :state
          "Lancaster" :county
          )))))

(deftest test-fact-day
  (with-open [con (jdbc/get-connection {:jdbcUrl "jdbc:h2:mem:covid"})]
    (let [_ (drop-fact-day! con)
          _ (create-fact-day! con)
          date-key (uuid)
          location-key (uuid)
          _ (insert-fact-day! con [date-key location-key 1 2 3])
          inserted-facts (fact-days con)
          only-fact (first inserted-facts)]
      (is (= 1 (count inserted-facts)) "there's 1 fact")
      (testing "fact day in db has parsed values"
        (are [value field] (= value (field only-fact))
          date-key :date_key
          location-key :location_key
          1 :case_change
          2 :death_change
          3 :recovery_change
          )))))

(deftest test-covid-day
  (with-open [con (jdbc/get-connection {:jdbcUrl "jdbc:h2:mem:covid"})]
    (let [_ (drop-covid-day! con)
          _ (create-covid-day! con)
          _ (insert-days! con [{:date "2020-01-02"
                                :country "US"
                                :state "Pennsylvania"
                                :county "Lancaster"
                                :cases-change 2
                                :deaths-change 4
                                :recoveries-change 6}])
          inserted-days (staged-data con)
          only-day (first inserted-days)]
      (is (= 1 (count inserted-days)) "there's 1 day staged")
      (testing "staged day in db has parsed values"
        (is (= #inst "2020-01-02T05" (:date only-day)))
        (are [value field] (= value (field only-day))
          "US" :country
          "Pennsylvania" :state
          "Lancaster" :county
          2 :case_change
          4 :death_change
          6 :recovery_change
          )))))
