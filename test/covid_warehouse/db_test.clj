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
