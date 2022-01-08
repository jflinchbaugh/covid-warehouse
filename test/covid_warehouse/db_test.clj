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
  (let [con (jdbc/get-connection {:jdbcUrl "jdbc:h2:mem:covid"})]
    (is
     (create-stage! con))))

(deftest test-create-dims
  (let [con (jdbc/get-connection {:jdbcUrl "jdbc:h2:mem:covid"})]
    (is
     (create-dims! con))))

(deftest test-fact-day
  (let [con (jdbc/get-connection {:jdbcUrl "jdbc:h2:mem:covid"})]
    (is
     (do (drop-fact-day! con)
         (create-fact-day! con)))))

(deftest test-dim-date
  (let [con (jdbc/get-connection {:jdbcUrl "jdbc:h2:mem:covid"})]
    (is
      (do (drop-dim-date! con)
          (create-dim-date! con)))))
