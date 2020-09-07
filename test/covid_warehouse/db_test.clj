(ns covid-warehouse.db-test
  (:require [covid-warehouse.db :refer :all]
            [clojure.test :refer :all]
            [java-time :as t]))

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

