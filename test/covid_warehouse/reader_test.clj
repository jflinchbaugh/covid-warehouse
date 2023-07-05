(ns covid-warehouse.reader-test
  (:require [covid-warehouse.reader :refer :all]
            [clojure.test :refer :all]
            [tick.core :as tc]
            [clojure.java.io :as io]))

(deftest test-get-csv-files
  (is (= ["01-01-2020.csv" "01-02-2020.csv"] (get-csv-files "test/files"))))

(deftest test-parse-date
  (testing "date formats"
    (are [in out] (= (tc/date out) (parse-date in))
      "01/02/2010" "2010-01-01"
      "01/02/10" "2010-01-01"
      "01/02/2010 01:01" "2010-01-01"
      "01/02/10 01:01" "2010-01-01"
      "2010-01-02T01:01:01" "2010-01-01"
      "2010-01-02 01:01:01" "2010-01-01"
      "2010-01-02 01:01" "2010-01-01"))

  (testing "error"
    (is (thrown? IllegalArgumentException (parse-date "12"))
      )))

(deftest test-overlap-location
  (is (overlap-location? {:country "US" :state "New York" :county "New York City"}))
  (is (not (overlap-location? {}))))

(deftest test-unify-country-name
  (are [in out] (= out (unify-country-name in))
    "x" "x"
    "UK" "United Kingdom"
    "Taiwan" "Taiwan*"
    "Mainland China" "China"
    "South Korea" "Korea, South"))

(deftest test-latest-daily
  (is (= [] (latest-daily nil)))
  (is (= [] (latest-daily [])))
  (is (= [{:country "c1"
           :state "s1"
           :county "c1"
           :date "d1"}
          {:country "c2"
           :state "s2"
           :county "c2"
           :date "d2"
           :val 2}]
         (latest-daily
          [{:country "c2"
            :state "s2"
            :county "c2"
            :date "d2"
            :val :dropped}
           {:country "c2"
            :state "s2"
            :county "c2"
            :date "d2"
            :val 2}
           {:country "c1"
            :state "s1"
            :county "c1"
            :date "d1"}]))))
