(ns covid-warehouse.reader-test
  (:require [covid-warehouse.reader :refer :all]
            [clojure.test :refer :all]
            [tick.core :as tc]
            [clojure.java.io :as io]))

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
