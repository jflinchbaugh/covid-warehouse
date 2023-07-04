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

(deftest test-amend-changes
  (is (= [] (amend-changes nil)))
  (is (= [] (amend-changes [])))
  (is (=
       [{:x 1 :cases-change 0 :deaths-change 0 :recoveries-change 0}]
       (amend-changes [{:x 1}])))
  (is (=
       [{:x 1 :cases-change 0 :deaths-change 0 :recoveries-change 0}
        {:x 2 :cases-change 0 :deaths-change 0 :recoveries-change 0}]
       (amend-changes [{:x 1} {:x 2}])))
  (is (=
       [{:x 1
         :cases 1
         :cases-change 1
         :deaths 1
         :deaths-change 1
         :recoveries 1
         :recoveries-change 1}
        {:x 2
         :cases 2
         :cases-change 1
         :deaths 3
         :deaths-change 2
         :recoveries 4
         :recoveries-change 3}]
       (amend-changes [{:x 1 :cases 1 :deaths 1 :recoveries 1}
                       {:x 2 :cases 2 :deaths 3 :recoveries 4}]))))
