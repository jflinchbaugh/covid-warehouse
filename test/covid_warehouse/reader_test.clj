(ns covid-warehouse.reader-test
  (:require [covid-warehouse.reader :as sut]
            [clojure.test :refer :all]))

(deftest test-read-6
  (testing "read-6"
    (is (= {:county ""
            :state "PA"
            :country "US"
            :date "d"
            :cases "1"
            :deaths "2"
            :recoveries "0"}
          (sut/read-6 ["PA" "US" "d" "1" "2" "0"])))))

(deftest test-read-8
  (testing "read-8"
    (is (= {:county ""
            :state "PA"
            :country "US"
            :date "d"
            :cases "1"
            :deaths "2"
            :recoveries "0"}
          (sut/read-8 ["PA" "US" "d" "1" "2" "0" :ignored :ignored])))))

(deftest test-read-12
  (testing "read-12"
    (is (= {:county "Lanc"
            :state "PA"
            :country "US"
            :date "d"
            :cases "1"
            :deaths "2"
            :recoveries "0"}
          (sut/read-12 [:ignored
                       "Lanc"
                       "PA"
                       "US"
                       "d"
                       :ignored
                       :ignored
                       "1"
                       "2"
                       "0"
                       :ignored
                       :ignored])))))

(deftest test-read-14
  (testing "read-14"
    (is (= {:county "Lanc"
            :state "PA"
            :country "US"
            :date "d"
            :cases "1"
            :deaths "2"
            :recoveries "0"}
          (sut/read-14 [:ignored
                        "Lanc"
                        "PA"
                        "US"
                        "d"
                        :ignored
                        :ignored
                        "1"
                        "2"
                        "0"
                        :ignored
                        :ignored
                        :ignored
                        :ignored])))))

(deftest test-fix-numbers
  (testing "fix-numbers converts all numbers"
    (are [in out] (= out (sut/fix-numbers in))
      {:cases "1" :deaths "2" :recoveries "3"} {:cases 1 :deaths 2 :recoveries 3}
      {:cases "1.0" :deaths "2.0" :recoveries "3.0"} {:cases 1 :deaths 2 :recoveries 3}
      {:cases "1.9" :deaths "2.9" :recoveries "3.9"} {:cases 1 :deaths 2 :recoveries 3}
      )))
