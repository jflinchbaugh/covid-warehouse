(ns covid-warehouse.reader-test
  (:require [covid-warehouse.reader :as sut]
            [clojure.test :refer :all]
            [java-time :as t]))

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
      {:cases "" :deaths "" :recoveries ""} {:cases nil :deaths nil :recoveries nil}
      {:cases "1" :deaths "2" :recoveries "3"} {:cases 1 :deaths 2 :recoveries 3}
      {:cases "1.0" :deaths "2.0" :recoveries "3.0"} {:cases 1 :deaths 2 :recoveries 3}
      {:cases "1.9" :deaths "2.9" :recoveries "3.9"} {:cases 1 :deaths 2 :recoveries 3}
      )))

(deftest test-fix-date
  (testing "fix-date"
    (are [in out] (= out (sut/fix-date in))
      {:date "01/02/2010"} {:date (t/local-date "2010-01-02")}
      {:date "01/02/10"} {:date (t/local-date "2010-01-02")}
      {:date "01/02/2010 01:01"} {:date (t/local-date "2010-01-02")}
      {:date "01/02/10 01:01"} {:date (t/local-date "2010-01-02")}
      {:date "2010-01-02T01:01:01"} {:date (t/local-date "2010-01-02")}
      {:date "2010-01-02 01:01:01"} {:date (t/local-date "2010-01-02")}
      ))
  (testing "fix-date error"
    (is (thrown? IllegalArgumentException (sut/fix-date {:date "12"}))
      )))
