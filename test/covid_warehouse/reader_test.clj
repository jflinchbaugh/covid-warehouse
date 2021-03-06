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
      {:date "01/02/2010"} {:date (t/local-date "2010-01-01")}
      {:date "01/02/10"} {:date (t/local-date "2010-01-01")}
      {:date "01/02/2010 01:01"} {:date (t/local-date "2010-01-01")}
      {:date "01/02/10 01:01"} {:date (t/local-date "2010-01-01")}
      {:date "2010-01-02T01:01:01"} {:date (t/local-date "2010-01-01")}
      {:date "2010-01-02 01:01:01"} {:date (t/local-date "2010-01-01")}
      {:date "2010-01-02 01:01"} {:date (t/local-date "2010-01-01")}
      ))

  (testing "fix-date error"
    (is (thrown? IllegalArgumentException (sut/fix-date {:date "12"}))
      )))

(deftest test-cols->maps
  (testing "cols->maps"
    (with-redefs [sut/read-6 (constantly :6)
                  sut/read-8 (constantly :8)
                  sut/read-12 (constantly :12)
                  sut/read-14 (constantly :14)]
      (are [in out] (= out (sut/cols->maps in))
        (repeat 6 :f) :6
        (repeat 8 :f) :8
        (repeat 12 :f) :12
        (repeat 14 :f) :14)))
  (testing "cols->maps error"
    (is (thrown? IllegalArgumentException (sut/cols->maps (repeat 15 :f))))))

(deftest test-read-csv
  (testing "read-csv skips first line and merges all files"
    (is (=
          #{["file 2" "line 2"]
            ["file 1" "line 2"]}
          (set (sut/read-csv "test/files"))))))
