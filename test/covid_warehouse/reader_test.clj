(ns covid-warehouse.reader-test
  (:require [covid-warehouse.reader :refer :all]
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
          (read-6 ["PA" "US" "d" "1" "2" "0"])))))

(deftest test-read-8
  (testing "read-8"
    (is (= {:county ""
            :state "PA"
            :country "US"
            :date "d"
            :cases "1"
            :deaths "2"
            :recoveries "0"}
          (read-8 ["PA" "US" "d" "1" "2" "0" :ignored :ignored])))))

(deftest test-read-12
  (testing "read-12"
    (is (= {:county "Lanc"
            :state "PA"
            :country "US"
            :date "d"
            :cases "1"
            :deaths "2"
            :recoveries "0"}
          (read-12 [:ignored
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
          (read-14 [:ignored
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
    (are [in out] (= out (fix-numbers in))
      {:cases "" :deaths "" :recoveries ""} {:cases nil :deaths nil :recoveries nil}
      {:cases "1" :deaths "2" :recoveries "3"} {:cases 1 :deaths 2 :recoveries 3}
      {:cases "1.0" :deaths "2.0" :recoveries "3.0"} {:cases 1 :deaths 2 :recoveries 3}
      {:cases "1.9" :deaths "2.9" :recoveries "3.9"} {:cases 1 :deaths 2 :recoveries 3}
      )))

(deftest test-fix-date
  (testing "fix-date"
    (are [in out] (= out (fix-date in))
      {:date "01/02/2010"} {:date (t/local-date "2010-01-01")}
      {:date "01/02/10"} {:date (t/local-date "2010-01-01")}
      {:date "01/02/2010 01:01"} {:date (t/local-date "2010-01-01")}
      {:date "01/02/10 01:01"} {:date (t/local-date "2010-01-01")}
      {:date "2010-01-02T01:01:01"} {:date (t/local-date "2010-01-01")}
      {:date "2010-01-02 01:01:01"} {:date (t/local-date "2010-01-01")}
      {:date "2010-01-02 01:01"} {:date (t/local-date "2010-01-01")}
      ))

  (testing "fix-date error"
    (is (thrown? IllegalArgumentException (fix-date {:date "12"}))
      )))

(deftest test-cols->maps
  (testing "cols->maps"
    (with-redefs [read-6 (constantly :6)
                  read-8 (constantly :8)
                  read-12 (constantly :12)
                  read-14 (constantly :14)]
      (are [in out] (= out (cols->maps in))
        (repeat 6 :f) :6
        (repeat 8 :f) :8
        (repeat 12 :f) :12
        (repeat 14 :f) :14)))
  (testing "cols->maps error"
    (is (thrown? IllegalArgumentException (cols->maps (repeat 15 :f))))))

(deftest test-read-csv
  (testing "read-csv skips first line and merges all files"
    (is (=
          #{["file 2" "line 2"]
            ["file 1" "line 2"]}
          (set (read-csv "test/files"))))))

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
