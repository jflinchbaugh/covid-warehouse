(ns covid-warehouse.reader-test
  (:require [covid-warehouse.reader :refer :all]
            [clojure.test :refer :all]
            [tick.core :as tc]
            [clojure.java.io :as io]))

(deftest test-get-csv-files
  (is (= ["03-01-2020.csv" "03-01-2023.csv"] (get-csv-files "test/files"))))

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
    (is (thrown? IllegalArgumentException (parse-date "12")))))

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

(deftest test-file->doc
  (testing "old file"
    (is
     (= {:file-name "test/files/03-01-2020.csv",
         :checksum "79529c876da6d5a109edbd5e19e7579b",
         :places
         '({:state "",
           :country "Italy",
           :date "2020-02-29",
           :cases 1694,
           :deaths 34,
           :recoveries 83}
          {:state "Yunnan",
           :country "China",
           :date "2020-02-29",
           :cases 174,
           :deaths 2,
           :recoveries 163})}
        (file->doc (io/file "test/files/03-01-2020.csv")))))

  (testing "new file"
    (is
     (= {:file-name "test/files/03-01-2023.csv",
         :checksum "9fd5127064276e79424878f674ed6132",
         :places
         '({:county "",
            :state "Western Australia",
            :country "Australia",
            :date "2023-03-01",
            :cases 1291077,
            :deaths 944,
            :recoveries nil}
           {:county "",
            :state "",
            :country "Austria",
            :date "2023-03-01",
            :cases 5919616,
            :deaths 21891,
            :recoveries nil}
           {:county "Lancaster",
            :state "Pennsylvania",
            :country "US",
            :date "2023-03-01",
            :cases 150606,
            :deaths 2088,
            :recoveries nil})}
        (file->doc (io/file "test/files/03-01-2023.csv"))))))
