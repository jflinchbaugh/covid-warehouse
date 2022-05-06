(ns covid-warehouse.storage-test
  (:require [covid-warehouse.storage :refer :all]
            [clojure.test :as t]))

(t/deftest test-add-day-id
  (t/testing "empty gets nil id"
    (t/is (= {:xt/id nil} (add-day-id {}))))
  (t/testing "file-name is used as id"
    (t/is (= {:xt/id "fn" :file-name "fn"} (add-day-id {:file-name "fn"})))))

(t/deftest test-add-place-id
  (t/testing "empty gets empty map as id"
    (t/is (= {:xt/id {}} (add-place-id {}))))
  (t/testing "file-name is used as id"
    (t/is (= {:xt/id {:country "U" :state "S" :county "C"}
              :country "U"
              :state "S"
              :county "C"}
            (add-place-id {:country "U" :state "S" :county "C"})))))

(t/deftest test-tag
  (t/testing "nil"
    (t/is (= {:type nil} (tag nil nil))))
  (t/testing "tag adds type"
    (t/is (= {:type :t
              :a :b} (tag :t {:a :b})))))
