(ns covid-warehouse.core-test
  (:require [clojure.test :refer :all]
            [covid-warehouse.core :refer :all]))

(deftest a-test
  (testing "FIXME, I fail."
    (is (= 0 0))))

(deftest test-roll-history
  (testing "empty"
    (is (= [] (roll-history 2 []))))
  (testing "single record"
    (is (=
         [{:case-change 1
           :death-change 2
           :recovery-change 3
           :case-change-history 1
           :death-change-history 2
           :recovery-change-history 3}]
         (roll-history
          2
          [{:case-change 1 :death-change 2 :recovery-change 3}]))))
  (testing "two records"
    (is (=
         [{:case-change 1
           :death-change 2
           :recovery-change 3
           :case-change-history 2
           :death-change-history 3
           :recovery-change-history 4}
          {:case-change 3
           :death-change 4
           :recovery-change 5
           :case-change-history 3
           :death-change-history 4
           :recovery-change-history 5}]
         (roll-history
          2
          [{:case-change 1 :death-change 2 :recovery-change 3}
           {:case-change 3 :death-change 4 :recovery-change 5}]))))
  (testing "two records, 1 roll"
    (is (=
         [{:case-change 1
           :death-change 2
           :recovery-change 3
           :case-change-history 1
           :death-change-history 2
           :recovery-change-history 3}
          {:case-change 3
           :death-change 4
           :recovery-change 5
           :case-change-history 3
           :death-change-history 4
           :recovery-change-history 5}]
         (roll-history
          1
          [{:case-change 1 :death-change 2 :recovery-change 3}
           {:case-change 3 :death-change 4 :recovery-change 5}])))))
