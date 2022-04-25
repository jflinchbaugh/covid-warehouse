(ns covid-warehouse.core-test
  (:require [clojure.test :as t]
            [covid-warehouse.core :refer :all]))

(t/deftest test-roll-history
  (t/testing "empty"
    (t/is (= [] (roll-history 2 []))))
  (t/testing "single record"
    (t/is (=
         [{:case-change 1
           :death-change 2
           :recovery-change 3
           :case-change-history 1
           :death-change-history 2
           :recovery-change-history 3}]
         (roll-history
          2
          [{:case-change 1 :death-change 2 :recovery-change 3}]))))
  (t/testing "two records"
    (t/is (=
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
  (t/testing "two records, 1 roll"
    (t/is (=
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
