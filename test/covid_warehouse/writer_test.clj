(ns covid-warehouse.writer-test
  (:require [covid-warehouse.writer :refer :all]
            [clojure.test :as t]
            [clojure.string :as str]
            [hiccup.util :as util]))

(t/deftest test-file-name
  (t/testing "file-name"
    (t/are [expected actual] (= expected actual)
      "" (file-name)
      "x" (file-name "x")
      "x-y" (file-name "x" "y")
      "x-y-z" (file-name "x" "y z"))))

(t/deftest test-html-name
  (t/testing "html-name"
    (t/is ".html" (html-file-name ""))
    (t/is "x.html" (html-file-name "x"))))

(t/deftest test-index-line
  (t/testing "index-line"
    (t/is (= [:li
              [:a {:href (util/to-uri ".html")} [""]]]
             (index-line [])))
    (t/is (= [:li
              [:a {:href (util/to-uri "us-thing.html")} ["us thing"]]]
             (index-line ["us" "thing"])))
    (t/is (= [:li
              [:a {:href (util/to-uri "us-thing-other.html")} ["us thing other"]]]
             (index-line ["us" "thing" "other"])))))

(t/deftest test-index-html
  (t/testing "index-html"
    (let [content (index-html [["US"] ["US" "PA"]])]
      (t/is (re-find #".*html.*head.*title.*COVID.*style.css.*" content))
      (t/is (re-find #".*html.*body.*ul.*li.*href" content))
      (t/is (re-find #"US-PA.html.*US PA.*" content)))))

(t/deftest test-index-json
  (t/testing "index-json"
    (let [content (index-json [["US"] ["US" "PA"]])]
      (t/is (re-find #"COVID" content))
      (t/is (re-find #"place.*US.*file.*US.json" content))
      (t/is (re-find #"place.*US PA.*file.*US-PA.json" content)))))

(t/deftest test-total-line
  (t/testing "total-line"
    (t/is (= [:tr
              [:td.date "Total"]
              [:td.death-change 10]
              [:td.death-graph ""]
              [:td.case-change 20]
              [:td.case-graph ""]]
             (total-line [{:death-change 1 :case-change 12}
                          {:death-change 9 :case-change 8}])))))

(t/deftest test-sqr
  (t/testing "sqr"
    (t/is (= 0 (sqr 0)))
    (t/is (= 1 (sqr -1)))
    (t/is (= 1 (sqr 1)))
    (t/is (= 4 (sqr 2)))))

(t/deftest test-mean
  (t/testing "mean"
    (t/is (= 0.0 (mean [])))
    (t/is (= 1.0 (mean [1])))
    (t/is (= 1.5 (mean [1 2])))
    (t/is (= 0.0 (mean [-1 0 1])))))

(t/deftest test-stddev
  (t/testing "stddev"
    (t/is (= 0.0 (stddev [])))
    (t/is (= 0.0 (stddev [1])))
    (t/is (= 0.5 (stddev [1 2])))
    (t/is (= 1.118033988749895 (stddev [1 2 3 4])))))

(t/deftest test-drop-outliers-stddev
  (t/testing "drop-outliers-stddev"
    (t/is (= [] (drop-outliers-stddev 1 [])))
    (t/is (= [1] (drop-outliers-stddev 1 [1])))
    (t/is (= [1 2] (drop-outliers-stddev 1 [1 2])))
    (t/is (= [1 2] (drop-outliers-stddev 1 [-4 1 2 10])))
    (t/is (= [-4 1 2 10] (drop-outliers-stddev 2 [-4 1 2 10])))))

(t/deftest test-report-html
  ;; TODO refactor to test hiccup instead of rendered html
  (t/testing "report with no days"
    (let [html (report-html "" [])]
      (t/is (re-find #"Date.*Deaths.*Deaths.*Cases.*Cases.*Total" html))))
  (t/testing "report with 2 days"
    (let [html (report-html "" [{:date "d1"
                                 :death-change 1
                                 :death-change-history 1
                                 :case-change 12
                                 :case-change-history 12}
                                {:date "d2"
                                 :death-change 9
                                 :death-change-history 9
                                 :case-change 8
                                 :case-change-history 8
                                 }])]
      (t/is (re-find #"Date.*Deaths.*Deaths.*Cases.*Cases.*Total" html))
      (t/is (re-find #"d1.*1.*12" html))
      (t/is (re-find #"d2.*9.*8" html))
      (t/is (re-find #"prepared" html)))))

(t/deftest test-report-json
  ;; TODO refactor to test map instead of rendered json string
  (t/testing "json report with no days"
    (let [json (report-json "" [])]
      (t/is (re-find
              #"title.*visualization.*total-cases.*total-deaths.*prepared.*days"
              json))))
  (t/testing "json report with 2 days"
    (let [json (report-json "" [{:date "d1"
                                 :death-change 1
                                 :death-change-history 1
                                 :case-change 12
                                 :case-change-history 12}
                                {:date "d2"
                                 :death-change 9
                                 :death-change-history 9
                                 :case-change 8
                                 :case-change-history 8
                                 }])]
      (t/is (re-find
              #"title.*visualization.*total-cases.*total-deaths.*prepared.*days"
              json))
      (t/is (re-find
             #"d1.*12.*1"
             json))
      (t/is (re-find
              #"d2.*8.*9"
              json))
      )))

(t/deftest test-day-row
  (t/testing "render hiccup with values"
    (t/is (= [:tr
              [:td.date "d"]
              [:td.death-change 4]
              [:td.death-graph 9]
              [:td.case-change 2]
              [:td.case-graph 2]]
             (day-row
              #(* 2 %)
              #(* 3 %)
              {:case-change-history 1
               :case-change 2
               :death-change-history 3
               :death-change 4
               :date "d"}))))
  (t/testing "render hiccup with negative values"
    (t/is (= [:tr.negative
              [:td.date "d"]
              [:td.death-change -4]
              [:td.death-graph 9]
              [:td.case-change 2]
              [:td.case-graph 2]]
            (day-row
              #(* 2 %)
              #(* 3 %)
              {:case-change-history 1
               :case-change 2
               :death-change-history 3
               :death-change -4
               :date "d"})))))

(t/deftest test-graph-bar
  (t/testing "graph-bar"
    (t/is (= [:meter {:min 0 :max 2 :high 2 :value 1}]
            (graph-bar 2 1)))))
