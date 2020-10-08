(ns covid-warehouse.writer-test
  (:require [covid-warehouse.writer :refer :all]
            [clojure.test :as t]
            [clojure.string :as str]
            [hiccup.util :as util]))

(t/deftest test-graph-line
  (t/testing "graph-line"
    (t/are [expected actual] (= expected actual)
      "" (graph-line "!" 5 1 0)
      "!!!!!" (graph-line "!" 5 1 1)
      "!!!!!" (graph-line "!" 5 1 1)
      "!!!" (graph-line "!" 5 2 1)
      "!!" (graph-line "!" 5 3 1)
      "!" (graph-line "!" 5 5 1)
      "!" (graph-line "!" 5 6 1)
      "xx" (graph-line "xx" 5 5 1))))

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
    (t/is (= [:li [:a {:href (util/to-uri ".html")} [""]]] (index-line [])))
    (t/is (= [:li [:a {:href (util/to-uri "us-thing.html")} ["us thing"]]]
            (index-line ["us" "thing"])))
    (t/is (= [:li [:a {:href (util/to-uri "us-thing-other.html")} ["us thing other"]]]
             (index-line ["us" "thing" "other"])))))

(t/deftest test-index-file
  (t/testing "index-file"
    (let [content (index-file [["US"] ["US" "PA"]])]
      (t/is (re-find #".*html.*head.*title.*COVID.*style.css.*" content))
      (t/is (re-find #".*html.*body.*ul.*li.*href" content))
      (t/is (re-find #"US-PA.html.*US PA.*" content)))))

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
