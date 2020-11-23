(ns covid-warehouse.writer-test
  (:require [covid-warehouse.writer :refer :all]
            [clojure.test :as t]
            [clojure.string :as str]
            [hiccup.util :as util]))

(t/deftest test-graph-line
  (t/testing "graph-line"
    (t/are [expected actual] (= expected actual)
      "" (graph-line "!" log-scale 5 1 0)
      "!!!!!" (graph-line "!" log-scale 5 1 1)
      "!!!!!" (graph-line "!" log-scale 5 1 1)
      "!!!" (graph-line "!" log-scale 5 2 1)
      "!!" (graph-line "!" log-scale 5 3 1)
      "!" (graph-line "!" log-scale 5 5 1)
      "!" (graph-line "!" log-scale 5 6 1)
      "xx" (graph-line "xx" log-scale 5 5 1))))

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
              [:a {:href (util/to-uri ".html")} [""]]
              " "
              [:a {:href (util/to-uri ".json")} ["(json)"]]]
             (index-line [])))
    (t/is (= [:li
              [:a {:href (util/to-uri "us-thing.html")} ["us thing"]]
              " "
              [:a {:href (util/to-uri "us-thing.json")} ["(json)"]]]
             (index-line ["us" "thing"])))
    (t/is (= [:li
              [:a {:href (util/to-uri "us-thing-other.html")} ["us thing other"]]
              " "
              [:a {:href (util/to-uri "us-thing-other.json")} ["(json)"]]]
             (index-line ["us" "thing" "other"])))))

(t/deftest test-index-html
  (t/testing "index-html"
    (let [content (index-html [["US"] ["US" "PA"]])]
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

(t/deftest test-log-scale
  (t/testing "log-scale"
    (t/is (= 0.0 (log-scale 0)))
    (t/is (= 0.0 (log-scale -1)))
    (t/is (= (Math/log 2.0) (log-scale 1)))))

(t/deftest test-linear-scale
  (t/testing "linear-scale"
    (t/is (= 0 (linear-scale 0)))
    (t/is (= 0 (linear-scale -1)))
    (t/is (= 1 (linear-scale 1)))))

(t/deftest test-sqr
  (t/testing "sqr"
    (t/is (= 0 (sqr 0)))
    (t/is (= 1 (sqr -1)))
    (t/is (= 1 (sqr 1)))
    (t/is (= 4 (sqr 2)))))

(t/deftest test-drop-greatest
  (t/testing "drop-greatest"
    (t/is (= [] (drop-greatest [])))
    (t/is (= [] (drop-greatest [1])))
    (t/is (= [1] (drop-greatest [1 1])))
    (t/is (= [1] (drop-greatest [2 1])))))

(t/deftest test-mean
  (t/testing "mean"
    (t/is (= 0.0 (mean [])))
    (t/is (= 1.0 (mean [1])))
    (t/is (= 1.5 (mean [1 2])))
    (t/is (= 0.0 (mean [-1 0 1])))))
