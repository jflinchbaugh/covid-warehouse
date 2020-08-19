(ns covid-warehouse.writer-test
  (:require [covid-warehouse.writer :as sut]
            [clojure.test :as t]
            [clojure.string :as str]))

(t/deftest test-graph-line
  (t/testing "graph-line"
    (t/are [expected actual] (= expected actual)
      "" (sut/graph-line "!" 5 1 0)
      "!!!!!" (sut/graph-line "!" 5 1 1)
      "!!!!!" (sut/graph-line "!" 5 1 1)
      "!!" (sut/graph-line "!" 5 2 1)
      "!" (sut/graph-line "!" 5 3 1)
      "!" (sut/graph-line "!" 5 5 1)
      "" (sut/graph-line "!" 5 6 1)
      "xx" (sut/graph-line "xx" 5 5 1))))

(t/deftest test-file-name
  (t/testing "file-name"
    (t/are [expected actual] (= expected actual)
      "" (sut/file-name)
      "x" (sut/file-name "x")
      "x-y" (sut/file-name "x" "y")
      "x-y-z" (sut/file-name "x" "y z"))))

(t/deftest test-html-name
  (t/testing "html-name"
    (t/is ".html" (sut/html-file-name ""))
    (t/is "x.html" (sut/html-file-name "x"))))

(t/deftest test-index-line
  (t/testing "index-line"
    (t/is (= [:li [:a {:href ".html"} ""]] (sut/index-line [])))
    (t/is (= [:li [:a {:href "us-thing.html"} "us thing"]] (sut/index-line ["us" "thing"])))
    (t/is (= [:li [:a {:href "us-thing-other.html"} "us thing other"]]
             (sut/index-line ["us" "thing" "other"])))))

(t/deftest test-index-file
  (t/testing "index-file"
    (let [content (sut/index-file [["US"] ["US" "PA"]])]
      (t/is (re-find #".*html.*head.*title.*COVID.*style.css.*" content))
      (t/is (re-find #".*html.*body.*ul.*li.*href" content))
      (t/is (re-find #"US-PA.html.*US PA.*" content)))))

(t/deftest test-total-line
  (t/testing "total-line"
    (t/is (= [:tr
              [:td.date "Total"]
              [:td.death-change 10]
              [:td.case-change 20]
              [:td.death-graph ""]
              [:td.case-graph ""]]
             (sut/total-line [{:death-change 1 :case-change 12}
                              {:death-change 9 :case-change 8}])))))
