(ns covid-warehouse.writer
  (:require [hiccup.core :as h]
            [hiccup.page :as p]
            [hiccup.element :as e]
            [clojure.string :as str]
            [cheshire.core :as json]))

(defn day-row [case-line death-line day]
  (let [case-change-history (:case-change-history day)
        death-change-history (:death-change-history day)
        cases (:case-change day)
        deaths (:death-change day)
        any-neg? (some neg? [cases deaths])]
    [(if any-neg? :tr.negative :tr)
     [:td.date (:date day)]
     [:td.death-change deaths]
     [:td.death-graph (death-line death-change-history)]
     [:td.case-change cases]
     [:td.case-graph (case-line case-change-history)]]))

(defn log-scale [count] (Math/log (inc (if (neg? count) 0 count))))

(defn linear-scale [count] (max count 0))

(defn mean [coll]
  (let [size (count coll)]
    (if (zero? size) 0.0
        (double (/ (reduce + coll) size)))))

(defn sqr [n] (* n n))

(defn stddev [coll]
  (let [avg (mean coll)
        size (count coll)]
    (if (zero? size) 0.0
        (Math/sqrt (/ (reduce + (map #(sqr (- % avg)) coll)) size)))))

(defn graph-line [ch scale fit-size max-count count]
  (let [graph-max (scale max-count)
        graph-count (scale count)
        block-size (if (zero? graph-max) 0 (/ graph-count graph-max))
        raw-size (int (* fit-size block-size))
        trunc-size (min fit-size raw-size)
        graph-tail (if (> raw-size fit-size) ">" "")]
    (str/join (conj (vec (repeat trunc-size ch)) graph-tail))))

(defn total-line [days]
  [:tr
   [:td.date "Total"]
   [:td.death-change (reduce + 0 (map :death-change days))]
   [:td.death-graph ""]
   [:td.case-change (reduce + 0 (map :case-change days))]
   [:td.case-graph ""]])

(defn drop-greatest
  [coll]
  (->> coll sort reverse rest))

(defn drop-outliers-stddev [threshold coll]
  (let [sdev (stddev coll)
        avg (mean coll)
        max-diff (* threshold sdev)
        f-dist #(Math/abs (- % avg))
        f-keep #(<= (f-dist %) max-diff)]
    (filter f-keep coll)))

(defn report [days]
  (let [title (str/trim (str/join " " ((juxt :country :state :county) (first days))))
        scale linear-scale
        drop-outliers (partial drop-outliers-stddev 4)]
    (str
     (p/html5 {:lang "en"}
              [:head
               [:title title]
               (p/include-css "style.css")]
              [:body
               (e/link-to "index.html" "<< Back")
               [:h1 title]
               [:table
                [:thead
                 [:tr
                  [:th.date "Date"]
                  [:th.death-change "Deaths"]
                  [:th.death-graph "Deaths"]
                  [:th.case-change "Cases"]
                  [:th.case-graph "Cases"]]]
                [:tbody
                 (total-line days)
                 (let [max-cases (apply max (drop-outliers (map :case-change-history days)))
                       max-deaths (apply max (drop-outliers (map :death-change-history days)))
                       case-line (partial graph-line "!" scale 75 max-cases)
                       death-line (partial graph-line "!" scale 50 max-deaths)]
                   (map (partial day-row case-line death-line) days))]]
               [:div.prepared (java.util.Date.)]]))))

(defn report-json [days]
  (let [title (str/trim
               (str/join " " ((juxt :country :state :county) (first days))))]
    (json/generate-string
     {:title title
      :max-cases (apply max (map :case-change-history days))
      :max-deaths (apply max (map :death-change-history days))
      :total-cases (reduce + 0 (map :case-change days))
      :total-deaths (reduce + 0 (map :death-change days))
      :prepared (java.util.Date.)
      :days (map
             #(select-keys % [:date :case-change :death-change])
             days)})))

(defn file-name [& lst]
  (str/replace (str/trim (str/join " " lst)) #" " "-"))

(defn html-file-name [f]
  (str f ".html"))

(defn json-file-name [f]
  (str f ".json"))

(defn index-line [place]
  (let [html-url (html-file-name (apply file-name place))
        json-url (json-file-name (apply file-name place))]
    [:li
     (e/link-to html-url (str/join " " place))
     " "
     (e/link-to json-url "(json)")]))

(defn index-html [places]
  (p/html5 {:lang "en"}
           [:head
            [:title "COVID Data"]
            (p/include-css "style.css")]
           [:body
            [:h1 "COVID Data"]
            [:div (e/link-to "index.json" "(json)")]
            [:ul (map index-line places)]
            [:div.prepared (java.util.Date.)]]))

(defn index-json [places]
  (json/generate-string
   {:title "COVID Data"
    :prepared (java.util.Date.)
    :places (map
             (fn [place]
               {:place (str/trim (str/join " " place))
                :file-name (json-file-name (apply file-name place))})
             places)}))
