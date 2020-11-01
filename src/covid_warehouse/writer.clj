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

(defn graph-line [ch fit-size max-count count]
  (let [log-max (Math/log (inc (if (neg? max-count) 0 max-count)))
        log-count (Math/log (inc (if (neg? count) 0 count)))
        block-size (if (zero? log-max) 0 (/ log-count log-max))
        c (int (* fit-size block-size))]
    (str/join (repeat c ch))))

(defn total-line [days]
  [:tr
   [:td.date "Total"]
   [:td.death-change (reduce + 0 (map :death-change days))]
   [:td.death-graph ""]
   [:td.case-change (reduce + 0 (map :case-change days))]
   [:td.case-graph ""]])

(defn report [days]
  (let [title (str/trim (str/join " " ((juxt :country :state :county) (first days))))]
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
                 (let [max-cases (apply max (map :case-change-history days))
                       max-deaths (apply max (map :death-change-history days))
                       case-line (partial graph-line "!" 75 max-cases)
                       death-line (partial graph-line "!" 50 max-deaths)]
                   (map (partial day-row case-line death-line) days))]]]))))

(defn report-json [days]
  (let [title (str/trim
               (str/join " " ((juxt :country :state :county) (first days))))]
    (json/generate-string
     {:title title
      :max-cases (apply max (map :case-change-history days))
      :max-deaths (apply max (map :death-change-history days))
      :death-total (reduce + 0 (map :death-change days))
      :case-total (reduce + 0 (map :case-change days))
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
  [:li (e/link-to (html-file-name (apply file-name place)) (str/join " " place))])

(defn index-html [places]
  (p/html5 {:lang "en"}
           [:head
            [:title "COVID Data"]
            (p/include-css "style.css")]
           [:body
            [:h1 "COVID Data"]
            [:ul
             (doall (map index-line places))]]))

(defn index-json [places]
  (json/generate-string
   {:places (map
             (fn [place]
               {:place place
                :file-name (json-file-name (apply file-name place))})
             places)}))
