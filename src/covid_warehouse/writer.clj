(ns covid-warehouse.writer
  (:require [hiccup.core :as h]
            [hiccup.page :as p]
            [hiccup.element :as e]
            [clojure.string :as str]))

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
  (let [c (int (* fit-size (/ count max-count)))]
    (str/join (repeat c ch))))

(defn total-line [days]
  [:tr
   [:td.date "Total"]
   [:td.death-change (reduce + 0 (map :death-change days)) ]
   [:td.death-graph ""]
   [:td.case-change (reduce + 0 (map :case-change days))]
   [:td.case-graph ""]])

(defn report [days]
  (let [title (str/trim (str/join " " ((juxt :country :state :county) (first days))))]
    (str
      (p/html5 {:lang "en"}
        [:head
         [:title title]
         [:link {:rel "stylesheet" :href "style.css"}]]
        [:body
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
                 death-line (partial graph-line "!" 50 max-deaths)
                 ]
             (map (partial day-row case-line death-line) days))]]]))))

(defn file-name [& lst]
  (str/replace (str/trim (str/join " " lst)) #" " "-"))

(defn html-file-name [f]
  (str f ".html"))

(defn index-line [place]
  [:li [:a {:href (html-file-name (apply file-name place))} (str/join " " place)]])

(defn index-file [places]
  (p/html5 {:lang "en"}
    [:head
     [:title "COVID Data"]
     [:link {:rel "stylesheet" :href "style.css"}]]
    [:body
     [:h1 "COVID Data"]
     [:ul
      (doall (map index-line places))]]))
