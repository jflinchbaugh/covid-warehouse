(ns covid-warehouse.writer
  (:require [hiccup.core :as h]
            [hiccup.page :as p]
            [hiccup.element :as e]
            [clojure.string :as str]))

(defn day-row [case-line death-line day]
  (let [cases (:case-change day)
        deaths (:death-change day)]
    [:tr
     [:td.date (:date day)]
     [:td.death-change (:death-change day)]
     [:td.case-change (:case-change day)]
     [:td.death-graph (death-line deaths)]
     [:td.case-graph (case-line cases)]]))

(defn graph-line [ch fit-size max-count count]
  (let [c (int (* fit-size (/ count max-count)))]
    (apply str (repeat c ch))))

(defn report [days]
  (let [title (str/join " " ((juxt :country :state :county) (first days)))]
    (str
      (p/html5 {:lang "en"}
        [:head
         [:title title]]
        [:body
         [:h1 title]
         [:table
          [:thead
           [:tr
            [:th.date "Date"]
            [:th.death-change "Deaths"]
            [:th.case-change "Cases"]
            [:th.death-graph "Deaths"]
            [:th.case-graph "Cases"]]]
          [:tbody
           (let [max-cases (apply max (map :case-change days))
                 max-deaths (apply max (map :death-change days))
                 case-line (partial graph-line "!" 60 max-cases)
                 death-line (partial graph-line "!" 30 max-deaths)
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
     [:title "COVID Data"]]
    [:body
     [:h1 "COVID Data"]
     [:ul
      (doall (map index-line places))]]))

(comment

  (index-line ["US" "Pennsylvania"])

  nil)
