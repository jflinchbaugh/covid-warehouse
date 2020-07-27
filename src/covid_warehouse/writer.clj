(ns covid-warehouse.writer
  (:require [hiccup.core :as h]
            [hiccup.page :as p]
            [clojure.string :as str]))

(defn day-row [day]
  [:tr
   [:td.date (:date day)]
   [:td.case-change (:case-change day)]
   [:td.death-change (:death-change day)]])

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
            [:th.case-change "Cases"]
            [:th.death-change "Deaths"]]]
          [:tbody
           (map day-row days)]]]))))

(comment

  nil)
