(ns covid-warehouse.writer
  (:require [hiccup.core :as h]
            [hiccup.page :as p]
            [hiccup.element :as e]
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
