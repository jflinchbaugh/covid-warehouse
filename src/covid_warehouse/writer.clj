(ns covid-warehouse.writer
  (:require [hiccup.core :as h]
            [hiccup.page :as p]
            [hiccup.element :as e]
            [clojure.string :as str]
            [cheshire.core :as json]))

(def outlier-threshold 4)

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

(defn upper-outlier-threshold [threshold coll]
  (+ (mean coll) (* threshold (stddev coll))))

(defn lower-outlier-threshold [threshold coll]
  (- (mean coll) (* threshold (stddev coll))))

(defn graph-bar [max-count count]
    [:meter {:min 0 :max max-count :high max-count :value count}])

(defn total-line [days]
  [:tr
   [:td.date "Total"]
   [:td.death-change (reduce + 0 (map :death-change days))]
   [:td.death-graph ""]
   [:td.case-change (reduce + 0 (map :case-change days))]
   [:td.case-graph ""]])

(defn drop-outliers-stddev [threshold coll]
  (let [upper-threshold (upper-outlier-threshold threshold coll)
        lower-threshold (lower-outlier-threshold threshold coll)
        f-keep #(<= lower-threshold % upper-threshold)]
    (filter f-keep coll)))

(defn report [days]
  (let [title (str/trim (str/join " " ((juxt :country :state :county) (first days))))
        drop-outliers (partial drop-outliers-stddev outlier-threshold)]
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
                 (let [cases (drop-outliers (map :case-change-history days))
                       deaths (drop-outliers (map :death-change-history days))
                       max-cases (if (empty? cases) 0 (apply max cases))
                       max-deaths (if (empty? deaths) 0 (apply max deaths))
                       case-line (partial graph-bar max-cases)
                       death-line (partial graph-bar max-deaths)]
                   (map (partial day-row case-line death-line) days))]]
               [:div.prepared (java.util.Date.)]]))))

(defn report-json [days]
  (let [title (str/trim
               (str/join " " ((juxt :country :state :county) (first days))))
        case-changes (map :case-change-history days)
        death-changes (map :death-change-history days)]
    (json/generate-string
     {:title title
      :visualization {:cases {:max (if (empty? case-changes)
                                     0
                                     (apply max case-changes))
                              :average (mean case-changes)
                              :stddev (stddev case-changes)
                              :upper-outlier-threshold (upper-outlier-threshold
                                                        outlier-threshold
                                                        case-changes)
                              :lower-outlier-threshold (lower-outlier-threshold
                                                        outlier-threshold
                                                        case-changes)}
                      :deaths {:max (if (empty? death-changes)
                                      0
                                      (apply max death-changes))
                               :average (mean death-changes)
                               :stddev (stddev death-changes)
                               :upper-outlier-threshold (upper-outlier-threshold
                                                         outlier-threshold
                                                         death-changes)
                               :lower-outlier-threshold (lower-outlier-threshold
                                                         outlier-threshold
                                                         death-changes)}}

      :total-cases (reduce + 0 (map :case-change days))
      :total-deaths (reduce + 0 (map :death-change days))
      :prepared (java.util.Date.)
      :days (map
             #(select-keys
               % [:date
                  :case-change
                  :case-change-history
                  :death-change
                  :death-change-history])
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
    [:li (e/link-to html-url (str/join " " place))]))

(defn index-html [places]
  (p/html5 {:lang "en"}
           [:head
            [:title "COVID Data"]
            (p/include-css "style.css")]
           [:body
            [:h1 "COVID Data"]
            [:ul (map index-line places)]
            [:div 
             [:span.prepared (java.util.Date.)]
             [:span.changelog
              (e/link-to
                "https://github.com/jflinchbaugh/covid-warehouse/commits/master"
                "Changelog")]]]))

(defn index-json [places]
  (json/generate-string
   {:title "COVID Data"
    :prepared (java.util.Date.)
    :places (map
             (fn [place]
               {:place (str/trim (str/join " " place))
                :file-name (json-file-name (apply file-name place))})
             places)}))
