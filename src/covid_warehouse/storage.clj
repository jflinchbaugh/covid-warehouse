(ns covid-warehouse.storage
  (:require [clojure.java.io :as io]
            [xtdb.api :as xt]
            [covid-warehouse.timer :refer :all]
            [tick.core :as tc]))

(def xtdb-server-url "http://localhost:4321/")

(defn start-xtdb!
  []
  (xt/new-api-client xtdb-server-url))

(defn stop-xtdb! [node]
  (.close node))

(defn day-id [rec]
  (:file-name rec))

(defn add-day-id
  [rec]
  (assoc rec :xt/id (day-id rec)))

(defn place-id [rec]
  (select-keys rec [:country :state :county]))

(defn add-place-id
  [rec]
  (assoc rec :xt/id (place-id rec)))

(defn tag
  [type rec]
  (assoc rec :type type))

(defn keys->db [namespace record]
  (update-keys record
               (fn [k]
                 (if (= :xt/id k) k (keyword (str namespace "/" (name k)))))))

(defn keys->mem [record]
  (update-keys record (comp keyword name)))

(defn put-stage-day [node record]
  (xt/await-tx
   node
   (xt/submit-tx
    node
    [[::xt/put
      (->>
       record
       (tag :stage)
       add-day-id
       (keys->db "covid-warehouse.stage.day"))]])))

(defn get-stage-days [node]
  (map (comp keys->mem first)
   (xt/q
    (xt/db node)
    '{:find [(pull d [*])]
      :where [[d :covid-warehouse.stage.day/type :stage]]
      :timeout 240000})))

(defn delete-stage-days [node]
  (let [db (xt/db node)
        days (map
               first
               (xt/q db
                          '{:find [d]
                            :where [[d :covid-warehouse.stage.day/type :stage]]}))
        ops (map (fn [d] [::xt/delete d]) days)]
    (xt/await-tx
      node
      (xt/submit-tx node ops))
    )
  )

(defn get-stage-checksums [node]
  (->>
   (xt/q
    (xt/db node)
    '{:find
      [(pull
        d
        [:covid-warehouse.stage.day/file-name
         :covid-warehouse.stage.day/checksum])]
      :where [[d :covid-warehouse.stage.day/type :stage]]
      :timeout 240000})
   (pmap
    (comp
     (juxt
      :covid-warehouse.stage.day/file-name
      :covid-warehouse.stage.day/checksum)
     first))
   set))

(defn put-place [node place]
  (xt/await-tx
   node
   (xt/submit-tx
    node
    [[::xt/put
      (->>
       place
       (tag :fact)
       add-place-id
       (keys->db "covid-warehouse.fact.place"))]])))

(defn get-places [node]
  (map (comp keys->mem first)
   (xt/q
    (xt/db node)
    '{:find [p (pull p [*])]
      :where [[p :covid-warehouse.fact.place/type :fact]
              [p :covid-warehouse.fact.place/current? true]]})))

(defn aggregate-date
  "given a list of date records all for the same day, sum them"
  [col]
  (let [date (:date (first col))
        sum-cases (reduce + (map :cases-change col))
        sum-deaths (reduce + (map :deaths-change col))
        sum-recoveries (reduce + (map :recoveries-change col))]
    {:date date
     :case-change sum-cases
     :death-change sum-deaths
     :recovery-change sum-recoveries}))

(defn get-dates-by-county [node [country state county]]
  (->>
   (xt/q
    (xt/db node)
    '{:find [(pull p [:covid-warehouse.fact.place/dates])]
      :where [[p :covid-warehouse.fact.place/type :fact]
              [p :covid-warehouse.fact.place/country country]
              [p :covid-warehouse.fact.place/state state]
              [p :covid-warehouse.fact.place/county county]]
      :in [country state county]}
    country state county)
   (map (comp :dates keys->mem first))
   (reduce concat)
   (sort-by :date)
   (partition-by :date)
   (map aggregate-date)
   reverse))

(defn get-dates-by-state [node [country state]]
  (->>
   (xt/q
    (xt/db node)
    '{:find [(pull p [:covid-warehouse.fact.place/dates])]
      :where [[p :covid-warehouse.fact.place/type :fact]
              [p :covid-warehouse.fact.place/country country]
              [p :covid-warehouse.fact.place/state state]]
      :in [country state]}
    country state)
   (map (comp :dates keys->mem first))
   (reduce concat)
   (sort-by :date)
   (partition-by :date)
   (map aggregate-date)
   reverse))

(defn get-dates-by-country [node [country]]
  (->>
   (xt/q
    (xt/db node)
    '{:find [(pull p [:covid-warehouse.fact.place/dates])]
      :where [[p :covid-warehouse.fact.place/type :fact]
              [p :covid-warehouse.fact.place/country country]]
      :in [country]}
    country)
   (map (comp :dates keys->mem first))
   (reduce concat)
   (sort-by :date)
   (partition-by :date)
   (map aggregate-date)
   reverse))

(defn get-countries [node]
  (->>
   (xt/q
    (xt/db node)
    '{:find [(pull d [:covid-warehouse.fact.place/country])]
      :where [[d :covid-warehouse.fact.place/type :fact]
              [d :covid-warehouse.fact.place/current? true]]})
   (map (comp keys->mem first))
   distinct))

(defn get-states [node country]
  (->>
   (xt/q
    (xt/db node)
    '{:find [(pull d [:country :state])]
      :where [[d :type :fact]
              [d :country country]
              [d :current? true]]
      :in [country]}
    country)
   (map (comp keys->mem first))
   distinct))

(defn get-counties [node country state]
  (->>
   (xt/q
    (xt/db node)
    '{:find [(pull d [:covid-warehouse.fact.place/country
                      :covid-warehouse.fact.place/state
                      :covid-warehouse.fact.place/county])]
      :where [[d :covid-warehouse.fact.place/type :fact]
              [d :covid-warehouse.fact.place/country country]
              [d :covid-warehouse.fact.place/state state]
              [d :covid-warehouse.fact.place/current? true]]
      :in [country state]}
    country state)
   (map (comp keys->mem first))
   distinct))

(comment

  (with-open [node (start-xtdb!)]
    (get-states node "US"))

  (def xtdb-node (start-xtdb!))

  (get-dates-by-country xtdb-node ["United Kingdom"])

  (get-dates-by-state xtdb-node ["US" "Pennsylvania"])

  (get-dates-by-county xtdb-node ["US" "Pennsylvania" "Lancaster"])

  (xt/sync xtdb-node)

  (xt/q
   (xt/db xtdb-node)
   '{:find [e]
     :where [[e :covid-warehouse.fact.place/country country]
             [e :covid-warehouse.fact.place/state state]
             [e :covid-warehouse.fact.place/county county]
             [e :covid-warehouse.fact.place/dates ds]
             [ds :covid-warehouse.fact.place/date _]]
     :in [country state county]}
   "US" "Pennsylvania" "York")

  nil)
