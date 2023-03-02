(ns covid-warehouse.storage
  (:require [clojure.java.io :as io]
            [xtdb.api :as xt]
            [covid-warehouse.timer :refer :all]
            [tick.core :as tc]))

(defn start-xtdb! []
  (letfn [(kv-store [dir]
            {:kv-store {:xtdb/module 'xtdb.rocksdb/->kv-store
                        :db-dir (io/file dir)
                        :sync? true}})]
    (xt/start-node
     {:xtdb/tx-log (kv-store "data/tx-log")
      :xtdb/document-store (kv-store "data/doc-store")
      :xtdb/index-store (kv-store "data/index-store")})))

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

(defn put-stage-day [node record]
  (xt/await-tx
   node
   (xt/submit-tx
    node
    [[::xt/put (->> record (tag :stage) add-day-id)]])))

(defn get-stage-days [node]
  (xt/q (xt/db node) '{:find [(pull d [*])]
                       :where [[d :type :stage]]
                       :timeout 240000}))

(defn get-stage-checksums [node]
  (->>
    (xt/q
      (xt/db node)
      '{:find [(pull d [:file-name :checksum])]
        :where [[d :type :stage]]
        :timeout 240000})
    (pmap (comp (juxt :file-name :checksum) first))
    set))

(defn put-place [node place]
  (xt/await-tx
   node
   (xt/submit-tx
    node
    [[::xt/put
      (->> place (tag :fact) add-place-id)]])))

(defn get-places [node]
  (xt/q (xt/db node) '{:find [p (pull p [*])]
                       :where [[p :type :fact]
                               [p :current? true]]}))

(defn aggregate-date
  "given a list of date records all for the same day, sum them"
  [col]
  (let [date (:date (first col))
        sum-cases (reduce + (map :cases-change col))
        sum-deaths (reduce + (map :deaths-change col))
        sum-recoveries (reduce + (map :recoveries-change col))]
    {:date (tc/format (tc/formatter "yyyy-MM-dd") date)
     :case-change sum-cases
     :death-change sum-deaths
     :recovery-change sum-recoveries}))

(defn get-dates-by-county [node [country state county]]
  (->>
   (xt/q
    (xt/db node)
    '{:find [(pull p [:dates])]
      :where [[p :type :fact]
              [p :country country]
              [p :state state]
              [p :county county]]
      :in [country state county]}
    country state county)
   (map (comp :dates first))
   (reduce concat)
   (sort-by :date)
   (partition-by :date)
   (map aggregate-date)
   reverse))

(defn get-dates-by-state [node [country state]]
  (->>
   (xt/q
    (xt/db node)
    '{:find [(pull p [:dates])]
      :where [[p :type :fact]
              [p :country country]
              [p :state state]]
      :in [country state]}
    country state)
   (map (comp :dates first))
   (reduce concat)
   (sort-by :date)
   (partition-by :date)
   (map aggregate-date)
   reverse))

(defn get-dates-by-country [node [country]]
  (->>
   (xt/q
    (xt/db node)
    '{:find [(pull p [:dates])]
      :where [[p :type :fact]
              [p :country country]]
      :in [country]}
    country)
   (map (comp :dates first))
   (reduce concat)
   (sort-by :date)
   (partition-by :date)
   (map aggregate-date)
   reverse))

(defn get-countries [node]
  (->>
   (xt/q
    (xt/db node)
    '{:find [(pull d [:country])]
      :where [[d :type :fact]
              [d :current? true]]})
   (map first)
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
   (map first)
   distinct))

(defn get-counties [node country state]
  (->>
   (xt/q
    (xt/db node)
    '{:find [(pull d [:country :state :county])]
      :where [[d :type :fact]
              [d :country country]
              [d :state state]
              [d :current? true]]
      :in [country state]}
    country state)
   (map first)
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
     :where [[e :country country]
             [e :state state]
             [e :county county]
             [e :dates ds]
             [ds :date _]]
     :in [country state county]}
   "US" "Pennsylvania" "York")

  nil)
