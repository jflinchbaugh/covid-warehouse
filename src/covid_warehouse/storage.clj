(ns covid-warehouse.storage
  (:require [clojure.java.io :as io]
            [xtdb.api :as xt]
            [covid-warehouse.timer :refer :all]
            [java-time :as t]))

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

(defonce xtdb-node (start-xtdb!))

(defn add-day-id
  [rec]
  (assoc rec :xt/id (:file-name rec)))

(defn add-place-id
  [rec]
  (assoc rec :xt/id (select-keys rec [:country :state :county])))

(defn tag
  [type rec]
  (assoc rec :type type))

(defn put-stage-day [node record]
  (let [tx (xt/submit-tx
            node
            [[::xt/put
              (->> record (tag :stage) add-day-id)]])]
    (xt/await-tx node tx)
    tx))

(defn get-stage-days [node]
  (xt/q (xt/db node) '{:find [(pull d [*])]
                       :where [[d :type :stage]]}))

(defn put-place [node place]
  (let [tx (xt/submit-tx
            node
            [[::xt/put
              (->> place (tag :fact) add-place-id)]])]
    (xt/await-tx node tx)
    tx))

(defn get-places [node]
  (xt/q (xt/db node) '{:find [p (pull p [*])]
                       :where [[p :type :fact]]}))

(defn aggregate-date
  "given a list of date records all for the same day, sum them"
  [col]
  (let [date (:date (first col))
        sum-cases (reduce + (map :cases-change col))
        sum-deaths (reduce + (map :deaths-change col))
        sum-recoveries (reduce + (map :recoveries-change col))]
    {:date (t/format "yyyy-MM-dd" date)
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
   reverse
   ))

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
   reverse
   ))

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
    reverse
    ))

(comment

  (get-dates-by-country xtdb-node ["United Kingdom"])

  (get-dates-by-state xtdb-node ["US" "Pennsylvania"])

  (get-dates-by-county xtdb-node ["US" "Pennsylvania" "Lancaster"])

  (xt/sync xtdb-node)

  (xt/submit-tx xtdb-node [[::xt/put {:xt/id "zig" :name "zig"}]])

  (xt/submit-tx xtdb-node [[::xt/evict "zig"]])


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