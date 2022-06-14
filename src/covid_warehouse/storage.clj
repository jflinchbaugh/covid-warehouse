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

(defn await-txs [node txs]
  (xt/await-tx node (last txs))
  txs)

(defn stop-xtdb! [node]
  (.close node))

(defn add-day-id
  [rec]
  (assoc rec :xt/id (:file-name rec)))

(defn add-place-id
  [id-name place rec]
  (assoc
   rec
   id-name
   (select-keys place [:country :state :county])))

(defn add-date-id
  [place rec]
  (assoc
   rec
   :xt/id
   (assoc
    (select-keys place [:country :state :county])
    :date (:date rec))))

(defn tag
  [type rec]
  (assoc rec :type type))

(defn put-stage-day [node record]
  (xt/submit-tx
   node
   [[::xt/put
     (->> record (tag :stage) add-day-id)]]))

(defn get-stage-days [node]
  (xt/q (xt/db node) '{:find [(pull d [*])]
                       :where [[d :type :stage]]
                       :timeout 240000}))

(defn make-date [place d]
  (assoc
    (select-keys d [:date :deaths-change :cases-change :recoveries-change])
    :place/country (place :country)
    :place/state (place :state)
    :place/county (place :county)))

(defn make-txs [place]
  (concat
    (for [date (:dates place)]
      [::xt/put (->>
                  date
                  (make-date place)
                  (tag :date)
                  (add-date-id place))])
    ))

(defn put-place [node txs]
  (xt/submit-tx node txs))

(defn get-places [node]
  (xt/q (xt/db node) '{:find [(pull d [:place/country :place/state :place/county])]
                       :where [[d :type :date]]
                       :timeout 240000}))

(defn get-dates [node]
  (xt/q (xt/db node) '{:find [(pull d [*])]
                       :where [[d :type :date]]
                       :timeout 240000}))

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
    '{:find [(pull d [*])]
      :where [
              [d :type :date]
              [d :place/country country]
              [d :place/state state]
              [d :place/county county]]
      :in [country state county]
      :timeout 240000}
    country state county)
   (map first)
   (sort-by :date)
   (partition-by :date)
   (map aggregate-date)
   reverse))

(defn get-dates-by-state [node [country state]]
  (->>
   (xt/q
    (xt/db node)
    '{:find [(pull d [*])]
      :where [[d :type :date]
              [d :place/country country]
              [d :place/state state]]
      :in [country state]
      :timeout 240000}
    country state)
   (map first)
   (sort-by :date)
   (partition-by :date)
   (map aggregate-date)
   reverse))

(defn get-dates-by-country [node [country]]
  (->>
   (xt/q
    (xt/db node)
    '{:find [(pull d [*])]
      :where [[d :type :date]
              [d :place/country country]]
      :in [country]
      :timeout 240000}
    country)
   (map first)
   (sort-by :date)
   (partition-by :date)
   (map aggregate-date)
   reverse))

(defn get-countries [node]
  (->>
   (xt/q
    (xt/db node)
    '{:find [(pull d [:place/country])]
      :where [[d :type :date]]
      :timeout 240000})
   (map first)))

(defn get-states [node country]
  (->>
   (xt/q
    (xt/db node)
    '{:find [(pull d [:place/country :place/state])]
      :where [[d :type :date]
              [d :place/country country]]
      :in [country]
      :timeout 240000}
    country)
   (map first)))

(defn get-counties [node country state]
  (->>
   (xt/q
    (xt/db node)
    '{:find [(pull d [:place/country :place/state :place/county])]
      :where [[d :type :date]
              [d :place/country country]
              [d :place/state state]]
      :in [country state]
      :timeout 240000}
    country state)
   (map first)))

(comment

  (def xtdb-node (start-xtdb!))

  (get-states xtdb-node "US")

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
