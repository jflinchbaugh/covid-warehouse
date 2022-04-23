(ns covid-warehouse.storage
  (:require [clojure.java.io :as io]
            [xtdb.api :as xt]
            [covid-warehouse.timer :refer :all]))

(defn start-xtdb! []
  (letfn [(kv-store [dir]
            {:kv-store {:xtdb/module 'xtdb.rocksdb/->kv-store
                        :db-dir (io/file dir)
                        :sync? true}})]
    (xt/start-node
     {:xtdb/tx-log (kv-store "data/tx-log")
      :xtdb/document-store (kv-store "data/doc-store")
      :xtdb/index-store (kv-store "data/index-store")})))

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

(defn get-place [node [country state county]]
  (xt/q (xt/db node) '{:find [(pull p [*])]
                       :where [[p :type :fact]
                               [p :country country]
                               [p :state state]
                               [p :county county]]
                       :in [country state county]}
    country state county))

(defn stop-xtdb! [node]
  (.close node))

(comment

  (get-place xtdb-node ["US" "Pennsylvania" "Lancaster"])

  (xt/sync xtdb-node)

  (timer "thing" (count (get-stage-docs xtdb-node)))

  (xt/submit-tx xtdb-node [[::xt/put {:xt/id "zig" :name "zig"}]])

  (xt/submit-tx xtdb-node [[::xt/evict "zig"]])

  (xt/q (xt/db xtdb-node) '{:find [(count e) (count e)]
                            :where [[e :country "US"]
                                    [e :state "Pennsylvania"]
                                    [e :county "York"]]})

  nil)
