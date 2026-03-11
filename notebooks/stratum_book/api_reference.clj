;; # API Reference
;;
;; A runnable reference for the
;; [stratum.api](https://github.com/replikativ/stratum/blob/main/src/stratum/api.clj)
;; namespace — the single namespace most users need.
;; Each section shows a function signature, a brief description,
;; and a live example with an assertion.

(ns stratum-book.api-reference
  (:require
   [stratum.api :as st]
   [stratum.dataset :as dataset]
   [stratum.storage :as storage]
   [scicloj.kindly.v4.kind :as kind]))

;; ---
;;
;; ## Querying

;; ### `q` — query the analytical engine
;;
;; Accepts a DSL map **or** an SQL string.

;; **DSL map** — Clojure data describing `:from`, `:where`, `:agg`,
;; `:group`, `:select`, `:join`, `:window`, `:order`, `:limit`,
;; `:offset`, `:having`, `:distinct`, and `:result`.

(def data
  {:product (into-array String ["A" "B" "A" "B" "A"])
   :qty (double-array [10 20 30 40 50])
   :price (double-array [1.0 2.0 1.5 2.5 1.0])})

(st/q {:from data
       :where [[:> :qty 15]]
       :group [:product]
       :agg [[:sum :qty] [:avg :price] [:count]]})

(kind/test-last
 [(fn [result]
    (and (= 2 (count result))
         (every? #(contains? % :sum) result)))])

;; **SQL string** — pass a table map as second argument.

(st/q "SELECT product, SUM(qty) AS total FROM d GROUP BY product"
      {"d" data})

(kind/test-last
 [(fn [result]
    (= 2 (count result)))])

;; ### `explain` — show execution plan without running
;;
;; Returns a map describing the strategy Stratum would use.

(st/explain {:from data
             :where [[:> :qty 15]]
             :agg [[:sum :qty]]})

(kind/test-last
 [(fn [plan]
    (contains? plan :strategy))])

;; ---
;;
;; ## Data Import

;; ### `from-maps` — sequence of maps to dataset
;;
;; Type-inferred: integers → `long[]`, floats → `double[]`, else → `String[]`.

(def people
  (st/from-maps [{:name "Alice" :age 30}
                 {:name "Bob" :age 25}
                 {:name "Carol" :age 35}]))

(st/q {:from people :where [[:> :age 28]] :agg [[:count]]})

(kind/test-last
 [(fn [result]
    (= 2 (:count (first result))))])

;; ### `from-csv` — read a CSV file
;;
;; Options: `:separator`, `:header?`, `:types`, `:limit`, `:name`.

;; (st/from-csv "data/orders.csv")
;; (st/from-csv "data/orders.tsv" :separator \tab)

;; ### `from-parquet` — read a [Parquet](https://parquet.apache.org/) file
;;
;; Options: `:columns` (select subset), `:limit`, `:name`.

;; (st/from-parquet "data/orders.parquet")
;; (st/from-parquet "data/orders.parquet" :columns ["qty" "price"] :limit 1000)

;; ---
;;
;; ## Column Encoding

;; ### `encode-column` — pre-encode a raw array
;;
;; `String[]` → dictionary-encoded (stored as `long[]` codes + dictionary),
;; `long[]`/`double[]` → passthrough.

(st/encode-column (into-array String ["x" "y" "x" "z"]))

(kind/test-last
 [(fn [col]
    (= :string (:dict-type col)))])

(st/encode-column (double-array [1.0 2.0 3.0]))

(kind/test-last
 [(fn [col]
    (= :float64 (:type col)))])

;; ### `index-from-seq` — create a persistent column index
;;
;; Produces a
;; [PersistentColumnIndex](https://github.com/replikativ/stratum/blob/main/src/stratum/index.clj)
;; with zone-map metadata per chunk, required for persistence and
;; zone-map pruning.

(st/index-from-seq :int64 [10 20 30 40 50])

(kind/test-last
 [(fn [idx]
    (= 5 (count idx)))])

(st/index-from-seq :float64 [1.1 2.2 3.3])

(kind/test-last
 [(fn [idx]
    (= 3 (count idx)))])

;; ---
;;
;; ## Data Conversion

;; ### `results->columns` — result maps to column arrays

(st/results->columns [{:a 1 :b "x"} {:a 2 :b "y"}])

(kind/test-last
 [(fn [cols]
    (and (contains? cols :a)
         (contains? cols :b)))])

;; ### `tuples->columns` — positional tuples to column map
;;
;; Column names supplied separately; types inferred from values.

(st/tuples->columns [[1 "Alice"] [2 "Bob"]] [:id :name])

(kind/test-last
 [(fn [cols]
    (and (contains? cols :id)
         (contains? cols :name)))])

;; ### `columns->tuples` — column map to positional tuples
;;
;; Takes a column map and a vector of column names specifying order.

(st/columns->tuples {:id (long-array [1 2])
                     :name (into-array String ["Alice" "Bob"])}
                    [:id :name])

(kind/test-last
 [(fn [tuples]
    (= [[1 "Alice"] [2 "Bob"]] tuples))])

;; ---
;;
;; ## Dataset Construction

;; ### `make-dataset` — create a StratumDataset
;;
;; Wraps a column map into an immutable dataset value with
;; metadata, schema introspection, and persistence support.

(def ds
  (st/make-dataset
   {:x (st/index-from-seq :int64 [1 2 3 4 5])
    :y (st/index-from-seq :float64 [1.1 2.2 3.3 4.4 5.5])}
   {:name "example"}))

(st/name ds)

(kind/test-last
 [(fn [n] (= "example" n))])

;; ### `row-count`

(st/row-count ds)

(kind/test-last
 [(fn [n] (= 5 n))])

;; ### `column-names`

(st/column-names ds)

(kind/test-last
 [(fn [names]
    (= (set names) #{:x :y}))])

;; ### `schema` — column types and nullability

(st/schema ds)

(kind/test-last
 [(fn [s]
    (and (contains? s :x)
         (contains? s :y)))])

;; ### `ensure-indexed` — convert array columns to index-backed

(def plain
  (st/make-dataset {:a (long-array [1 2 3])
                    :b (double-array [1.0 2.0 3.0])}))

(def indexed-ds (st/ensure-indexed plain))

(st/q {:from indexed-ds :agg [[:sum :b]]})

(kind/test-last
 [(fn [result]
    (= 6.0 (:sum (first result))))])

;; ---
;;
;; ## Persistence
;;
;; Stratum datasets persist to
;; [Konserve](https://github.com/replikativ/konserve) stores
;; with branch-based versioning and
;; [snapshot isolation](https://en.wikipedia.org/wiki/Snapshot_isolation).

(require '[konserve.store :as kstore])

(def store-cfg
  {:backend :file
   :path "/tmp/stratum-apiref"
   :id #uuid "660e8400-e29b-41d4-a716-446655440001"})

(when (kstore/store-exists? store-cfg {:sync? true})
  (kstore/delete-store store-cfg {:sync? true}))

(def store (kstore/create-store store-cfg {:sync? true}))

;; ### `sync!` — persist dataset to a branch

(def saved (st/sync! ds store "main"))

(:id (:commit-info saved))

(kind/test-last
 [(fn [id] (uuid? id))])

;; ### `load` — load dataset from a branch

(def loaded (st/load store "main"))

(st/row-count loaded)

(kind/test-last
 [(fn [n] (= 5 n))])

;; ### `fork` — O(1) structural fork

(def forked
  (-> (st/fork ds)
      transient
      (dataset/append! {:x 6 :y 6.6})
      persistent!))

(st/row-count forked)

(kind/test-last
 [(fn [n] (= 6 n))])

;; ### `gc!` — mark-and-sweep garbage collection

(st/sync! forked store "experiment")

(dataset/delete-branch! store "experiment")

(st/gc! store)

(kind/test-last
 [(fn [gc-result]
    (contains? gc-result :deleted-pss-nodes))])

;; Clean up:

(kstore/delete-store store-cfg {:sync? true})

;; ---
;;
;; ## Query Normalization
;;
;; Utilities for programmatic query construction — normalize
;; user-facing syntax to internal form.

;; ### `normalize-pred`

(st/normalize-pred [:> :price 10])

(kind/test-last
 [(fn [pred]
    (vector? pred))])

;; ### `normalize-agg`

(st/normalize-agg [:sum :qty])

(kind/test-last
 [(fn [agg]
    (= :sum (:op agg)))])

;; ### `normalize-expr`

(st/normalize-expr [:* :price :qty])

(kind/test-last
 [(fn [expr]
    (= :mul (:op expr)))])

;; ---
;;
;; ## Anomaly Detection
;;
;; [Isolation forest](https://en.wikipedia.org/wiki/Isolation_forest)
;; implementation — train on columnar data, score/predict in a single
;; pass.

;; ### `train-iforest` — train a model

(def normal-data
  {:x (double-array (repeatedly 500 #(+ 50.0 (* 5.0 (- (rand) 0.5)))))
   :y (double-array (repeatedly 500 #(+ 50.0 (* 5.0 (- (rand) 0.5)))))})

(def model
  (st/train-iforest {:from normal-data
                     :n-trees 50
                     :sample-size 64
                     :seed 42
                     :contamination 0.05}))

(:n-trees model)

(kind/test-last
 [(fn [n] (= 50 n))])

;; ### `iforest-score` — anomaly scores ∈ [0, 1]
;;
;; Higher scores indicate more anomalous points.

(def test-data
  {:x (double-array [50.0 50.0 999.0])
   :y (double-array [50.0 50.0 999.0])})

(def scores (st/iforest-score model test-data))

(seq scores)

(kind/test-last
 [(fn [s]
    (> (nth s 2) (nth s 0)))])

;; ### `iforest-predict` — binary labels (1 = anomaly)

(seq (st/iforest-predict model test-data))

(kind/test-last
 [(fn [preds]
    (= 1 (long (nth preds 2))))])
