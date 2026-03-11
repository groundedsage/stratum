;; # CSV Import
;;
;; Stratum reads CSV files into typed columnar datasets with automatic
;; type detection, configurable separators, and
;; [PostgreSQL-compatible NULL semantics](https://www.postgresql.org/docs/current/sql-copy.html).
;;
;; This notebook exercises every `from-csv` option and the companion
;; `from-maps` helper, serving as both documentation and regression
;; test.

(ns stratum-book.csv-import
  (:require
   [stratum.csv :as csv]
   [stratum.query :as q]
   [stratum.dataset :as dataset]
   [scicloj.kindly.v4.kind :as kind]))

;; ---
;;
;; ## Helpers
;;
;; A small utility to write a CSV string to a temp file.

(defn write-temp-csv [content]
  (let [f (java.io.File/createTempFile "stratum-book" ".csv")]
    (.deleteOnExit f)
    (spit f content)
    (.getAbsolutePath f)))

;; ---
;;
;; ## Basic Import
;;
;; `from-csv` auto-detects column types: integers → `:int64`,
;; decimals → `:float64`, everything else → dictionary-encoded strings.

(def basic-ds
  (csv/from-csv
   (write-temp-csv "name,age,score\nAlice,30,95.5\nBob,25,87.3\nCarol,35,92.1")))

(dataset/row-count basic-ds)

(kind/test-last
 [(fn [n] (= 3 n))])

;; Three columns detected:

(set (dataset/column-names basic-ds))

(kind/test-last
 [(fn [names]
    (= #{:name :age :score} names))])

;; Types are inferred correctly — `age` is integer, `score` is float,
;; `name` is a string (stored as dictionary-encoded `:int64` codes
;; with a `:dict` lookup array).

(dataset/column-type basic-ds :age)

(kind/test-last
 [(fn [t] (= :int64 t))])

(dataset/column-type basic-ds :score)

(kind/test-last
 [(fn [t] (= :float64 t))])

(some? (:dict (dataset/column basic-ds :name)))

(kind/test-last
 [true?])

;; ---
;;
;; ## Type Override
;;
;; The `:types` option forces a column to a specific type.
;; Here we force an integer column to be read as `:double`.

(def override-ds
  (csv/from-csv
   (write-temp-csv "id,value\n1,100\n2,200\n3,300")
   :types {"value" :double}))

(dataset/column-type override-ds :value)

(kind/test-last
 [(fn [t] (= :float64 t))])

;; ---
;;
;; ## Custom Separator
;;
;; Tab-separated files work with `:separator \tab`.

(def tsv-ds
  (csv/from-csv
   (write-temp-csv "a\tb\n1\t2\n3\t4")
   :separator \tab))

(count (dataset/column-names tsv-ds))

(kind/test-last
 [(fn [n] (= 2 n))])

(dataset/column-type tsv-ds :a)

(kind/test-last
 [(fn [t] (= :int64 t))])

;; ---
;;
;; ## No Header
;;
;; When `:header?` is false, columns are named `:col0`, `:col1`, etc.

(def no-header-ds
  (csv/from-csv
   (write-temp-csv "1,hello\n2,world\n3,test")
   :header? false))

(set (dataset/column-names no-header-ds))

(kind/test-last
 [(fn [names]
    (and (contains? names :col0)
         (contains? names :col1)))])

;; ---
;;
;; ## Row Limit
;;
;; `:limit` caps the number of rows read.

(def limited-ds
  (csv/from-csv
   (write-temp-csv "x\n1\n2\n3\n4\n5")
   :limit 3))

(dataset/row-count limited-ds)

(kind/test-last
 [(fn [n] (= 3 n))])

;; ---
;;
;; ## Query Integration
;;
;; CSV-imported datasets plug straight into the query engine.

(def products-ds
  (csv/from-csv
   (write-temp-csv "product,price,qty\nWidget,10.50,5\nGadget,25.00,3\nWidget,10.50,2")))

(def by-product
  (->> (q/q {:from products-ds :group [:product] :agg [[:sum :qty]]})
       (into {} (map (fn [r] [(:product r) (:sum r)])))))

by-product

(kind/test-last
 [(fn [m]
    (and (= 7.0 (get m "Widget"))
         (= 3.0 (get m "Gadget"))))])

;; ---
;;
;; ## `from-maps` — Sequence of Maps
;;
;; Converts `[{:a 1 :b "x"} ...]` into a typed dataset. Types are
;; inferred from the first non-nil value per column.

;; Integer column:

(def maps-ds (csv/from-maps [{:a 1 :b "x"} {:a 2 :b "y"} {:a 3 :b "x"}]))

(dataset/column-type maps-ds :a)

(kind/test-last
 [(fn [t] (= :int64 t))])

;; String column is dictionary-encoded:

(some? (:dict (dataset/column maps-ds :b)))

(kind/test-last
 [true?])

;; Double column:

(dataset/column-type
 (csv/from-maps [{:v 1.5} {:v 2.5}])
 :v)

(kind/test-last
 [(fn [t] (= :float64 t))])

;; Nil values are handled gracefully:

(dataset/row-count
 (csv/from-maps [{:x 1 :y nil} {:x 2 :y 3}]))

(kind/test-last
 [(fn [n] (= 2 n))])

;; ---
;;
;; ## PostgreSQL NULL Semantics
;;
;; Stratum follows PostgreSQL `COPY` conventions:
;;
;; - **Unquoted empty field** → NULL (sentinel value)
;; - **Quoted empty field `""`** → empty string `""`
;;
;; This distinction matters for analytics: NULLs are excluded from
;; aggregates, while empty strings are counted.

;; ### String columns

(def null-ds
  (csv/from-csv
   (write-temp-csv "a,b,c\n1,,3\n4,\"\",6")))

(dataset/row-count null-ds)

(kind/test-last
 [(fn [n] (= 2 n))])

;; Column `b` — row 0 has an unquoted empty field (NULL sentinel
;; `Long/MIN_VALUE`), row 1 has a quoted empty string:

(let [b-col (dataset/column null-ds :b)
      data  ^longs (:data b-col)
      dict  ^"[Ljava.lang.String;" (:dict b-col)]
  {:row-0-null?  (= Long/MIN_VALUE (aget data 0))
   :row-1-value  (aget dict (int (aget data 1)))})

(kind/test-last
 [(fn [{:keys [row-0-null? row-1-value]}]
    (and row-0-null?
         (= "" row-1-value)))])

;; ### Numeric columns
;;
;; For integers, NULL is `Long/MIN_VALUE`. For doubles, NULL is `NaN`.

(def null-num-ds
  (csv/from-csv
   (write-temp-csv "x,y\n1,10.5\n,\n3,30.5")))

(let [x ^longs   (:data (dataset/column null-num-ds :x))
      y ^doubles (:data (dataset/column null-num-ds :y))]
  {:x0 (aget x 0) :x1-null? (= Long/MIN_VALUE (aget x 1)) :x2 (aget x 2)
   :y0 (aget y 0) :y1-nan?  (Double/isNaN (aget y 1))      :y2 (aget y 2)})

(kind/test-last
 [(fn [{:keys [x0 x1-null? x2 y0 y1-nan? y2]}]
    (and (= 1 x0) x1-null? (= 3 x2)
         (= 10.5 y0) y1-nan? (= 30.5 y2)))])

;; ---
;;
;; ## Low-Level CSV Parser
;;
;; `read-csv` returns raw row vectors. Unquoted empty fields are `nil`;
;; quoted empty fields are `""`.

(csv/read-csv "a,b,c\n1,,3\n4,\"\",6")

(kind/test-last
 [(fn [rows]
    (let [rows (vec rows)]
      (and (= ["a" "b" "c"] (first rows))
           (= ["1" nil "3"] (second rows))
           (= ["4" "" "6"]  (nth rows 2)))))])

;; Trailing comma produces a trailing `nil`:

(csv/read-csv "a,b\n1,\n2,3")

(kind/test-last
 [(fn [rows]
    (let [rows (vec rows)]
      (nil? (second (second rows)))))])

;; All unquoted empty fields → all `nil`:

(csv/read-csv "a,b,c\n,,")

(kind/test-last
 [(fn [rows]
    (= [nil nil nil] (second (vec rows))))])

;; Quoted fields containing commas are preserved:

(csv/read-csv "a,b\n\"hello,world\",2")

(kind/test-last
 [(fn [rows]
    (= "hello,world" (first (second (vec rows)))))])
