(ns scct.core
  (:gen-class :main true)
  (:require [flambo.conf :as conf]
            [flambo.api :as f]
            [flambo.tuple :as ft]
            [clojure.string :as s]
            [clj-time.format :as dt]
            [clj-time.core :as t]
            [clj-time.coerce :as tc]
            [clj-time.local :as tl]))

(f/defsparkfn fourth-and-sixth-csv
  [line]
  "takes the fourth and sixth string from a string, split by semicolon"
  (let [[_ fourth sixth]
        (re-matches #"(?:[^;]+;){3}([^;]+);[^;]+;([^;]+).*" line)]
    [fourth
     sixth]))

(f/defsparkfn normalize-query-str
  [querystr]
  "minimizes spacing and converts to lower case"
  (s/lower-case
    (s/trim
      (s/replace (s/replace querystr
                            #"\s+"
                            " ")
                 #"[^\w ]"
                 ""))))

(f/defsparkfn parse-time-str-mb-long
  [datestr]
  "returns a date parsed from input string, in milliseconds, or nil"
  (tc/to-long
   (try (dt/parse (dt/formatter :date-hour-minute-second-ms) datestr)
     (catch Exception e
       (try (dt/parse (dt/formatter :date-hour-minute-second) datestr)
         (catch Exception e
           (try (dt/parse (dt/formatter :date-hour-minute) datestr)
             (catch Exception e nil))))))))

(f/defsparkfn line-to-time-query-tuple2
  [line]
  "returns a Spark tuple of the normalized query string and date time in msec from a csv record"
  (let [[fourth sixth] (fourth-and-sixth-csv line)]
    (ft/tuple
      (parse-time-str-mb-long sixth)
      (normalize-query-str fourth))))

(f/defsparkfn flip-tuple
  [tuple2]
  (let [kv (f/untuple tuple2)
        k (first kv)
        v (second kv)]
    (ft/tuple v k)))

(f/defsparkfn squared [x] (* x x))
(f/defsparkfn abs [x] #(if (<= % 0) (- x) x))

(f/defsparkfn iter-seq
  [iter]
  ;"Takes a Scala iterable, and turns it into a lazy-seq"
  (lazy-seq
    (when (.hasNext iter)
     (cons (.next iter)
           (iter-seq iter)))))

(f/defsparkfn iterable-seq
  [s]
  ;"Takes a Scala iterable s, and returns a lazy-seq of its contents."
  (iter-seq (.iterator s)))

(f/defsparkfn timestamp-entries-to-xy-tuple
  [iterable]
  (seq
    (map-indexed #(ft/tuple %1 (- %2))
      (map (partial apply -)
        (partition 2 1 (iterable-seq iterable))))))

(def means-and-coords-to-least-sq-slope ;(query, ((meanX, meanY), ((X1, Y1), (X2, Y2)...(Xn, Yn)))
  (ft/key-val-val-fn
    (f/fn [query means coords]
      (let [[X_ Y_] (f/untuple means)
            gridpoints (map #(let [[x y] (f/untuple %)] {:x x :y y}) (iterable-seq coords))
            numeratorpartials (map #(* (- (% :x) X_) (- (% :y) Y_)) gridpoints)
            denominatorpartials (map #(squared (- (% :x) X_)) gridpoints)
            numerator (reduce + numeratorpartials)
            denominator (reduce + denominatorpartials)
            slope (/ numerator denominator)]
        (ft/tuple slope query))))) ; (query, slope)

(def c
  (-> (conf/spark-conf)
      (conf/master "local[*]")
      (conf/app-name "skct")))

(defmacro save-rdd!
  [rdd]
  `(f/save-as-text-file ~rdd (str "spark_textfiles/" (name '~rdd) (tc/to-long (tl/local-now)))))

(defn -main []
  (f/with-context sc c
    (let
      [date-query-ordered
        (-> (f/text-file sc "resources/kaggle_geo - Erik .csv")
            (f/map-to-pair line-to-time-query-tuple2) ; (query date, query)
            (f/filter (ft/key-val-fn (f/fn [timestamp query] timestamp))) ; filter out nil timestamp
            f/sort-by-key ; (query date sorted, query)
            f/cache)

       distinct-query-and-meanX
        (-> date-query-ordered
            f/values ;(query)
            (f/map-to-pair (f/fn [key] (ft/tuple key 1))) ;(query, 1)
            (f/reduce-by-key (f/fn [_ __] (+ _ __))) ;(query, count)
            (f/filter (f/fn [tuple] (> (second (f/untuple tuple)) 2))) ;eliminate queries with too few data points
            (f/map-values (f/fn [count] (/ count 2)))
            f/cache)
             ;(query, meanX) where count >=3
       query-Y-and-meanX
        (-> date-query-ordered ; (timestamp, query) ordered by timestamp
            (f/map-to-pair flip-tuple) ; (query, timestamp) ordered by timestamp
            (f/join distinct-query-and-meanX); join distinct queries with enough data points (query, count) to
            (f/partition-by (f/hash-partitioner (f/partition-count date-query-ordered)))
            f/cache)
                ; (query, (Y, meanX))
       distinct-query-and-meanY
        (-> query-Y-and-meanX
            (f/map-values (ft/key-val-fn (f/fn [timestamp halfn] (/ timestamp (* 2 halfn)))))
            (f/reduce-by-key (f/fn [_ __] (+ _ __))) ; (query, meanY)
            f/cache)

       distinct-queries-and-XY
        (-> date-query-ordered
            (f/map-to-pair flip-tuple)
            (f/join distinct-query-and-meanX) ;filter out <3 data point entries
            (f/map-to-pair (ft/key-val-val-fn (f/fn [query date meanX] (ft/tuple query date)))) ; forget about meanX for now
            f/group-by-key
            (f/partition-by (f/hash-partitioner (f/partition-count date-query-ordered)))
            (f/map-values timestamp-entries-to-xy-tuple) ; (query, (X, Y))
            f/cache)

       distinct-query-and-meanXmeanY-and-XY
        (-> distinct-query-and-meanX ;(query, meanX)
            (f/join distinct-query-and-meanY) ;(query, (meanX, meanY))
            (f/join distinct-queries-and-XY) ;(query, ((meanX, meanY), ((X1, Y1), (X2, Y2)...(Xn, Yn)))
            f/cache)

       distinct-slope-and-query
        (-> distinct-query-and-meanXmeanY-and-XY
            (f/map-to-pair means-and-coords-to-least-sq-slope)
            f/cache)]

      (let [ascending
              (fn [kv1 kv2]
                (let [[n1 _] (f/untuple kv1)
                      [n2 _] (f/untuple kv2)]
                  (- n1 n2)))
            descending
              (fn [kv1 kv2]
                (let [[n1 _] (f/untuple kv1)
                      [n2 _] (f/untuple kv2)]
                  (- n2 n1)))
            explain
              (fn [kv]
                (let [[msec_n query] (f/untuple kv)
                      word (if (neg? msec_n) "less" "longer")]
                  (str "Search string: |"
                       query
                       "| ddt/dx in Milliseconds over N: |" msec_n
                       "| Explanation: Everytime someone searches for " query
                       ", it will take " (with-precision 2 (bigdec (abs (/ msec_n 1000 3600 24))))
                       " days " word " for the next person to search for " query "\r\n")))

            top-ten
              (-> distinct-slope-and-query
                  f/sort-by-key
                  f/cache
                  (f/take-ordered 10 ascending))

            bottom-ten
              (-> distinct-slope-and-query
                  f/sort-by-key
                  f/cache
                  (f/take-ordered 10 descending))]

        (spit "result.txt"
          (str "Top 10 fastest growing searches:\r\n"
               (apply str (map explain top-ten))
               "Top 10 fastest declining searches:\r\n"
               (apply str (map explain bottom-ten)))))



      ;(f/save-as-text-file date-query-ordered "spark_textfiles/date-query-ordered")
      ;(f/save-as-text-file counted-queries "spark_textfiles/counted-queries")
      ;(f/save-as-text-file distinct-queries-and-dates "spark_textfiles/distinct-queries-and-dates")
      ;(println "got to query-slope!!!\n\n")
      (save-rdd! distinct-slope-and-query))))

  ;    (-> distinct-query-and-meanY
   ;       (f/take 40)
    ;      f/collect
     ;     clojure.pprint/pprint))))
