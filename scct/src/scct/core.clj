(ns scct.core
  (:gen-class)
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

(f/defsparkfn differences
  [iterable]
  (seq
    (map -
      (map (partial apply -)
        (partition 2 1 (iterable-seq iterable))))))

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

       distinct-queries-and-Y
        (-> date-query-ordered
            (f/map-to-pair flip-tuple)
            (f/join distinct-query-and-meanX) ;filter out <3 data point entries
            (f/map-to-pair (ft/key-val-val-fn (f/fn [query date meanX] (ft/tuple query date)))) ; forget about meanX for now
            f/group-by-key
            (f/partition-by (f/hash-partitioner (f/partition-count date-query-ordered)))
            (f/map-values differences)
            ;(f/flat-map-to-pair
             ; (f/fn [rdd]
              ;  (f/fold (._2 rdd)
               ;         []
                ;        (f/fn [diffs value]
                 ;         (if (empty? diffs)
                  ;          [value]
                   ;         (conj (pop diffs)
                    ;          [(- value (peek diffs))]
            f/cache)]



      ;(f/save-as-text-file date-query-ordered "spark_textfiles/date-query-ordered")
      ;(f/save-as-text-file counted-queries "spark_textfiles/counted-queries")
      ;(f/save-as-text-file distinct-queries-and-dates "spark_textfiles/distinct-queries-and-dates")
      ;(println "got to query-slope!!!\n\n")
      (save-rdd! distinct-queries-and-Y))))

  ;    (-> distinct-query-and-meanY
   ;       (f/take 40)
    ;      f/collect
     ;     clojure.pprint/pprint))))
