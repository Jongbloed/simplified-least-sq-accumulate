(ns scct.core
  (:gen-class)
  (:require [flambo.conf :as conf]
            [flambo.api :as f]
            [flambo.tuple :as ft]
            [clojure.string :as s]
            [clj-time.format :as dt]
            [clj-time.core :as t]
            [clj-time.coerce :as tc]))

(f/defsparkfn fourth-and-sixth-csv
  "takes the fourth and sixth string from a string, split by semicolon"
  [line]
  (let [[_ fourth sixth]
        (re-matches #"(?:[^;]+;){3}([^;]+);[^;]+;([^;]+).*" line)]
    [fourth
     sixth]))

(f/defsparkfn normalize-query-str
  "minimizes spacing and converts to lower case"
  [querystr]
  (s/lower-case
    (s/trim
      (s/replace (s/replace querystr
                            #"\s+"
                            " ")
                 #"[^\w ]"
                 ""))))

(f/defsparkfn parse-time-str-mb-long
  "returns a date parsed from input string, in milliseconds, or nil"
  [datestr]
  (tc/to-long
   (try (dt/parse (dt/formatter :date-hour-minute-second-ms) datestr)
     (catch Exception e
       (try (dt/parse (dt/formatter :date-hour-minute-second) datestr)
         (catch Exception e
           (try (dt/parse (dt/formatter :date-hour-minute) datestr)
             (catch Exception e nil))))))))

(f/defsparkfn line-to-time-query-tuple2
  "returns a Spark tuple of the normalized query string and date time in msec from a csv record"
  [line]
  (let [[fourth sixth] (fourth-and-sixth-csv line)]
    (ft/tuple
      (parse-time-str-mb-long sixth)
      (normalize-query-str fourth))))

(f/defsparkfn flip-tuple
  [tuple2]
  (let [kv (f/untuple tuple)
        k (first kv)
        v (second kv)]
    (ft/tuple v k)))

(f/defsparkfn squared [x] (* x x))

(f/defsparkfn accumulate-slope-fraction
  [acc kvv_query_timestamp_count]
  (let [k_tuple2 (f/untuple kvv_query_timestamp_count)]
    (if (nil? (:n acc))
     ;then
      (let [query (first k_tuple2)
            [firstTimestamp totalDataPoints] (f/untuple (second k_tuple2))]

        { :n 1
          :meanX (/ totalDataPoints 2)
          :meanY 0
          :timestamp firstTimestamp
          :NumeratorB 0
          :DenominatorB 0})
     ;else
      (let [[currentTimestamp _] (f/untuple (second k_tuple2))
            n (+ 1 (:n acc))
            cX_ (:meanX acc) ; X = n, dx = 1 (constant), so meanX = count/2 (constant so use or to evaluate once)
            oldY_ (:meanY acc)
            oldX (:n acc)
            newX (+ oldX 1)
            newY (/ 1 (- currentTimestamp (:timestamp acc))) ;Y = 1 / timespan between hits
            newY_ (+ (* oldY_ (/ (- n 1) n)) (/ newY n))
            oldNumerator (:NumeratorB acc)
            oldDenominator (:DenominatorB acc)
            newDenominator (+ oldDenominator (sq (- newX cX_)))
            newNumerator (+ oldNumerator
                             (* (- oldX cX_) (- oldY_ newY_))
                             (* (- newX cX_) (- newY newY_)))]
        { :n n
          :meanX cX_
          :meanY newY_
          :timestamp currentTimestamp
          :NumeratorB newNumerator
          :DenominatorB newDenominator}))))


(def c
  (-> (conf/spark-conf)
      (conf/master "local[*]")
      (conf/app-name "skct")))

(defn -main []
  (f/with-context sc c
    (let
      [date-query-ordered
        (-> (f/text-file sc "resources/kaggle_geo - Erik .csv")
            (f/map-to-pair line-to-time-query-tuple2) ; (query date, query)
            (f/filter (ft/key-val-fn [timestamp query] timestamp)) ; filter out nil timestamp
            f/sort-by-key ; (query date sorted, query)
            f/cache)
       counted-queries
        (-> date-query-ordered
            f/values ;(query)
            (f/map-to-pair (f/fn [key] (ft/tuple key 1))) ;(query, 1)
            (f/reduce-by-key (f/fn [_ __] (+ _ __))) ;(query, count)
            (f/filter (f/fn [tuple] (> (second (f/untuple tuple)) 2))) ;eliminate queries with too few data points
            f/cache) ;(query, count) where count >=3
       distinct-queries-and-dates
        (-> date-query-ordered ; (timestamp, query) ordered by timestamp
            (f/map-to-pair flip-tuple) ; (query, timestamp) ordered by timestamp
            (f/join counted-queries); join distinct queries with enough data points (query, count) to
            f/cache)    ; (query, (timestamp, count))
       query-slope
        (-> distinct-queries-and-dates
            (f/fold nil accumulate-slope-fraction)
            (f/map-to-pair (ft/key-val-fn [key acc] (ft/tuple key (/ (:NumeratorB acc) (:DenominatorB acc))))))]

      (-> query-slope
          (f/take-ordered 40)
          f/collect
          clojure.pprint/pprint))))
