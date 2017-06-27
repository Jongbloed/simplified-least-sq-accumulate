(ns scct.core
  (:gen-class)
  (:require [flambo.conf :as conf]
            [flambo.api :as f]
            [flambo.tuple :as ft]
            [clojure.string :as s]
            [clj-time.format :as dt]
            [clj-time.core :as t]))

(defn extract-query-and-time
  [line]
  (let [[_ fourth sixth]
        (re-matches #"(?:[^;]+;){3}([^;]+);[^;]+;([^;]+).*" line)]
    {:key fourth :datestr sixth}))

(defn make-time-query-tuple
  [dk]
  (let [{datestr :datestr key :key} dk]
  ;(ft/tuple key (msec-since-2000 datestr)))
   (ft/tuple
     (t/in-millis
       (t/interval
         (t/date-time 2000)
         (try (dt/parse (dt/formatter :date-hour-minute-second-ms) datestr)
           (catch Exception e
             (try (dt/parse (dt/formatter :date-hour-minute-second) datestr)
               (catch Exception e nil))))))

     (s/lower-case (s/trim (s/replace (s/replace key #"\s+" " ") #"[^\w ]" ""))))))

(f/defsparkfn sq [x] (* x x))

(f/defsparkfn accumulate-slope-fraction
  [acc tuple2_total_timestamp]
  (if (not (:n acc))
    (let [totalDataPoints (first (f/untuple tuple2_total_timestamp))
          firstTimestamp (second (f/untuple tuple2_total_timestamp))]
      { :n 1
        :meanX (/ totalDataPoints 2)
        :meanY 0
        :timestamp firstTimestamp
        :NumeratorB 0
        :DenominatorB 0})

    (let [currentTimestamp (second (f/untuple tuple2_total_timestamp))
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
        :DenominatorB newDenominator})))


(def c
  (-> (conf/spark-conf)
      (conf/master "local[*]")
      (conf/app-name "skct")))

(defn -main []
  (f/with-context sc c
    (let
      [date-query-ordered
        (-> (f/text-file sc "resources/kaggle_geo - Erik .csv")
            (f/map-to-pair (f/fn [line] (make-time-query-tuple (extract-query-and-time line)))) ; (query date, query)
            f/sort-by-key ; (query date sorted, query)
            f/cache)
       counted-queries
        (-> date-query-ordered
            f/values
            (f/map-to-pair (f/fn [key] (ft/tuple key 1)))
            (f/reduce-by-key (f/fn [_ __] (+ _ __)))
            (f/filter (f/fn [tuple] (> (second (f/untuple tuple)) 2))) ; eliminate queries with too little data points
            f/cache)
       distinct-queries-and-dates
        (-> counted-queries
            (f/join (f/map-to-pair date-query-ordered (f/fn [tuple] (ft/tuple (second (f/untuple tuple)) (first (f/untuple tuple))))))
            (f/cache))    ; (query, (totalQueryCount, date))
       query-slope
        (-> distinct-queries-and-dates
            (f/fold nil accumulate-slope-fraction))]
            ;(f/map-to-pair (f/fn [key_acc] (ft/tuple (first (f/untuple key_acc)) (let [acc (second (f/untuple key_acc))] (/ (:NumeratorB acc) (:DenominatorB acc)))))))]
      (-> query-slope
          (f/take-ordered 40)
          f/collect
          clojure.pprint/pprint))))
