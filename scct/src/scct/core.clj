(ns scct.core
  (:gen-class :main true)
  (:require [flambo.conf :as conf]
            [flambo.api :as f]
            [flambo.tuple :as ft]
            [clojure.string :as s]
            [clj-time.format :as dt]
            [clj-time.core :as t]
            [clj-time.coerce :as tc]
            [clj-time.local :as tl])
  (:import  (java.lang Math)))

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
  (let [timestamp
        (try (dt/parse (dt/formatter :date-hour-minute-second-ms) datestr)
          (catch Exception e
            (try (dt/parse (dt/formatter :date-hour-minute-second) datestr)
              (catch Exception e
                (try (dt/parse (dt/formatter :date-hour-minute) datestr)
                  (catch Exception e nil))))))]
    (if (nil? timestamp) nil
      (t/in-millis
       (t/interval
        (t/date-time 1970 1 1)
        timestamp)))))


(f/defsparkfn line-to-query-time-tuple2
  [line]
  "returns a Spark tuple of the normalized query string and date time in msec from a csv record"
  (let [[fourth sixth] (fourth-and-sixth-csv line)]
    (ft/tuple
      (normalize-query-str fourth)
      (parse-time-str-mb-long sixth))))

(f/defsparkfn flip-tuple
  [tuple2]
  (let [kv (f/untuple tuple2)
        k (first kv)
        v (second kv)]
    (ft/tuple v k)))

(f/defsparkfn squared [x] (* x x))
(f/defsparkfn abs [x] (if (> x 0) x (- x)))

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

(f/defsparkfn compute-hits-over-time-n
  [absolute_t]
  (seq
    (map-indexed (f/fn [k v] (ft/tuple k v))
      (map
        (f/fn hits-over-time [querycount_time_tuples]
          (let [ [[new_visitors1 time1]
                  [new_visitors2 time2]]
                 querycount_time_tuples]
            (/
              (* 1000000000(/ (+ new_visitors2 new_visitors1) 2)) ;without multiplying by a big number here, the ordering is off, probably an overflow thing
              (let [dt (-  time2 time1)]
                (if (zero? dt) 1 ;avoid DivideByZero in the rare case that ALL the entries of a query were with the exact same timestamp
                  dt)))))

        (partition 2 1
          ((f/fn filter-and-count-duplicates
            [remaining-values]
            (lazy-seq
              (if (empty? remaining-values)
                (sequence '())
                (let [nextvalue (first remaining-values)
                      dupcount (count (take-while #(= % nextvalue) remaining-values))
                      pair [dupcount nextvalue]]
                  (cons pair
                    (if (= (inc dupcount) (count (take (inc dupcount) remaining-values)))
                      (filter-and-count-duplicates (nthnext remaining-values dupcount))
                      (sequence '())))))))
           (iterable-seq absolute_t)))))))

(def means-and-coords-to-least-sq-slope ;(query, ((meanX, meanY), ((X1, Y1), (X2, Y2)...(Xn, Yn)))
  (ft/key-val-val-fn
    (f/fn [query means coords]
      (let [[X_ Y_] (f/untuple means)
            gridpoints (map (f/fn [x_y](let [[x y] (f/untuple x_y)] {:x x :y y})) (iterable-seq coords))
            numeratorpartials (map (f/fn [point](* (- (point :x) X_) (- (point :y) Y_))) gridpoints)
            denominatorpartials (map (f/fn [point](squared (- (point :x) X_))) gridpoints)
            numerator (reduce (f/fn [_ __] (+ _ __)) numeratorpartials)
            denominator (reduce (f/fn [_ __] (+ _ __)) denominatorpartials)
            slope (if (zero? denominator) 0
                    (/ numerator denominator))]
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
      [distinct-query-and-coords
        (-> (f/text-file sc "resources/kaggle_geo - Erik .csv.sample2")
            (f/map-to-pair line-to-query-time-tuple2) ; (query, timestamp)
            (f/filter (ft/key-val-fn (f/fn [query timestamp] (not (nil? timestamp))))) ; filter out unparsable date values (and CSV header ;)
            (f/partition-by (f/hash-partitioner 4)) ;ensure groups can be reduced in one go
            f/group-by-key
            (f/map-values (f/fn [timestamps] (seq (sort (iterable-seq timestamps))))) ; times should be in chronological order now
            (f/map-values compute-hits-over-time-n) ; (query, ((X1, Y1), (X2, Y2)...(Xn, Yn)))
            f/cache)

       distinct-query-and-means
        (-> distinct-query-and-coords
            (f/map-to-pair
              (ft/key-val-fn
               (f/fn
                [query coords]
                (let [yvalues (map (ft/key-val-fn (f/fn [x y] y )) coords)
                      numdatapoints (count yvalues)
                      sumY (reduce + yvalues)]
                  (ft/tuple query (ft/tuple sumY numdatapoints))))))
            (f/filter (ft/key-val-val-fn (f/fn [query sumY n] (> n 0)))) ; it does happen that only some entries with same timestamp have been found
            (f/map-to-pair                                               ; in which case we still can't calculate change in frequency
              (ft/key-val-val-fn
               (f/fn
                [query sumY n]
                (ft/tuple
                  query
                  (ft/tuple
                    (/ (dec n) 2)   ; meanX = (n-1) / 2
                    (/ sumY n)))))) ; meanY = S(y) / n
            f/cache)

        slope-query
        (-> distinct-query-and-means
            (f/join distinct-query-and-coords)
            (f/map-to-pair means-and-coords-to-least-sq-slope)
            f/cache)]

      (let [ascending
              (fn [kv1 kv2]
                (let [[a1 _] (f/untuple kv1)
                      [a2 _] (f/untuple kv2)]
                  (- a1 a2)))
            descending
              (fn [kv1 kv2]
                (let [[a1 _] (f/untuple kv1)
                      [a2 _] (f/untuple kv2)]
                  (- a2 a1)))
            describe-msec
              (fn [msec]
                (let [oneday (* 1000 60 60 24)
                      onehour (/ oneday 24)
                      oneminute (/ onehour 60)
                      days (int (/ msec oneday))
                      hours (int (/ (- msec (* days oneday)) onehour))
                      minutes (int (/ (- msec (* days oneday) (* hours onehour)) oneminute))]
                  (str (if (pos? days) (str days " days, ") "")
                       (if (pos? hours) (str hours " hours and ") "")
                       (if (pos? hours) (str minutes " minutes ") ""))))
            explain
              (fn [kv]
                (let [[hits-per-msec_dx query] (f/untuple kv)
                      word (if (neg? hits-per-msec_dx) "declining" "increasing")]
                  (str "                  Search string: [" query
                       "]\r\n  dU/dt² in Change in interest over msec²: [" hits-per-msec_dx
                       "]\r\n    Explanation: The average number of users per millisecond that search for \"" query
                       "\"appears to be " word " by 1 every " (describe-msec (abs (/ 1 hits-per-msec_dx)))
                       "\r\n\r\n")))

            top-ten
              (-> slope-query
                  (f/filter (ft/key-val-fn (f/fn [slope _] (> slope 0))))
                  (f/take-ordered 10 descending))

            bottom-ten
              (-> slope-query
                  (f/filter (ft/key-val-fn (f/fn [slope _] (< slope 0))))
                  (f/take-ordered 10 ascending))]

        (spit "resultaat.txt"
          (str "Top 10 fastest growing searches:\r\n\r\n"
               (apply str (map explain top-ten))
               "\r\n\r\n\r\n\r\nTop 10 fastest declining searches:\r\n\r\n"
               (apply str (map explain bottom-ten)))))

      (save-rdd! distinct-query-and-coords)
      (save-rdd! distinct-query-and-means)
      (save-rdd! slope-query))))
