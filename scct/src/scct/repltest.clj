(def testseq '(1 2 3 4 5 5 7 9 11 13 13 16))
(def myfunc (f/fn [iterable] (into [] (flatten (map #((if zero? (second %) (/ first 2))) (partition 2 1 (map - (map (partial apply -) (partition 2 1 (sequence iterable))))))))))

(def testdata (ft/tuple "magnetron" (ft/tuple (ft/tuple 3 4) '( (ft/tuple 1 2)  (ft/tuple 2 4)  (ft/tuple 3 5) (ft/tuple 4 4) (ft/tuple 5 5)))))



(map #(let [[[n1 d1][n2 d2]]%]  (/ (/(+ n2 n1)2) (- d2 d1) )) (partition 2 1 heeleind))


(defn compute-dy_dx
  [remaining-values]
  (lazy-seq
    (if (empty? remaining-values)
      (sequence
       [])
      (let [n  (count (take-while #(= % (first remaining-values)) remaining-values))
            pair  [n (first remaining-values)]]
        (cons pair
          (if (= (inc n) (count (take (inc n ) remaining-values)))
            (compute-dy_dx (nthnext remaining-values n))
            (sequence
             [])))))))

(map
  (fn slope [n_t]
    (let [ [[y1 x1]
            [y2 x2
              n_t]]]
     (/
       (/(+ y2 y1) 2)
       (- x2 x1))))

  (partition 2 1))

((fn filter-and-count-duplicates
  [remaining-values]
  (lazy-seq
    (if (empty? remaining-values)
      (sequence
       [])
      (let [n  (count (take-while #(= % (first remaining-values)) remaining-values))
            pair  [n (first remaining-values)]]
        (cons pair
          (if (= (inc n) (count (take (inc n ) remaining-values)))
            (filter-and-count-duplicates (nthnext remaining-values n))
            (sequence
             [])))))))     '(1 2 3 4 5 5 7 9 11 13 13 16))
