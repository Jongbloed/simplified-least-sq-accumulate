(def testseq '(1 2 3 4 5 7 9 11 13 16))
(def myfunc (f/fn [iterable] (into [] (map - (map (partial apply -) (partition 2 1 (sequence iterable)))))))
