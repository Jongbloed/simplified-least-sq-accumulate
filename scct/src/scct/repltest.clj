(def testseq '(1 2 3 4 5 7 9 11 13 16))
(def myfunc (f/fn [iterable] (into [] (map - (map (partial apply -) (partition 2 1 (sequence iterable)))))))

(def testdata (ft/tuple "magnetron" (ft/tuple (ft/tuple 3 4) '( (ft/tuple 1 2)  (ft/tuple 2 4)  (ft/tuple 3 5) (ft/tuple 4 4) (ft/tuple 5 5) ))))
