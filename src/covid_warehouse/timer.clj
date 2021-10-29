(ns covid-warehouse.timer)

(defmacro timer
  "Evaluates expr and prints the time it took.  Returns the value of expr."
  {:added "1.0"}
  [msg expr]
  `(do
     (println (str ~msg ": starting"))
     (let [start# (. System (nanoTime))
           ret# ~expr]
       (println
         (str ~msg ": " (/ (double (- (. System (nanoTime)) start#)) 1000000000.0) "s"))
       ret#)))
