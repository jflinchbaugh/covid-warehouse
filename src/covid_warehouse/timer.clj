(ns covid-warehouse.timer)

(defmacro timer
  "Evaluates expr and prints the time it took.  Returns the value of expr."
  {:added "1.0"}
  [msg expr]
  `(do
     (println (str (java.util.Date.) ": " ~msg ": starting"))
     (let [start# (. System (nanoTime))
           ret# ~expr]
       (println
         (str (java.util.Date.) ": " ~msg ": " (/ (double (- (. System (nanoTime)) start#)) 1000000000.0) "s"))
       ret#)))
