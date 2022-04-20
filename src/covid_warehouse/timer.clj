(ns covid-warehouse.timer
  (:require
   [taoensso.timbre :as l]))

(defmacro timer
  "Evaluates expr and prints the time it took.  Returns the value of expr."
  {:added "1.0"}
  [msg expr]
  `(do
     (l/info (str ~msg ": starting"))
     (let [start# (. System (nanoTime))
           ret# ~expr]
       (l/info
         (str ~msg ": " (/ (double (- (. System (nanoTime)) start#)) 1000000000.0) "s"))
       ret#)))
