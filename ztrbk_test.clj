(ns ztrbk-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is]]
            [ztrbk :refer [apply-retention]]))

;; Test data - shared across tests
(def prefix "test_")
(def alt-prefix "other_")

(def snapshots
  ["tank/data@test_2024-01-01_00-00"
   "tank/data@test_2024-01-01_12-00"
   "tank/data@test_2024-01-02_00-00"
   "tank/data@test_2024-01-03_00-00"
   "tank/data@test_2024-01-10_00-00"
   "tank/data@test_2024-02-01_00-00"
   "tank/data@test_2024-03-01_00-00"
   ;; These should be filtered out (different prefix)
   "tank/data@other_2024-01-01_00-00"
   "tank/data@other_2024-12-31_23-59"])

(deftest preserve-min-all-test
  (let [result (apply-retention snapshots prefix
                                {:type :all} nil
                                0 7 1 1)]
    (is (= 7 (count (:keep result))))
    (is (= 0 (count (:destroy result))))))

(deftest preserve-min-latest-test
  (let [result (apply-retention snapshots prefix
                                {:type :latest} nil
                                0 7 1 1)]
    (is (= 1 (count (:keep result))))
    (is (= ["tank/data@test_2024-03-01_00-00"] (:keep result)))
    (is (= 6 (count (:destroy result))))))

(deftest preserve-min-no-test
  (let [result (apply-retention snapshots prefix
                                {:type :no} nil
                                0 7 1 1)]
    (is (= 0 (count (:keep result))))
    (is (= 7 (count (:destroy result))))))

(deftest preserve-min-duration-days-test
  (binding [ztrbk/*now* (java.time.LocalDateTime/parse "2024-01-05T00:00"
                                                       (java.time.format.DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm"))]
    (let [result (apply-retention snapshots prefix
                                  {:type :duration :unit :days :amount 3} nil
                                  0 7 1 1)]
      ;; Should keep snapshots from 2024-01-02 onwards (within 3 days of 2024-01-05)
      ;; But note: snapshots after "now" are also kept since they're not expired
      (is (= 5 (count (:keep result))))
      (is (= 2 (count (:destroy result)))))))

(deftest preserve-days-test
  (binding [ztrbk/*now* (java.time.LocalDateTime/parse "2024-01-15T00:00"
                                                       (java.time.format.DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm"))]
    (let [result (apply-retention snapshots prefix
                                  {:type :no}
                                  {:days 5}
                                  0 7 1 1)]
      ;; Should keep daily snapshots within the last 5 days
      (is (= ["tank/data@test_2024-01-10_00-00"
              "tank/data@test_2024-02-01_00-00"
              "tank/data@test_2024-03-01_00-00"]
             (:keep result)))
      (is (= 4 (count (:destroy result)))))))

(deftest preserve-weeks-test
  (binding [ztrbk/*now* (java.time.LocalDateTime/parse "2024-03-15T00:00"
                                                       (java.time.format.DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm"))]
    (let [result (apply-retention snapshots prefix
                                  {:type :no}
                                  {:weeks 4}
                                  0 7 1 1)]
      ;; Should keep one snapshot per week within the last 4 weeks
      (is (= ["tank/data@test_2024-03-01_00-00"]
             (:keep result))))))

(deftest preserve-months-test
  (binding [ztrbk/*now* (java.time.LocalDateTime/parse "2024-06-01T00:00"
                                                       (java.time.format.DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm"))]
    (let [result (apply-retention snapshots prefix
                                  {:type :no}
                                  {:months 3}
                                  0 7 1 1)]
      ;; Should keep monthly snapshots within the last 3 months
      (is (= ["tank/data@test_2024-03-01_00-00"] (:keep result))))))

(deftest preserve-combined-test
  (binding [ztrbk/*now* (java.time.LocalDateTime/parse "2024-04-01T00:00"
                                                       (java.time.format.DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm"))]
    (let [result (apply-retention snapshots prefix
                                  {:type :no}
                                  {:days 3 :weeks 2 :months 3}
                                  0 7 1 1)]
      ;; Should keep combination of daily, weekly, and monthly
      (is (>= (count (:keep result)) 3)))))

(deftest alt-prefix-filtered-out-test
  (let [result (apply-retention snapshots prefix
                                {:type :all} nil
                                0 7 1 1)]
    ;; Should only process snapshots with matching prefix
    (is (every? #(str/includes? % prefix) (:keep result)))
    (is (not-any? #(str/includes? % alt-prefix) (:keep result)))))
