(ns bet.impl.pool-test
  (:require [bet.impl.pool :as pool]
            [clojure.core.async :as async]
            [clojure.test :refer :all]))

(def resources (atom {}))

(defn mock-create!
  [k]
  (async/go
    (async/<! (async/timeout (inc (rand-int 1000))))
    (let [r {:k k :resource (rand-int Integer/MAX_VALUE)}]
      (swap! resources update k #(conj % r))
      r)))

(defn update-or-dissoc
  [m k f]
  (let [v (f (get m k))]))


(defn mock-destroy!
  [k r]
  (async/go
    (swap! resources
           (fn [m k f]
             (if-let [v (seq (f (get m k)))]
               (assoc m k v)
               (dissoc m k)))
           k
           (fn [e] (remove #(= % r) e)))))

(use-fixtures :each
  (fn [f]
    (reset! resources {})
    (f)))

(deftest test-basic-pool
  (testing "That basic acquire/release semantics work"
    (let [pool (pool/resource-pool mock-create! mock-destroy!)]
      (let [rsrc (async/<!! (pool/acquire! pool :foo))]
        (is (not (nil? rsrc)))
        (async/<!! (pool/release! pool :foo rsrc))
        (is (empty? @resources))))))

(deftest test-multi-pool
  (testing "that many go-blocks can acquire and release resources"
    (let [maker (fn [n]
                  (async/go-loop []))])))