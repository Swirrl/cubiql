(ns cubiql.query-model-test
  (:require [clojure.test :refer :all]
            [cubiql.query-model :refer :all :as qm]
            [cubiql.vocabulary :refer [rdfs:label]])
  (:import [java.net URI]))

(deftest add-binding-test
  (testing "Default"
    (let [value "value"
          qm (add-binding empty-model [[:key (URI. "http://predicate")]] value)]
      (is (= value (get-path-binding-value qm [:key])))
      (is (= false (is-path-binding-optional? qm [:key])))))

  (testing "Optional"
    (let [value (URI. "http://value")
          qm (add-binding empty-model [[:key (URI. "http://predicate")]] value :optional? true)]
      (is (= value (get-path-binding-value qm [:key])))
      (is (= true (is-path-binding-optional? qm [:key])))))

  (testing "Required"
    (let [value (URI. "http://value")
          qm (add-binding empty-model [[:key (URI. "http://predicate")]] value :optional? false)]
      (is (= value (get-path-binding-value qm [:key])))
      (is (= false (is-path-binding-optional? qm [:key])))))

  (testing "Optional then required"
    (let [predicate (URI. "http://predicate")
          value "value"
          qm (-> empty-model
                 (add-binding [[:key predicate]] ::qm/var :optional? true)
                 (add-binding [[:key predicate]] value))]
      (is (= value (get-path-binding-value qm [:key])))
      (is (= false (is-path-binding-optional? qm [:key])))))

  (testing "Required then optional"
    (let [predicate (URI. "http://predicate")
          value "value"
          qm (-> empty-model
                 (add-binding [[:key predicate]] ::qm/var :optional? false)
                 (add-binding [[:key predicate]] value :optional true))]
      (is (= value (get-path-binding-value qm [:key])))
      (is (= false (is-path-binding-optional? qm [:key])))))

  (testing "Multiple values"
    (let [v1 "value1"
          v2 (URI. "http://value2")
          predicate (URI. "http://predicate")
          qm (add-binding empty-model [[:key predicate]] v1)]
      (is (thrown? Exception (add-binding qm [[:key predicate]] v2)))))

  (testing "Inconsistent predicates"
    (let [p1 (URI. "http://p1")
          p2 (URI. "http://p2")
          qm (add-binding empty-model [[:k p1] [:label rdfs:label]] ::qm/var)]
      (is (thrown? Exception (add-binding qm [[:k p2] [:label rdfs:label]] "value"))))))

(deftest add-filter-test
  (testing "Existing binding"
    (let [f1 ['= "value"]
          f2 ['>= 3]
          predicate (URI. "http://p")
          qm (-> empty-model
                 (add-binding [[:key predicate]] ::qm/var)
                 (add-filter [:key] f1)
                 (add-filter [:key] f2))]
      (is (= #{f1 f2} (set (get-path-filters qm [:key]))))))

  (testing "Non-existent binding"
    (is (thrown? Exception (add-filter empty-model [:invalid :path] ['>= 3])))))

(deftest add-order-by-test
  (let [qm (-> empty-model
               (add-binding [[:dim1 (URI. "http://dim1")] [:label rdfs:label]] "label")
               (add-binding [[:dim2 (URI. "http://dim2")]] (URI. "http://value2"))
               (add-order-by {:ASC [:dim2]})
               (add-order-by {:DESC [:dim1 :label]}))]
    (is (= [{:ASC [:dim2]} {:DESC [:dim1 :label]}])
        (get-order-by qm))))

