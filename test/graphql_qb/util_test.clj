(ns graphql-qb.util-test
  (:require [clojure.test :refer :all]
            [graphql-qb.util :refer :all]
            [grafter.rdf :as rdf]))

(deftest rename-key-test
  (testing "Key exists"
    (is (= {:new-key 1} (rename-key {:old-key 1} :old-key :new-key))))

  (testing "Key missing strict"
    (is (thrown? IllegalArgumentException (rename-key {:k 1} :old-key :new-key :strict? true))))

  (testing "Key missing non-strict"
    (let [m {:k 1}]
      (is (= m (rename-key m :old-key :new-key))))))

(deftest find-best-language-test
  (testing "Exact language match"
    (let [strings [(rdf/language "English" :en)
                   (rdf/language "Deutsche" :de)
                   (rdf/language "Francais" :fr)
                   "No langauge"]]
      (is (= "English" (find-best-language strings "en")))))

  (testing "English match"
    (let [strings [(rdf/language "English" :en)
                   (rdf/language "Deutsche" :de)
                   (rdf/language "Espanol" :es)]]
      (is (= "English" (find-best-language strings "fr")))))

  (testing "String literal matches"
    (let [strings [(rdf/language "Francais" :fr)
                   (rdf/language "Espanol" :es)
                   "No language"]]
      (is (= "No language" (find-best-language strings "de")))))

  (testing "No matches"
    (let [strings [(rdf/language "Espanol" :es)
                   (rdf/language "Francais" :fr)
                   (rdf/language "Deutsche" :de)]]
      (is (nil? (find-best-language strings "jp")))))

  (testing "Empty collection"
    (is (nil? (find-best-language [] "en")))))

(deftest strict-get-test
  (testing "Key exists"
    (is (= 1 (strict-get {:a 1 :b 2} :a))))

  (testing "Key does not exist"
    (is (thrown? Exception (strict-get {:a 1 :b 2} :missing))))

  (testing "Key does not exist with description"
    (is (thrown-with-msg? Exception #"Widget :missing not found" (strict-get {:a 1 :b 2} :missing :key-desc "Widget")))))

(deftest strict-map-by-test
  (testing "Unique keys"
    (let [index->item (fn [i] {:key i :value (str "value" i)})
          items (map index->item (range 1 11))
          expected (into {} (map (fn [v] [(:key v) v]) items))]
      (is (= expected (strict-map-by :key items)))))

  (testing "Duplicate keys"
    (let [items [{:key 1 :value "value1"}
                 {:key 2 :value "value2"}
                 {:key 1 :value "value3"}]]
      (is (thrown? Exception (strict-map-by :key items))))))

(deftest to-multimap-test
  (let [maps [{:a 1 :b 2}
              {:a 3 :b nil :c 4}
              {:a 5 :d 6}
              {:a nil :b 7 :c 8 :d 9}
              {}
              {:c 10}]]
    (is (= {:a [1 3 5]
            :b [2 7]
            :c [4 8 10]
            :d [6 9]}
           (to-multimap maps)))))

(deftest xmls-boolean->boolean-test
  (are [b expected] (= expected (xmls-boolean->boolean b))
    false false
    true true
    0 false
    0N false
    1 true
    1N true
    nil false))