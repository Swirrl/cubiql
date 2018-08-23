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
                   (rdf/language "Deutsch" :de)
                   (rdf/language "Francais" :fr)
                   "No langauge"]]
      (is (= "English" (find-best-language strings "en")))))

  (testing "String literal matches"
    (let [strings [(rdf/language "English" :en)
                   (rdf/language "Espanol" :es)
                   "No language"]]
      (is (= "No language" (find-best-language strings "de")))))

  (testing "No matches"
    (let [strings [(rdf/language "English" :en)
                   (rdf/language "Francais" :fr)
                   (rdf/language "Deutsch" :de)]]
      (is (nil? (find-best-language strings "es")))))

  (testing "Empty collection"
    (is (nil? (find-best-language [] "en")))))