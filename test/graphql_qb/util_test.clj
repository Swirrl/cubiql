(ns graphql-qb.util-test
  (:require [clojure.test :refer :all]
            [graphql-qb.util :refer :all]))

(deftest rename-key-test
  (testing "Key exists"
    (is (= {:new-key 1} (rename-key {:old-key 1} :old-key :new-key))))

  (testing "Key missing strict"
    (is (thrown? IllegalArgumentException (rename-key {:k 1} :old-key :new-key :strict? true))))

  (testing "Key missing non-strict"
    (let [m {:k 1}]
      (is (= m (rename-key m :old-key :new-key))))))