(ns graphql-qb.resolvers-test
  (:require [clojure.test :refer :all]
            [graphql-qb.resolvers :refer :all])
  (:import [java.net URI]))

(deftest combine-dimension-results-test
  (let [dim1-uri (URI. "http://dim/gender")
        dim2-uri (URI. "http://dim2")
        dim3-uri (URI. "http://dim3")

        dim1-member1-uri (URI. "http://male")
        dim1-member2-uri (URI. "http://female")
        dim3-member1-uri (URI. "http://dim3mem1")

        codelist-members [{:dim dim1-uri :member dim1-member1-uri :label "Male"}
                          {:dim dim3-uri :member dim3-member1-uri :label "Member"}
                          {:dim dim1-uri :member dim1-member2-uri :label "Female"}]
        dimension-results [{:uri dim1-uri :label "Gender"}
                           {:uri dim2-uri}
                           {:uri dim3-uri :label "Dimension 3"}]]

    (is (= [{:uri dim1-uri :label "Gender" :values [{:uri dim1-member1-uri :label "Male"}
                                                    {:uri dim1-member2-uri :label "Female"}]}
            {:uri dim2-uri}
            {:uri dim3-uri :label "Dimension 3" :values [{:uri dim3-member1-uri :label "Member"}]}]
           (combine-dimension-results dimension-results
                                      codelist-members)))))


