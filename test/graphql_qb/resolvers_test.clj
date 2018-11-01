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

(deftest get-limit-test
  (are [expected requested configured-max] (= expected (get-limit {:first requested} {:max-observations-page-size configured-max}))
       ;no configured limit, no requested page size
       default-limit nil nil

       ;no requested page size, configured limit greater than default
       default-limit nil (* 2 default-limit)

       ;no requested page size, configured limit less than default
       (- default-limit 10) nil (- default-limit 10)

       ;no configured limit, requested exceeds default max
       default-max-observations-page-size (+ default-max-observations-page-size 10) nil

       ;no configured limit, requested less than default max
       (- default-max-observations-page-size 10) (- default-max-observations-page-size 10) nil

       ;less than configured limit
       9000 9000 10000

       ;more than configured limit
       10000 20000 10000

       ;requested negative
       0 -1 nil
       ))

(deftest total-count-required?-test
  (are [selections expected] (= expected (total-count-required? selections))
    ;;total_matches selected
    {:total_matches nil} true

    ;;next_page requested
    {:page {:next_page nil}} true

    ;;total_matches and next_page requested
    {:total_matches nil :page {:next_page nil}} true

    ;;total matches and next page not requested
    {:sparql nil :page {:count nil :observation {:median nil}}} false))

(deftest calculate-next-page-offset-test
  (are [offset limit total-matches expected] (= expected (calculate-next-page-offset offset limit total-matches))
       ;;unknown total-matches
       100 10 nil nil

       ;;not last page
       100 20 200 120

       ;;last page
       100 20 110 nil))