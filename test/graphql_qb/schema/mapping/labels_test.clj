(ns graphql-qb.schema.mapping.labels-test
  (:require [clojure.test :refer :all]
            [graphql-qb.schema.mapping.labels :refer :all]
            [graphql-qb.types :as types]
            [grafter.rdf :as rdf])
  (:import [java.net URI]))

(defn enum-name-value-map [enum-group]
  (into {} (map (juxt :name :value)) (:items enum-group)))

(deftest components-enum-group-test
  (let [dim1-uri (URI. "http://dim1")
        measure1-uri (URI. "http://measure1")
        components [{:uri dim1-uri
                     :label "Gender"
                     :field-name :gender}
                    {:uri measure1-uri
                     :label "Count"
                     :field-name :count}]
        components-enum-group (components-enum-group :dataset_test components)
        name->uri (into {} (map (juxt :name :value) (:items components-enum-group)))]
    (is (= :dataset_test_dimension_measures (:name components-enum-group)))
    (is (= {:GENDER dim1-uri :COUNT measure1-uri} name->uri))))

(deftest aggregation-measures-enum-group-test
  (testing "With numeric measures"
    (let [measure1-uri (URI. "http://measure1")
          measure2-uri (URI. "http://measure2")
          measure3-uri (URI. "http://measure3")
          m1 {:uri measure1-uri :label "Ratio" :is-numeric? true}
          m2 {:uri measure2-uri :label "Not numeric" :is-numeric? false}
          m3 {:uri measure3-uri :label "Count" :is-numeric? true}
          aggregation-enum (aggregation-measures-enum-group :dataset_test [m1 m2 m3])]
      (is (= :dataset_test_aggregation_measures (:name aggregation-enum)))
      (is (= {:RATIO measure1-uri :COUNT measure3-uri} (enum-name-value-map aggregation-enum)))))

  (testing "Without numeric measures"
    (let [m1 {:uri (URI. "http://measure1") :label "Measure 1" :is-numeric? false}
          m2 {:uri (URI. "http://measure2") :label "Measure 2" :is-numeric? false}]
      (is (nil? (aggregation-measures-enum-group :dataset_test [m1 m2]))))))

(deftest get-dataset-observations-result-mapping-test
  )

(deftest get-datasets-enum-mappings-test
  (let [ds1-uri (URI. "http://ds1")
        ds2-uri (URI. "http://ds2")
        dim1-uri (URI. "http://dim1")
        dim2-uri (URI. "http://dim2")
        dim3-uri (URI. "http://dim3")

        dim1-label "Dimension 1"
        dim2-label "Dimension 2"
        dim3-label "Dimension 3"

        dim1-doc "Description for dimension 1"

        dim1-val1-uri (URI. "http://dim1val1")
        dim1-val2-uri (URI. "http://dim1val2")

        dim2-val1-uri (URI. "http://dim2val1")
        dim2-val2-uri (URI. "http://dim2val2")
        dim2-val3-uri (URI. "http://dim2val3")
        dim2-val4-uri (URI. "http://dim2val4")

        dim3-val1-uri (URI. "http://dim3val1")

        bindings [{:ds ds1-uri :dim dim1-uri :member dim1-val1-uri :vallabel "First"}
                  {:ds ds1-uri :dim dim1-uri :member dim1-val2-uri :vallabel "Second"}
                  {:ds ds2-uri :dim dim2-uri :member dim2-val1-uri :vallabel "Male"}
                  {:ds ds2-uri :dim dim2-uri :member dim2-val2-uri :vallabel "Female"}
                  {:ds ds2-uri :dim dim2-uri :member dim2-val3-uri :vallabel "All"}
                  {:ds ds2-uri :dim dim2-uri :member dim2-val4-uri :vallabel "All"}
                  {:ds ds2-uri :dim dim3-uri :member dim3-val1-uri :vallabel "Label"}]

        config {}
        datasets [(types/->Dataset ds1-uri :dataset1 [(types/->Dimension dim1-uri 1 types/enum-type)] [])
                  (types/->Dataset ds2-uri :dataset2 [(types/->Dimension dim2-uri 1 types/enum-type)
                                                      (types/->Dimension dim3-uri 2 types/enum-type)] [])]
        dimension-labels {dim1-uri {:label dim1-label :doc dim1-doc}
                          dim2-uri {:label dim2-label :doc nil}
                          dim3-uri {:label dim3-label :doc ""}}
        result (get-datasets-enum-mappings datasets bindings dimension-labels config)]
    (is (= {ds1-uri {dim1-uri {:label dim1-label :doc dim1-doc :items [(types/->EnumMappingItem :FIRST dim1-val1-uri "First")
                                                                       (types/->EnumMappingItem :SECOND dim1-val2-uri "Second")]}}
            ds2-uri {dim2-uri {:label dim2-label :doc "" :items [(types/->EnumMappingItem :MALE dim2-val1-uri "Male")
                                                                 (types/->EnumMappingItem :FEMALE dim2-val2-uri "Female")
                                                                 (types/->EnumMappingItem :ALL_1 dim2-val3-uri "All")
                                                                 (types/->EnumMappingItem :ALL_2 dim2-val4-uri "All")]}
                     dim3-uri {:label dim3-label :doc "" :items [(types/->EnumMappingItem :LABEL dim3-val1-uri "Label")]}}}
           result))))

(deftest identify-dimension-labels-test
  (let [dim1-uri (URI. "http://dim1")
        dim2-uri (URI. "http://dim2")
        dim3-uri (URI. "http://dim3")

        bindings [{:dim dim1-uri :label (rdf/language "First dimension" :en) :doc nil}
                  {:dim dim1-uri :label (rdf/language "Primero dimencion" :es) :doc nil}
                  {:dim dim1-uri :label nil :doc "Dimension 1"}
                  {:dim dim2-uri :label (rdf/language "Dimension 2" :en) :doc nil}
                  {:dim dim2-uri :label nil :doc (rdf/language "Dimension two" :en)}
                  {:dim dim2-uri :label nil :doc (rdf/language "Dimencion numero dos" :es)}
                  {:dim dim3-uri :label "Dimension 3" :doc nil}]

        config {:schema-label-language "en"}]
    (is (= {dim1-uri {:label "First dimension" :doc "Dimension 1"}
            dim2-uri {:label "Dimension 2" :doc "Dimension two"}
            dim3-uri {:label "Dimension 3" :doc nil}}
           (identify-dimension-labels bindings config)))))

(deftest identify-measure-labels-test
  (let [measure1-uri (URI. "http://measure1")
        measure2-uri (URI. "http://measure2")

        bindings [{:measure measure1-uri :label (rdf/language "First measure" :en)}
                  {:measure measure1-uri :label "Second label"}
                  {:measure measure2-uri :label "Second measure"}
                  {:measure measure2-uri :label (rdf/language "Segundo measure" :es)}]

        config {:schema-label-language "en"}]
    (is (= {measure1-uri "First measure"
            measure2-uri "Second measure"}
           (identify-measure-labels bindings config)))))