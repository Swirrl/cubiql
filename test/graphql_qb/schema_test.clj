(ns graphql-qb.schema-test
  (:require [clojure.test :refer :all]
            [graphql-qb.schema :refer :all]
            [graphql-qb.types :as types]
            [com.walmartlabs.lacinia.schema :as ls])
  (:import [java.net URI]))

(deftest dataset-observation-schema-model-test
  (let [dim1 {:field-name :dim1 :type types/string-type}
        dim2 {:field-name :dim2 :type types/decimal-type}
        measure1 {:field-name :measure1 :type (types/->FloatMeasureType)}
        measure2 {:field-name :measure2 :type (types/->StringMeasureType)}
        dsm {:uri (URI. "http://test")
             :schema :dataset_test
             :dimensions [dim1 dim2]
             :measures [measure1 measure2]}
        schema-model (dataset-observation-schema-model dsm)]
    (is (= {:uri {:type :uri}
            :dim1 {:type 'String}
            :dim2 {:type 'Float}
            :measure1 {:type 'Float}
            :measure2 {:type 'String}}
           schema-model))))

(deftest dataset-observation-dimensions-input-schema-model-test
  (let [dim1 {:field-name :dim1 :type types/string-type}
        dim2 {:field-name :dim2 :type types/decimal-type}
        dim3 {:field-name :dim3 :type (types/->MappedEnumType :enum-name types/enum-type nil [])}
        dsm {:uri (URI. "http://test")
             :schema :dataset_test
             :dimensions [dim1 dim2 dim3]
             :measures []}
        schema-model (dataset-observation-dimensions-input-schema-model dsm)]
    (is (= {:dim1 {:type 'String}
            :dim2 {:type 'Float}
            :dim3 {:type :enum-name}}
           schema-model))))

(deftest get-order-by-test
  (let [dim1-uri (URI. "http://dim1")
        dim2-uri (URI. "http://dim2")
        dim3-uri (URI. "http://dim3")
        measure1-uri (URI. "http://measure1")
        dim1 (types/->Dimension dim1-uri 1 (types/->DecimalType))
        dim2 (types/->Dimension dim2-uri 2 (types/->StringType))
        dim3 (types/->Dimension dim3-uri 3 types/enum-type)
        measure1 (types/->MeasureType measure1-uri 1 true)

        dim1-mapping {:uri dim1-uri :field-name :dim1 :dimension dim1}
        dim2-mapping {:uri dim2-uri :field-name :dim2 :dimension dim2}
        dim3-mapping {:uri dim3-uri :field-name :dim3 :dimension dim3}
        measure1-mapping {:uri measure1-uri :field-name :measure1 :measure measure1}

        dsm {:uri (URI. "http://test")
             :schema :dataset_test
             :dimensions [dim1-mapping dim2-mapping dim3-mapping]
             :measures [measure1-mapping]}
        order [dim1-uri measure1-uri dim2-uri]
        order-spec {measure1-uri :ASC dim2-uri :DESC}]
    (is (= [[dim1 :ASC] [measure1 :ASC] [dim2 :DESC]]
           (get-order-by {:order order :order_spec order-spec} dsm)))))

(deftest map-dimension-filter-test
  (let [dim1-uri (URI. "http://dim1")
        dim2-uri (URI. "http://dim2")
        dim3-uri (URI. "http://dim3")
        dim4-uri (URI. "http://dim4")

        dim2-value (URI. "http://value2")

        dim1 (types/->Dimension dim1-uri 1 types/decimal-type)
        dim2 (types/->Dimension dim2-uri 2 types/enum-type)
        dim3 (types/->Dimension dim3-uri 3 types/string-type)
        dim4 (types/->Dimension dim4-uri 4 types/decimal-type)

        dsm {:uri (URI. "http://test")
             :schema :dataset_test
             :dimensions [{:uri dim1-uri :dimension dim1}
                          {:uri dim2-uri :dimension dim2}
                          {:uri dim3-uri :dimension dim3}
                          {:uri dim4-uri :dimension dim4}]
             :measures []}
        dimension-args {dim1-uri 5
                        dim2-uri dim2-value
                        dim3-uri "value1"}]
    (is (= {dim1 5
            dim2 (URI. "http://value2")
            dim3 "value1"
            dim4 nil}
           (map-dimension-filter dimension-args dsm)))))

(deftest map-dataset-observation-args-test
  (let [dim1-uri (URI. "http://dim1")
        dim2-uri (URI. "http://dim2")
        dim3-uri (URI. "http://dim3")
        measure1-uri (URI. "http://measure1")

        dim2-value1 (URI. "http://value1")
        dim2-value2 (URI. "http://value2")
        dim2-mapped-type (types/->MappedEnumType :enum2 types/enum-type "" [(types/->EnumMappingItem :VALUE1 dim2-value1 "Value 1")
                                                                            (types/->EnumMappingItem :VALUE2 dim2-value2 "Value 2")])
        dsm {:uri        (URI. "http://test")
             :schema     :dataset_test
             :dimensions [{:uri dim1-uri :field-name :dim1 :type types/decimal-type :enum-name :DIM1}
                          {:uri dim2-uri :field-name :dim2 :type dim2-mapped-type :enum-name :DIM2}
                          {:uri dim3-uri :field-name :dim3 :type types/string-type :enum-name :DIM3}]
             :measures   [{:uri measure1-uri :field-name :measure1 :type (types/->FloatMeasureType) :enum-name :MEASURE1}]}
        args {:dimensions {:dim1 4 :dim2 :VALUE2 :dim3 "value"}
              :order [:MEASURE1 :DIM1 :DIM3]
              :order_spec {:dim1 :ASC :dim3 :DESC :measure1 :DESC}}
        expected {:dimensions {dim1-uri 4 dim2-uri dim2-value2 dim3-uri "value"}
                  :order      [measure1-uri dim1-uri dim3-uri]
                  :order_spec {dim1-uri :ASC dim3-uri :DESC measure1-uri :DESC}}]
    (is (= expected (map-dataset-observation-args args dsm)))))

(deftest map-dataset-measure-results-test
  (let [measure1-uri (URI. "http://measure1")
        measure2-uri (URI. "http://measure2")
        measure1-label "First measure"
        measure2-label "Second measure"
        measure1-name :MEASURE1
        measure2-name :OTHER_NAME

        measure1 {:uri measure1-uri :enum-name measure1-name}
        measure2 {:uri measure2-uri :enum-name measure2-name}

        results [{:uri measure1-uri :label measure1-label}
                 {:uri measure2-uri :label measure2-label}]

        dsm {:uri (URI. "http://test-dataset")
             :schema :dataset_test
             :dimensions []
             :measures [measure1 measure2]}]
    (is (= [{:uri measure1-uri :label measure1-label :enum_name (name measure1-name)}
            {:uri measure2-uri :label measure2-label :enum_name (name measure2-name)}]
           (map-dataset-measure-results dsm results)))))

(deftest annotate-dataset-dimensions-test
  (let [dim1-uri (URI. "http://dim1")
        dim2-uri (URI. "http://dim2")
        dim3-uri (URI. "http://dim3")

        dim1-val1 {:uri (URI. "http://dim1val1") :label "Value 1"}
        dim1-val2 {:uri (URI. "http://dim1val2") :label "Value 2"}
        dim1-result {:uri dim1-uri :values [dim1-val1 dim1-val2]}
        dim2-result {:uri dim2-uri :values nil}

        dim3-val1-uri (URI. "http://dim3val1")
        dim3-val2-uri (URI. "http://dim3val2")
        dim3-val3-uri (URI. "http://dim3val3")
        dim3-result {:uri dim3-uri :values [{:uri dim3-val1-uri :label "Value 1"}
                                            {:uri dim3-val2-uri :label "Value 2"}
                                            {:uri dim3-val3-uri :label "Value 3"}]}

        dsm {:uri (URI. "http://test-dataset")
             :schema :dataset_test
             :dimensions [{:uri dim1-uri :type types/ref-area-type :enum-name :DIM1}
                          {:uri dim2-uri :type types/decimal-type :enum-name :DIM2}
                          {:uri dim3-uri
                           :type (types/->MappedEnumType :dim3 types/enum-type "" [(types/->EnumMappingItem :VALUE1 dim3-val1-uri "Value 1")
                                                                                    (types/->EnumMappingItem :VALUE3 dim3-val3-uri "Value 3")
                                                                                    (types/->EnumMappingItem :VALUE2 dim3-val2-uri "Value 2")])
                           :enum-name :DIM3}]}

        result (annotate-dataset-dimensions dsm [dim1-result dim2-result dim3-result])]
    (is (= [{:uri dim1-uri :enum_name "DIM1" :values [(ls/tag-with-type dim1-val1 :unmapped_dim_value)
                                                      (ls/tag-with-type dim1-val2 :unmapped_dim_value)]}
            {:uri dim2-uri :enum_name "DIM2" :values nil}
            {:uri dim3-uri :enum_name "DIM3" :values [(ls/tag-with-type
                                                        {:uri dim3-val1-uri :label "Value 1" :enum_name "VALUE1"} :enum_dim_value)
                                                      (ls/tag-with-type
                                                        {:uri dim3-val2-uri :label "Value 2" :enum_name "VALUE2"} :enum_dim_value)
                                                      (ls/tag-with-type
                                                        {:uri dim3-val3-uri :label "Value 3" :enum_name "VALUE3"} :enum_dim_value)]}]
           result))))

(deftest dataset-order-spec-schema-model-test
  (let [dsm {:uri (URI. "http://test-dataset")
             :schema :dataset_test
             :dimensions [{:field-name :gender}
                          {:field-name :area}]
             :measures [{:field-name :median}
                        {:field-name :count}]}]
    (is (= {:gender {:type :sort_direction}
            :area {:type :sort_direction}
            :median {:type :sort_direction}
            :count {:type :sort_direction}})
        (dataset-order-spec-schema-model dsm))))

(deftest map-observation-selections-test
  (let [dim1-uri (URI. "http://dim1")
        dim2-uri (URI. "http://dim2")
        dim3-uri (URI. "http://dim3")
        dim4-uri (URI. "http://dim4")
        measure1-uri (URI. "http://measure1")
        measure2-uri (URI. "http://measure2")

        dsm {:uri (URI. "http://test-dataset")
             :schema :dataset_test
             :dimensions [{:uri dim1-uri :type types/ref-area-type :field-name :dim1}
                          {:uri dim2-uri :type types/ref-period-type :field-name :dim2}
                          {:uri dim3-uri :type types/enum-type :field-name :dim3}
                          {:uri dim4-uri :type types/decimal-type :field-name :dim4}]
             :measures [{:uri measure1-uri :type (types/->FloatMeasureType) :field-name :measure1}
                        {:uri measure2-uri :type (types/->StringMeasureType) :field-name :measure2}]}

        dim1-selections {:label nil :uri nil}
        dim2-selections {:uri nil :start nil}
        selections {:uri nil
                    :dim1 dim1-selections
                    :dim2 dim2-selections
                    :dim3 nil
                    :measure2 nil}]
    (is (= {dim1-uri dim1-selections
            dim2-uri dim2-selections
            dim3-uri nil
            measure2-uri nil}
           (map-observation-selections dsm selections)))))

(deftest dataset-enum-types-schema-test
  (let [dim1 {:uri  (URI. "http://dim1")
              :type types/string-type}
        dim2 {:uri (URI. "http://dim2")
              :type (types/->MappedEnumType :enum1 types/enum-type "description" [(types/->EnumMappingItem :VALUE1 (URI. "http://val1") "value1")
                                                                                  (types/->EnumMappingItem :VALUE2 (URI. "http://val2") "value2")])}
        dim3 {:uri  (URI. "http://dim3")
              :type types/decimal-type}
        dim4 {:uri (URI. "http://dim4")
              :type (types/->MappedEnumType :enum2 types/enum-type nil [(types/->EnumMappingItem :VALUE3 (URI. "http://val3") "value3")])}
        dsm {:uri (URI. "http://test")
             :schema :dataset_test
             :dimensions [dim1 dim2 dim3 dim4]
             :measures []}
        enums-schema (dataset-enum-types-schema dsm)]
    (is (= {:enum1 {:values [:VALUE1 :VALUE2] :description "description"}
            :enum2 {:values [:VALUE3]}}
           enums-schema))))