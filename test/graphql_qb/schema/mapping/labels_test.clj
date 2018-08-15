(ns graphql-qb.schema.mapping.labels-test
  (:require [clojure.test :refer :all]
            [graphql-qb.schema.mapping.labels :refer :all]
            [graphql-qb.types :as types])
  (:import [java.net URI]))

(defn enum-name-value-map [enum-group]
  (into {} (map (juxt :name :value)) (:items enum-group)))

(deftest dataset-dimensions-measures-enum-group-test
  (let [dim1-uri (URI. "http://dim1")
        measure1-uri (URI. "http://measure1")
        dataset-mapping {:uri        (URI. "http://test")
                         :schema     :dataset_test
                         :dimensions [{:uri dim1-uri
                                       :label "Gender"
                                       :field-name :gender}]
                         :measures [{:uri measure1-uri
                                     :label "Count"
                                     :field-name :count}]}
        components-enum-group (dataset-dimensions-measures-enum-group dataset-mapping)
        name->uri (into {} (map (juxt :name :value) (:items components-enum-group)))]
    (= :dataset_test_dimension_measures (:name components-enum-group))
    (= {:GENDER dim1-uri :COUNT measure1-uri} name->uri)))

(deftest dataset-aggregation-measures-enum-group-test
  (testing "With numeric measures"
    (let [measure1-uri (URI. "http://measure1")
          measure2-uri (URI. "http://measure2")
          measure3-uri (URI. "http://measure3")
          m1 {:uri measure1-uri :label "Ratio" :is-numeric? true}
          m2 {:uri measure2-uri :label "Not numeric" :is-numeric? false}
          m3 {:uri measure3-uri :label "Count" :is-numeric? true}
          dsm {:uri (URI. "http://test")
               :schema :dataset_test
               :dimensions []
               :measures [m1 m2 m3]}
          aggregation-enum (dataset-aggregation-measures-enum-group dsm)]
      (= :dataset_test_aggregation_measures (:name aggregation-enum))
      (= {:RATIO measure1-uri :COUNT measure3-uri} (enum-name-value-map aggregation-enum))))

  (testing "Without numeric measures"
    (let [m1 {:uri (URI. "http://measure1") :label "Measure 1" :is-numeric? false}
          m2 {:uri (URI. "http://measure2") :label "Measure 2" :is-numeric? false}
          dsm {:uri (URI. "http://test")
               :schema :dataset_test
               :dimensions []
               :measures [m1 m2]}]
      (is (nil? (dataset-aggregation-measures-enum-group dsm))))))

(deftest get-dataset-observations-result-mapping-test
  )

(deftest dataset-enum-types-schema-test
  (let [dim1 {:uri  (URI. "http://dim1")
              :type types/string-type}
        dim2 {:uri (URI. "http://dim2")
              :type (->MappedEnumType :enum1 types/enum-type "description" [(->EnumMappingItem :VALUE1 (URI. "http://val1") "value1")
                                                                            (->EnumMappingItem :VALUE2 (URI. "http://val2") "value2")])}
        dim3 {:uri  (URI. "http://dim3")
              :type types/decimal-type}
        dim4 {:uri (URI. "http://dim4")
              :type (->MappedEnumType :enum2 types/enum-type nil [(->EnumMappingItem :VALUE3 (URI. "http://val3") "value3")])}
        dsm {:uri (URI. "http://test")
             :schema :dataset_test
             :dimensions [dim1 dim2 dim3 dim4]
             :measures []}
        enums-schema (dataset-enum-types-schema dsm)]
    (is (= {:enum1 {:values [:VALUE1 :VALUE2] :description "description"}
            :enum2 {:values [:VALUE3]}}
           enums-schema))))
