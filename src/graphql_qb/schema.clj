(ns graphql-qb.schema
  (:require [graphql-qb.types :as types]
            [graphql-qb.resolvers :as resolvers]
            [graphql-qb.schema-model :as sm]
            [clojure.pprint :as pp]
            [graphql-qb.schema.mapping.labels :as mapping]))

(defn enum-type-name [dataset {:keys [enum-name] :as enum-type}]
  (types/field-name->type-name enum-name (types/dataset-schema dataset)))

(defn type-schema-type-name [dataset type]
  (cond
    (types/is-ref-area-type? type) :ref_area
    (types/is-ref-period-type? type) :ref_period
    (types/is-enum-type? type) (enum-type-name dataset type)))

(defn type-schema-input-type-name [dataset type]
  (cond
    (types/is-ref-area-type? type) :uri
    (types/is-ref-period-type? type) :ref_period_filter
    (types/is-enum-type? type) (enum-type-name dataset type)))

;;TODO: move to resolvers namespace
(defn argument-mapping-resolver [arg-mapping inner-resolver]
  (fn [context args field]
    (let [mapped-args (mapping/transform-argument arg-mapping args)]
      (inner-resolver context mapped-args field))))

(defn create-aggregation-resolver [dataset aggregation-fn aggregation-measures-enum]
  (let [arg-mapping (mapping/->MapTransform {:measure aggregation-measures-enum})
        inner-resolver (fn [context args field]
                         ;;TODO: move this measure-uri -> measure resolution function somewhere else
                         (let [updated-args (update args :measure (fn [measure-uri]
                                                                    (types/get-dataset-measure-by-uri dataset measure-uri)))]
                           (resolvers/resolve-observations-aggregation aggregation-fn context updated-args field)))]
    (argument-mapping-resolver arg-mapping inner-resolver)))

(defn create-observation-resolver [dataset dataset-enum-mappings]
  (let [arg-mapping (mapping/get-dataset-observations-argument-mapping dataset dataset-enum-mappings)]
    (argument-mapping-resolver
      arg-mapping
      (fn [context args field]
        (let [dim-filter (sm/map-dimension-filter args dataset)
              order-by (sm/get-order-by args dataset)
              updated-args (-> args
                               (assoc ::resolvers/dimensions-filter dim-filter)
                               (assoc ::resolvers/order-by order-by))]
          (resolvers/resolve-observations context updated-args field))))))

(defn wrap-observations-mapping [inner-resolver dataset dataset-enum-mappings]
  (fn [context args observations-field]
    (let [result (inner-resolver context args observations-field)
          projection (merge {:uri :obs} (types/dataset-result-projection dataset))
          result-mapping (mapping/get-dataset-observations-result-mapping dataset dataset-enum-mappings)
          mapped-result (mapv (fn [obs-bindings]
                                (let [sparql-result (types/project-result projection obs-bindings)]
                                  (mapping/transform-result result-mapping sparql-result)))
                              (::resolvers/observation-results result))]
      (assoc result :observations mapped-result))))

(defn create-aggregation-field [dataset field-name aggregation-measures-enum-mapping aggregation-fn]
  {field-name
   {:type    'Float
    :args    {:measure {:type (sm/non-null aggregation-measures-enum-mapping) :description "The measure to aggregate"}}
    :resolve (create-aggregation-resolver dataset aggregation-fn aggregation-measures-enum-mapping)}})

(defn get-aggregations-schema-model [dataset aggregation-measures-enum-mapping]
  {:type
   {:fields
    (merge
      (create-aggregation-field dataset :max aggregation-measures-enum-mapping :max)
      (create-aggregation-field dataset :min aggregation-measures-enum-mapping :min)
      (create-aggregation-field dataset :sum aggregation-measures-enum-mapping :sum)
      (create-aggregation-field dataset :average aggregation-measures-enum-mapping :avg))}})

(defn dataset-observation-dimensions-schema-model [{:keys [dimensions] :as dataset}]
  (into {} (map (fn [{:keys [field-name type] :as dim}]
                  [field-name {:type (type-schema-type-name dataset type)}])
                dimensions)))

;;TODO: combine with dataset-observation-dimensions-schema-model?
(defn dataset-observation-dimensions-input-schema-model [{:keys [dimensions] :as dataset}]
  (into {} (map (fn [{:keys [field-name type] :as dim}]
                  [field-name {:type (type-schema-input-type-name dataset type)}])
                dimensions)))

(defn dataset-observation-schema-model [dataset]
  (let [dimensions-model (dataset-observation-dimensions-schema-model dataset)
        measures (map (fn [{:keys [field-name] :as measure}]
                        [field-name {:type 'String}])
                      (:measures dataset))]
    (into {:uri {:type :uri}}
          (concat dimensions-model measures))))

(defn dataset-order-spec-schema-model [dataset]
  (let [dim-measures (types/dataset-dimension-measures dataset)]
    (into {} (map (fn [{:keys [field-name]}]
                    [field-name {:type :sort_direction}]))
          dim-measures)))

(defn get-observation-schema-model [dataset dataset-enum-mappings]
  (let [dimensions-measures-enum-mapping (mapping/dataset-dimensions-measures-enum-group dataset)
        obs-model {:type
                   {:fields
                    {:sparql
                     {:type        'String
                      :description "SPARQL query used to retrieve matching observations."
                      :resolve     :resolve-observation-sparql-query}
                     :page
                     {:type
                      {:fields
                       {:next_page    {:type :SparqlCursor :description "Cursor to the next page of results"}
                        :count        {:type 'Int}
                        :observations {:type [{:fields (dataset-observation-schema-model dataset)}] :description "List of observations on this page"}}}
                      :args        {:after {:type :SparqlCursor}
                                    :first {:type 'Int}}
                      :description "Page of results to retrieve."
                      :resolve     (wrap-observations-mapping resolvers/resolve-observations-page dataset dataset-enum-mappings)}
                     :total_matches {:type 'Int}}}
                   :args
                   {:dimensions {:type {:fields (dataset-observation-dimensions-input-schema-model dataset)}}
                    :order      {:type [dimensions-measures-enum-mapping]}
                    :order_spec {:type {:fields (dataset-order-spec-schema-model dataset)}}}
                   :resolve (create-observation-resolver dataset dataset-enum-mappings)}
        aggregation-measures-enum-mapping (mapping/dataset-aggregation-measures-enum-group dataset)]
    (if (nil? aggregation-measures-enum-mapping)
      obs-model
      (let [aggregation-fields (get-aggregations-schema-model dataset aggregation-measures-enum-mapping)]
        (assoc-in obs-model [:type :fields :aggregations] aggregation-fields)))))

(defn get-query-schema-model [{:keys [description] :as dataset} dataset-enum-mappings]
  (let [schema-name (types/dataset-schema dataset)
        observations-model (get-observation-schema-model dataset dataset-enum-mappings)]
    {schema-name
     {:type
      {:implements  [:dataset_meta]
       :fields      {:uri          {:type :uri :description "Dataset URI"}
                     :title        {:type 'String :description "Dataset title"}
                     :description  {:type 'String :description "Dataset description"}
                     :licence      {:type :uri :description "URI of the licence the dataset is published under"}
                     :issued       {:type :DateTime :description "When the dataset was issued"}
                     :modified     {:type :DateTime :description "When the dataset was last modified"}
                     :publisher    {:type :uri :description "URI of the publisher of the dataset"}
                     :schema       {:type 'String :description "Name of the GraphQL query root field corresponding to this dataset"}
                     :dimensions   {:type        [:dim]
                                    :resolve     (fn [context args _field]
                                                   (resolvers/resolve-dataset-dimensions context args dataset))
                                    :description "Dimensions within the dataset"}
                     :measures     {:type        [:measure]
                                    :description "Measure types within the dataset"}
                     :observations observations-model}
       :description (or description "")}
      :resolve (fn [context args field]
                 (resolvers/resolve-dataset context dataset))}}))

(defn get-dataset-schema [dataset dataset-enum-mapping]
  (let [ds-enums-schema (mapping/dataset-enum-types-schema dataset dataset-enum-mapping)
        enums-schema {:enums ds-enums-schema}

        query-model (get-query-schema-model dataset dataset-enum-mapping)
        query-schema (sm/visit-queries query-model)]
    (-> query-schema
        (sm/merge-schemas enums-schema))))
