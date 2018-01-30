(ns graphql-qb.schema
  (:require [graphql-qb.types :as types]
            [graphql-qb.resolvers :as resolvers]
            [graphql-qb.schema-model :as sm]
            [clojure.pprint :as pp]
            [graphql-qb.schema.mapping.labels :as mapping]))

(def observation-uri-schema-mapping
  {:type :uri
   :description "URI of the observation"
   :->graphql identity})

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

(defn create-aggregation-resolver [aggregation-fn aggregation-measures-enum]
  (fn [context args field]
    (let [resolved-args (update args :measure #(types/from-graphql aggregation-measures-enum %))]
      (resolvers/resolve-observations-aggregation aggregation-fn context resolved-args field))))

(defn create-observation-resolver [dataset]
  (fn [context args field]
    (let [mapped-args ((sm/observation-args-mapper dataset) args)]
      (resolvers/resolve-observations context mapped-args field))))

(defn dimension-type-name [dataset dim]
  (type-schema-type-name dataset (:type dim)))

(defn dimension->schema-mapping [dataset {:keys [field-name type doc label] :as dim}]
  (let [->graphql (if (types/is-enum-type? type)
                    (fn [v] (types/to-graphql dim v))
                    identity)]
    {field-name {:type (dimension-type-name dataset dim) :->graphql ->graphql :description (some-> (or doc label) str)}}))

(defn measure->schema-mapping [{:keys [field-name] :as measure}]
  ;;TODO: get measure description?
  {field-name {:type 'String :->graphql str :description ""}})

(defn get-dataset-schema-mapping [{:keys [dimensions measures] :as ds}]
  (let [dim-mappings (map #(dimension->schema-mapping ds %) dimensions)
        measure-mappings (map measure->schema-mapping measures)]
    (into {:uri observation-uri-schema-mapping} (concat dim-mappings measure-mappings))))

(defn apply-schema-mapping [mapping sparql-result]
  (into {} (map (fn [[field-name {:keys [->graphql]}]]
                  [field-name (->graphql (get sparql-result field-name))])
                mapping)))

(defn wrap-observations-mapping [inner-resolver dataset]
  (fn [context args observations-field]
    (let [result (inner-resolver context args observations-field)
          projection (merge {:uri :obs} (types/dataset-result-projection dataset))
          schema-mapping (get-dataset-schema-mapping dataset)
          mapped-result (mapv (fn [obs-bindings]
                                (let [sparql-result (types/project-result projection obs-bindings)]
                                  (apply-schema-mapping schema-mapping sparql-result)))
                              (::resolvers/observation-results result))]
      (assoc result :observations mapped-result))))

(defn get-aggregations-schema-model [aggregation-measures-type-name aggregation-measures-enum]
  {:type
   {:fields
    {:max
     {:type    'Float
      :args    {:measure {:type (sm/non-null aggregation-measures-type-name) :description "The measure to aggregate"}}
      :resolve (create-aggregation-resolver :max aggregation-measures-enum)}
     :min
     {:type    'Float
      :args    {:measure {:type (sm/non-null aggregation-measures-type-name) :description "The measure to aggregate"}}
      :resolve (create-aggregation-resolver :min aggregation-measures-enum)}
     :sum
     {:type    'Float
      :args    {:measure {:type (sm/non-null aggregation-measures-type-name) :description "The measure to aggregate"}}
      :resolve (create-aggregation-resolver :max aggregation-measures-enum)}
     :average
     {:type    'Float
      :args    {:measure {:type (sm/non-null aggregation-measures-type-name) :description "The measure to aggregate"}}
      :resolve (create-aggregation-resolver :max aggregation-measures-enum)}}}})

(defn get-aggregation-measures-enum [dataset]
  (let [aggregation-measures (types/dataset-aggregate-measures dataset)]
    (if-not (empty? aggregation-measures)
      (types/build-enum :aggregation_measures aggregation-measures))))

(defn merge-aggregations-schema-model [dataset observations-model aggregation-measures-type-name aggregation-measures-type]
  (if (some? aggregation-measures-type)
    (let [aggregation-fields (get-aggregations-schema-model aggregation-measures-type-name aggregation-measures-type)]
      (assoc-in observations-model [:type :fields :aggregations] aggregation-fields))
    observations-model))

(defn merge-schemas [s1 s2]
  (merge-with (fn [v1 v2]
                (if (and (map? v1) (map? v2))
                  (merge-schemas v1 v2)
                  v2))
              s1 s2))

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

(defn get-observation-schema-model [dataset dimensions-measures-enum-name]
  {:type
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
      :resolve     (wrap-observations-mapping resolvers/resolve-observations-page dataset)}
     :total_matches {:type 'Int}}}
   :args
   {:dimensions {:type {:fields (dataset-observation-dimensions-input-schema-model dataset)}}
    :order      {:type [dimensions-measures-enum-name]}
    :order_spec {:type {:fields (dataset-order-spec-schema-model dataset)}}}
   :resolve (create-observation-resolver dataset)})

(defn get-query-schema-model [{:keys [description] :as dataset} dataset-enum-mappings dimensions-measures-enum-name aggregation-measures-type-name aggregation-measures-type]
  (let [schema-name (types/dataset-schema dataset)
        observations-model (get-observation-schema-model dataset dimensions-measures-enum-name)
        observations-model (merge-aggregations-schema-model dataset observations-model aggregation-measures-type-name aggregation-measures-type)]
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
        dimensions-measures-enum (mapping/dataset-dimensions-measures-enum-group dataset)
        dimensions-measures-enum-schema (mapping/enum-mapping->schema dataset :dimension_measures dimensions-measures-enum)
        dimensions-measures-enum-name (mapping/enum-type-name dataset :dimension_measures)

        aggregation-measures-type-name (mapping/enum-type-name dataset :aggregation_measures)
        aggregation-measures-type (get-aggregation-measures-enum dataset)
        aggregation-measures-type-schema (if (some? aggregation-measures-type)
                                           {aggregation-measures-type-name
                                            {:values (mapv :name (:values aggregation-measures-type))}})

        enums-schema {:enums (merge ds-enums-schema dimensions-measures-enum-schema aggregation-measures-type-schema)}

        query-model (get-query-schema-model dataset dataset-enum-mapping dimensions-measures-enum-name aggregation-measures-type-name aggregation-measures-type)
        query-schema (sm/visit-queries query-model)]
    (-> query-schema
        (sm/merge-schemas enums-schema))))
