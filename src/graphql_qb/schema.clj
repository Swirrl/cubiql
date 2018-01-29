(ns graphql-qb.schema
  (:require [graphql-qb.types :as types]
            [graphql-qb.resolvers :as resolvers]
            [graphql-qb.schema-model :as sm]
            [clojure.pprint :as pp]
            [graphql-qb.util :as util]
            [graphql-qb.schema.mapping.labels :as mapping]))

(def observation-uri-schema-mapping
  {:type :uri
   :description "URI of the observation"
   :->graphql identity})

(defn get-enum-names [{:keys [values] :as enum-type}]
  {:pre [(types/is-enum-type? enum-type)]}
  (mapv :name values))

(defn enum-type-name [dataset {:keys [enum-name] :as enum-type}]
  (types/field-name->type-name enum-name (types/dataset-schema dataset)))

(defn enum->schema [dataset enum-type]
  {(enum-type-name dataset enum-type) {:values (get-enum-names enum-type)}})

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

(defn dimension-input-schema [dataset {:keys [field-name doc label type] :as dim}]
  {field-name {:type (type-schema-input-type-name dataset type)
               :description (some-> (or doc label) str)}})

(defn dataset-observation-filter-schema [{:keys [dimensions] :as dataset}]
  (apply merge (map #(dimension-input-schema dataset %) dimensions)))

(defn dataset-resolver-name [dataset]
  (let [schema (types/dataset-schema dataset)]
    (keyword (str "resolve_" (name schema)))))

(defn dataset-dimensions-resolver-name [dataset]
  (let [schema (types/dataset-schema dataset)]
    (keyword (str "resolve_" (name schema) "_dimensions"))))

(defn dataset-observations-resolver-name [dataset]
  (let [schema (types/dataset-schema dataset)]
    (keyword (str "resolve_" (name schema) "_observations"))))

(defn dataset-observations-page-resolver-name [dataset]
  (let [schema (types/dataset-schema dataset)]
    (keyword (str "resolve_" (name schema) "_observations_page"))))

(defn aggregation-resolver-name [dataset aggregation-fn]
  (let [schema (types/dataset-schema dataset)]
    (keyword (str "resolve_" (name schema) "_observations_aggregation_" (name aggregation-fn)))))

(defn get-dataset-sort-specification-schema [dataset]
  (into {} (map (fn [m] [(types/->field-name m)
                         {:type :sort_direction}])
                (types/dataset-dimension-measures dataset))))

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

(defn schema-mapping->observations-schema [mapping]
  (util/map-values (fn [m] (select-keys m [:type :description])) mapping))

(defn dataset-observation-schema [dataset]
  (-> dataset
      (get-dataset-schema-mapping)
      (schema-mapping->observations-schema)))

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

(defn add-enum-schema [schema dataset enum]
  (let [enum-schema (enum->schema dataset enum)]
    (update schema :enums #(merge % enum-schema))))

(defn merge-aggregations-schema [partial-schema dataset]
  (let [aggregation-measures (types/dataset-aggregate-measures dataset)]
    (if-not (empty? aggregation-measures)
      (let [schema (types/dataset-schema dataset)
            aggregation-fields-type-name (types/field-name->type-name :aggregations schema)
            aggregation-types-type-name (types/field-name->type-name :aggregation_measure_types schema)
            observation-result-type-name (types/field-name->type-name :observation_result schema)
            aggregation-measures-enum (types/build-enum :aggregation_measure_types aggregation-measures)]
        (-> partial-schema
            (assoc-in [:objects aggregation-fields-type-name]
                      {:fields
                       {:max     {:type    'Float
                                  :args    {:measure {:type (list 'non-null aggregation-types-type-name) :description "The measure to aggregate"}}
                                  :resolve (aggregation-resolver-name dataset :max)}
                        :min     {:type    'Float
                                  :args    {:measure {:type (list 'non-null aggregation-types-type-name) :description "The measure to aggregate"}}
                                  :resolve (aggregation-resolver-name dataset :min)}
                        :sum     {:type    'Float
                                  :args    {:measure {:type (list 'non-null aggregation-types-type-name) :description "The measure to aggregate"}}
                                  :resolve (aggregation-resolver-name dataset :sum)}
                        :average {:type    'Float
                                  :args    {:measure {:type (list 'non-null aggregation-types-type-name) :description "The measure to aggregate"}}
                                  :resolve (aggregation-resolver-name dataset :avg)}}})
            (assoc-in [:objects observation-result-type-name :fields :aggregations]
                      {:type aggregation-fields-type-name})
            (add-enum-schema dataset aggregation-measures-enum)
            (assoc-in [:resolvers (aggregation-resolver-name dataset :max)] (create-aggregation-resolver :max aggregation-measures-enum))
            (assoc-in [:resolvers (aggregation-resolver-name dataset :min)] (create-aggregation-resolver :min aggregation-measures-enum))
            (assoc-in [:resolvers (aggregation-resolver-name dataset :sum)] (create-aggregation-resolver :sum aggregation-measures-enum))
            (assoc-in [:resolvers (aggregation-resolver-name dataset :avg)] (create-aggregation-resolver :avg aggregation-measures-enum))))
      partial-schema)))

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
  {:fields
   {:observations
    {:type
     {:fields
      {:sparql        {:type        'String
                       :description "SPARQL query used to retrieve matching observations."
                       :resolve     :resolve-observation-sparql-query}
       :page          {:type
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
     {:dimensions {:type {:fields (dataset-observation-dimensions-schema-model dataset)}}
      :order      {:type [dimensions-measures-enum-name]}
      :order_spec {:type {:fields (dataset-order-spec-schema-model dataset)}}}}}})

(defn merge-observations-schema [partial-schema dataset]
  (let [schema (types/dataset-schema dataset)
        observation-type-name (types/field-name->type-name :observation schema)
        observation-result-type-name (types/field-name->type-name :observation_result schema)
        observation-filter-type-name (types/field-name->type-name :observation_dimensions schema)
        observation-ordering-fields-enum (types/build-enum :observation_ordering (types/dataset-dimension-measures dataset))
        observation-ordering-fields-enum-name (enum-type-name dataset observation-ordering-fields-enum)
        field-orderings-type-name (types/field-name->type-name :field_ordering schema)
        observations-resolver-name (dataset-observations-resolver-name dataset)
        observations-page-resolver-name (dataset-observations-page-resolver-name dataset)
        observation-results-page-type-name (types/field-name->type-name :observation_results_page schema)]
    (-> partial-schema
        (assoc-in [:objects schema :fields :observations]
                  {:type        observation-result-type-name
                   :args        {:dimensions {:type observation-filter-type-name}
                                 :order      {:type (list 'list observation-ordering-fields-enum-name)}
                                 :order_spec {:type field-orderings-type-name}}
                   :resolve     observations-resolver-name
                   :description "Observations matching the given criteria"})
        (assoc-in [:objects observation-result-type-name]
                  {:fields
                   {:sparql        {:type        'String
                                    :description "SPARQL query used to retrieve matching observations."
                                    :resolve     :resolve-observation-sparql-query}
                    :page          {:type        observation-results-page-type-name
                                    :args        {:after {:type :SparqlCursor}
                                                  :first {:type 'Int}}
                                    :description "Page of results to retrieve."
                                    :resolve     observations-page-resolver-name}
                    :total_matches {:type 'Int}}})
        (assoc-in [:objects observation-results-page-type-name]
                  {:fields
                   {:next_page    {:type :SparqlCursor :description "Cursor to the next page of results"}
                    :count        {:type 'Int}
                    :observations {:type (list 'list observation-type-name) :description "List of observations on this page"}}})
        (assoc-in [:objects observation-type-name]
                  {:fields (dataset-observation-schema dataset)})
        (add-enum-schema dataset observation-ordering-fields-enum)
        (assoc-in [:input-objects observation-filter-type-name] {:fields (dataset-observation-filter-schema dataset)})
        (assoc-in [:input-objects field-orderings-type-name] {:fields (get-dataset-sort-specification-schema dataset)})
        (assoc-in [:resolvers observations-resolver-name]
                  (create-observation-resolver dataset))
        (assoc-in [:resolvers observations-page-resolver-name]
                  (wrap-observations-mapping resolvers/resolve-observations-page dataset)))))

(defn get-query-schema-model [{:keys [description] :as dataset} dataset-enum-mappings]
  (let [schema-name (types/dataset-schema dataset)
        dimensions-measures-enum-name (types/field-name->type-name :observation_ordering schema-name)]
    {schema-name
     {:type
      {:implements [:dataset_meta]
       :fields {:uri          {:type :uri :description "Dataset URI"}
                :title        {:type 'String :description "Dataset title"}
                :description  {:type 'String :description "Dataset description"}
                :licence      {:type :uri :description "URI of the licence the dataset is published under"}
                :issued       {:type :DateTime :description "When the dataset was issued"}
                :modified     {:type :DateTime :description "When the dataset was last modified"}
                :publisher    {:type :uri :description "URI of the publisher of the dataset"}
                :schema       {:type 'String :description "Name of the GraphQL query root field corresponding to this dataset"}
                :dimensions   {:type [:dim]
                               :resolve     (fn [context args _field]
                                              (resolvers/resolve-dataset-dimensions context args dataset))
                               :description "Dimensions within the dataset"}
                :measures     {:type [:measure]
                               :description "Measure types within the dataset"}
                ;;TODO: change get-observation-schema-model so get-in is not required
                :observations (get-in (get-observation-schema-model dataset dimensions-measures-enum-name) [:fields :observations])}
       :description (or description "")}
      :resolve (fn [context args field]
                 (resolvers/resolve-dataset context dataset))}}))

(defn get-dataset-schema [{:keys [description] :as dataset} dataset-enum-mappings]
  (let [schema (types/dataset-schema dataset)
        enums-schema (mapping/dataset-enum-types-schema dataset dataset-enum-mappings)
        resolver-name (dataset-resolver-name dataset)
        dimensions-resolver-name (dataset-dimensions-resolver-name dataset)
        fixed-schema {:enums enums-schema
                     :objects
                      {schema
                       {:implements  [:dataset_meta]
                        :fields
                                     {:uri          {:type :uri :description "Dataset URI"}
                                      :title        {:type 'String :description "Dataset title"}
                                      :description  {:type 'String :description "Dataset description"}
                                      :licence      {:type :uri :description "URI of the licence the dataset is published under"}
                                      :issued       {:type :DateTime :description "When the dataset was issued"}
                                      :modified     {:type :DateTime :description "When the dataset was last modified"}
                                      :publisher    {:type :uri :description "URI of the publisher of the dataset"}
                                      :schema       {:type 'String :description "Name of the GraphQL query root field corresponding to this dataset"}
                                      :dimensions   {:type        '(list :dim)
                                                     :resolve     dimensions-resolver-name
                                                     :description "Dimensions within the dataset"}
                                      :measures     {:type        '(list :measure)
                                                     :description "Measure types within the dataset"}}
                        :description (or description "")}}

                      :queries
                      {schema
                       {:type    schema
                        :resolve resolver-name}}

                      :resolvers
                      {resolver-name (fn [context args field]
                                       (resolvers/resolve-dataset context dataset))
                       dimensions-resolver-name        (fn [context args _field]
                                                         (resolvers/resolve-dataset-dimensions context args dataset))}}]
    (-> fixed-schema
        (merge-observations-schema dataset)
        (merge-aggregations-schema dataset))))
