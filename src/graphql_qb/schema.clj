(ns graphql-qb.schema
  (:require [graphql-qb.types :as types]
            [graphql-qb.resolvers :as resolvers]
            [graphql-qb.schema-model :as sm]
            [clojure.pprint :as pp]
            [graphql-qb.util :as util]))

(def observation-uri-type-schema
  {:type :uri
   :description "URI of the observation"})

(defn get-enum-names [{:keys [values] :as enum-type}]
  {:pre [(types/is-enum-type? enum-type)]}
  (mapv :name values))

(defn enum->schema [enum]
  {(types/type-name enum) {:values (get-enum-names enum)}})

(defn dataset-observation-schema [{:keys [dimensions measures] :as dataset}]
  (let [dimension-schemas (map types/->schema-element dimensions)
        measure-type-schemas (map types/->schema-element measures)]
    (into {:uri observation-uri-type-schema}
          (concat dimension-schemas measure-type-schemas))))

(defn dataset-observation-filter-schema [{:keys [dimensions] :as dataset}]
  (apply merge (map types/->input-schema-element dimensions)))

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

(defn dimension->schema-mapping [{:keys [field-name type] :as dim}]
  (let [->graphql (if (types/is-enum-type? type)
                    (fn [v] (types/to-graphql dim v))
                    identity)]
    {field-name {:type (types/type-name type) :->graphql ->graphql}}))

(defn measure->schema-mapping [{:keys [field-name] :as measure}]
  {field-name {:type 'String :->graphql str}})

(defn get-dataset-schema-mapping [{:keys [dimensions measures] :as ds}]
  (let [dim-mappings (map dimension->schema-mapping dimensions)
        measure-mappings (map measure->schema-mapping measures)]
    (into {:uri {:type :uri :->graphql identity}} (concat dim-mappings measure-mappings))))

(defn schema-mapping->observations-schema [mapping]
  (util/map-values :type mapping))

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

(defn add-enum-schema [schema enum]
  (let [enum-schema (enum->schema enum)]
    (update schema :enums #(merge % enum-schema))))

(defn merge-aggregations-schema [partial-schema dataset]
  (let [aggregation-measures (types/dataset-aggregate-measures dataset)]
    (if-not (empty? aggregation-measures)
      (let [schema (types/dataset-schema dataset)
            aggregation-fields-type-name (types/field-name->type-name :aggregations schema)
            aggregation-types-type-name (types/field-name->type-name :aggregation_measure_types schema)
            observation-result-type-name (types/field-name->type-name :observation_result schema)
            aggregation-measures-enum (types/build-enum schema :aggregation_measure_types aggregation-measures)]
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
            (assoc-in [:objects observation-result-type-name :aggregations]
                      {:type aggregation-fields-type-name})
            (add-enum-schema aggregation-measures-enum)
            (assoc-in [:resolvers (aggregation-resolver-name dataset :max)] (create-aggregation-resolver :max aggregation-measures-enum))
            (assoc-in [:resolvers (aggregation-resolver-name dataset :min)] (create-aggregation-resolver :min aggregation-measures-enum))
            (assoc-in [:resolvers (aggregation-resolver-name dataset :sum)] (create-aggregation-resolver :sum aggregation-measures-enum))
            (assoc-in [:resolvers (aggregation-resolver-name dataset :avg)] (create-aggregation-resolver :avg aggregation-measures-enum))))
      partial-schema)))

(defn merge-observations-schema [partial-schema dataset]
  (let [schema (types/dataset-schema dataset)
        observation-type-name (types/field-name->type-name :observation schema)
        observation-result-type-name (types/field-name->type-name :observation_result schema)
        observation-filter-type-name (types/field-name->type-name :observation_dimensions schema)
        dimensions-measures-fields-enum (types/build-enum schema :observation_ordering (types/dataset-dimension-measures dataset))
        field-orderings-type-name (types/field-name->type-name :field_ordering schema)
        observations-resolver-name (dataset-observations-resolver-name dataset)
        observations-page-resolver-name (dataset-observations-page-resolver-name dataset)
        observation-results-page-type-name (types/field-name->type-name :observation_results_page schema)]
    (-> partial-schema
        (assoc-in [:objects schema :fields :observations]
                  {:type        observation-result-type-name
                   :args        {:dimensions {:type observation-filter-type-name}
                                 :order      {:type (list 'list (types/type-name dimensions-measures-fields-enum))}
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
        (add-enum-schema dimensions-measures-fields-enum)
        (assoc-in [:input-objects observation-filter-type-name] {:fields (dataset-observation-filter-schema dataset)})
        (assoc-in [:input-objects field-orderings-type-name] {:fields (get-dataset-sort-specification-schema dataset)})
        (assoc-in [:resolvers observations-resolver-name]
                  (create-observation-resolver dataset))
        (assoc-in [:resolvers observations-page-resolver-name]
                  (wrap-observations-mapping resolvers/resolve-observations-page dataset)))))

(defn get-dataset-schema [{:keys [description] :as dataset}]
  (let [schema (types/dataset-schema dataset)
        dataset-enums (types/dataset-enum-types dataset)
        enums-schema (apply merge (map enum->schema dataset-enums))
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
                      {(dataset-resolver-name dataset) (fn [context args field]
                                                         (resolvers/resolve-dataset context dataset))
                       dimensions-resolver-name        (fn [context args _field]
                                                         (resolvers/resolve-dataset-dimensions context args dataset))}}]
    (-> fixed-schema
        (merge-observations-schema dataset)
        (merge-aggregations-schema dataset))))

