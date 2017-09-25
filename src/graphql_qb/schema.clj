(ns graphql-qb.schema
  (:require [graphql-qb.types :as types]))

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
  (apply merge (map types/->schema-element dimensions)))

(defn dataset-resolver [dataset]
  (let [schema (types/dataset-schema dataset)]
    (keyword (str "resolve_" (name schema)))))

(defn get-dataset-sort-specification-schema [{:keys [dimensions measures] :as dataset}]
  (into {} (map (fn [m] [(types/->field-name m)
                         {:type :sort_direction}]) (concat dimensions measures))))

(defn get-dataset-schema [{:keys [description dimensions measures] :as dataset}]
  (let [schema (types/dataset-schema dataset)
        observation-schema (dataset-observation-schema dataset)
        observation-filter-schema (dataset-observation-filter-schema dataset)
        observation-filter-type-name (types/field-name->type-name :observation_dimensions schema)

        aggregation-measures (types/dataset-aggregate-measures dataset)
        aggregation-measures-enum (types/build-enum schema :aggregation_measure_types aggregation-measures)
        aggregation-types-type-name (types/field-name->type-name :aggregation_measure_types schema)
        aggregation-fields-type-name (types/field-name->type-name :aggregations schema)

        dimensions-measures-fields-enum (types/build-enum schema :observation_ordering (concat dimensions measures))
        field-orderings-type-name (types/field-name->type-name :field_ordering schema)

        observation-type-name (types/field-name->type-name :observation schema)
        observation-result-type-name (types/field-name->type-name :observation_result schema)
        observation-dims-type-name (types/field-name->type-name :observation_dimensions schema)
        observation-results-page-type-name (types/field-name->type-name :observation_results_page schema)
        
        dataset-enums (types/get-enums dataset)
        all-enums (concat dataset-enums [aggregation-measures-enum dimensions-measures-fields-enum])
        enums-schema (apply merge (map enum->schema all-enums))

        resolver-name (dataset-resolver dataset)]
    {:enums enums-schema
     :objects
     {schema
      {:implements [:dataset_meta]
       :fields
       {:uri {:type :uri :description "Dataset URI"}
        :title {:type 'String :description "Dataset title"}
        :description {:type 'String :description "Dataset description"}
        :schema {:type 'String :description "Name of the GraphQL query root field corresponding to this dataset"}
        :dimensions {:type '(list :dim)
                     :description "Dimensions within the dataset"
                     :resolve :resolve-dataset-dimensions}
        :measures {:type '(list :measure)
                   :description "Measure types within the dataset"
                   :resolve :resolve-dataset-measures}
        :observations {:type observation-result-type-name
                       :args {:dimensions {:type observation-dims-type-name}
                              :order {:type (list 'list (types/type-name dimensions-measures-fields-enum))}
                              :order_spec {:type field-orderings-type-name}}
                       :resolve :resolve-observations
                       :description "Observations matching the given criteria"}}
       :description description}

      observation-result-type-name
      {:fields
       {:sparql {:type 'String :description "SPARQL query used to retrieve matching observations."}
        :page {:type observation-results-page-type-name
               :args {:after {:type :SparqlCursor}
                      :first {:type 'Int}}
               :description "Page of results to retrieve."
               :resolve :resolve-observations-page}
        :aggregations {:type aggregation-fields-type-name}
        :total_matches {:type 'Int}}}

      observation-results-page-type-name
      {:fields
       {:next_page {:type :SparqlCursor :description "Cursor to the next page of results"}
        :count {:type 'Int}
        :result {:type (list 'list observation-type-name) :description "List of observations on this page"}}}

      aggregation-fields-type-name
      {:fields
       {:max {:type 'Float
              :args {:measure {:type (list 'non-null aggregation-types-type-name) :description "The measure to aggregate"}}
              :resolve :resolve-observations-max}
        :min {:type 'Float
              :args {:measure {:type (list 'non-null aggregation-types-type-name):description "The measure to aggregate"}}
              :resolve :resolve-observations-min}
        :sum {:type 'Float
              :args {:measure {:type (list 'non-null aggregation-types-type-name) :description "The measure to aggregate"}}
              :resolve :resolve-observations-sum}
        :average {:type 'Float
                  :args {:measure {:type (list 'non-null aggregation-types-type-name) :description "The measure to aggregate"}}
                  :resolve :resolve-observations-average}}}
      
      observation-type-name
      {:fields observation-schema}}

     :input-objects
     {observation-filter-type-name
      {:fields observation-filter-schema}

      field-orderings-type-name
      {:fields (get-dataset-sort-specification-schema dataset)}}

     :queries
     {schema
      {:type schema
       :resolve resolver-name}}}))

