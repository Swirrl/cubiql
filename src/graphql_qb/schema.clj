(ns graphql-qb.schema
  (:require [graphql-qb.types :as types]))

(def observation-uri-type-schema
  {:type :uri
   :description "URI of the observation"})

(defn get-enum-schema-values [{:keys [values->uri] :as enum-type}]
  {:pre [(types/is-enum-type? enum-type)]}
  (vec (keys values->uri)))

(defn enum->schema [enum]
  {(types/type-name enum) {:values (get-enum-schema-values enum)}})

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

(defn get-dataset-schema [{:keys [description] :as dataset}]
  (let [schema (types/dataset-schema dataset)
        observation-schema (dataset-observation-schema dataset)
        observation-filter-schema (dataset-observation-filter-schema dataset)
        observation-filter-type-name (types/field-name->type-name :observation_dimensions schema)

        aggregation-measures (types/dataset-aggregate-measures dataset)
        aggregation-measures-enum (types/build-enum schema :aggregation_measure_types aggregation-measures)
        aggregation-types-type-name (types/field-name->type-name :aggregation_measure_types schema)
        aggregation-fields-type-name (types/field-name->type-name :aggregations schema)
        ;aggregation-type-enum-schema (aggregate-measure-types->enums-schema schema aggregation-types-type-name aggregation-measure-types)

        observation-type-name (types/field-name->type-name :observation schema)
        observation-result-type-name (types/field-name->type-name :observation_result schema)
        observation-dims-type-name (types/field-name->type-name :observation_dimensions schema)
        observation-results-page-type-name (types/field-name->type-name :observation_results_page schema)
        
        ;dims-enums-schema (dimensions->enums-schema dims)
        dataset-enums (types/get-enums dataset)
        all-enums (concat dataset-enums [aggregation-measures-enum])
        enums-schema (apply merge (map enum->schema all-enums))

        resolver-name (dataset-resolver dataset)]
    {:enums enums-schema
     :objects
     {schema
      {:fields
       {:uri {:type :uri :description "Dataset URI"}
        :title {:type 'String :description "Dataset title"}
        :description {:type 'String :description "Dataset description"}
        :schema {:type 'String :description "Name of the GraphQL query root for this dataset"}
        :dimensions {:type '(list :dim) :description "Dimensions within the dataset"}
        :observations {:type observation-result-type-name
                       :args {:dimensions {:type observation-dims-type-name}}
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
        :total_matches {:type 'Int}
        :free_dimensions {:type '(list :dim)}}}

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
      {:fields observation-filter-schema}}

     :queries
     {schema
      {:type schema
       :resolve resolver-name}}}))

(defn wat []
  #_(let [observation-result-type-name (field-name->type-name :observation_result schema)
        observation-results-page-type-name (field-name->type-name :observation_results_page schema)
        observation-type-name (field-name->type-name :observation schema)
        observation-dims-type-name (field-name->type-name :observation_dimensions schema)
        resolver-name (keyword (str "resolve_" (name schema)))
        resolver-map {resolver-name (fn [context args field]
                                      (resolve-dataset uri context args field))}])
  #_{:enums enums-schema
     :objects
     {schema
      {:fields
       {:uri {:type :uri :description "Dataset URI"}
        :title {:type 'String :description "Dataset title"}
        :description {:type 'String :description "Dataset description"}
        :schema {:type 'String :description "Name of the GraphQL query root for this dataset"}
        :dimensions {:type '(list :dim) :description "Dimensions within the dataset"}
        :observations {:type observation-result-type-name
                       :args {:dimensions {:type observation-dims-type-name}}
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
        :total_matches {:type 'Int}
        :free_dimensions {:type '(list :dim)}}}

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
      {:fields observation-fields}}

     :input-objects
     {observation-dims-type-name
      {:fields (into {} obs-dim-schemas)}}

     :queries
     {schema
      {:type schema
       :resolve resolver-name}}

     :resolvers resolver-map})
