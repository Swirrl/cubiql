(ns graphql-qb.schema
  (:require [graphql-qb.types :as types]
            [graphql-qb.resolvers :as resolvers]
            [graphql-qb.schema-model :as sm]
            [graphql-qb.schema.mapping.labels :as mapping]
            [graphql-qb.context :as context]
            [graphql-qb.schema.mapping.dataset :as dsm]
            [com.walmartlabs.lacinia.schema :as ls]
            [graphql-qb.util :as util]
            [graphql-qb.schema.mapping.dataset :as ds-mapping])
  (:import [graphql_qb.types EnumType RefPeriodType RefAreaType DecimalType StringType UnmappedType StringMeasureType FloatMeasureType]
           [graphql_qb.schema.mapping.labels MappedEnumType]))

(defprotocol ToGraphQLInputType
  (->input-type-name [this]
    "Returns the name of the type to use for the argument representation of this type"))

(defprotocol ToGraphQLOutputType
  (->output-type-name [this]
    "Return the name of the type to use for the output representation of this type"))

(extend-protocol ToGraphQLInputType
  RefAreaType
  (->input-type-name [_ref-area-type] :uri)

  RefPeriodType
  (->input-type-name [_ref-period-type] :ref_period_filter)

  EnumType
  (->input-type-name [_enum-type] :uri)

  DecimalType
  (->input-type-name [_decimal-type] 'Float)

  StringType
  (->input-type-name [_string-type] 'String)

  UnmappedType
  (->input-type-name [_unmapped-type] 'String)

  MappedEnumType
  (->input-type-name [{:keys [enum-type-name]}] enum-type-name))

(extend-protocol ToGraphQLOutputType
  RefAreaType
  (->output-type-name [_ref-area-type] :ref_area)

  RefPeriodType
  (->output-type-name [_ref-period-type] :ref_period)

  EnumType
  (->output-type-name [_enum-type] :uri)

  DecimalType
  (->output-type-name [_decimal-type] 'Float)

  StringType
  (->output-type-name [_string-type] 'String)

  UnmappedType
  (->output-type-name [_unmapped-type] 'String)

  MappedEnumType
  (->output-type-name [{:keys [enum-type-name]}] enum-type-name)

  StringMeasureType
  (->output-type-name [_string-type] 'String)

  FloatMeasureType
  (->output-type-name [_float-type] 'Float))

(def default-argument-transform-impl
  {:transform-argument (fn [_type value] value)})

;;TODO: check implementations!
(extend RefAreaType mapping/ArgumentTransform default-argument-transform-impl)
(extend RefPeriodType mapping/ArgumentTransform default-argument-transform-impl)
(extend EnumType mapping/ArgumentTransform default-argument-transform-impl)
(extend DecimalType mapping/ArgumentTransform default-argument-transform-impl)
(extend StringType mapping/ArgumentTransform default-argument-transform-impl)
(extend UnmappedType mapping/ArgumentTransform default-argument-transform-impl)

(defn create-aggregation-resolver [dataset-mapping aggregation-fn aggregation-measures-enum]
  (fn [context {:keys [measure] :as args} field]
    (let [measure-uri (mapping/transform-argument aggregation-measures-enum measure)
          {:keys [type] :as measure-mapping} (ds-mapping/get-measure-by-uri dataset-mapping measure-uri)
          result (resolvers/resolve-observations-aggregation aggregation-fn context {:measure measure-mapping} field)]
      (mapping/transform-result type result))))

(defn get-order-by
  "Returns an ordered list of [component-uri sort-direction] given a sequence of component URIs and an associated
   (possibly partial) specification for the order direction of each field. If the sort direction is not specified
   for an ordered field it will be sorted in ascending order."
  [{:keys [order order_spec] :as args} dataset-mapping]
  (map (fn [dm-uri]
         (let [{:keys [uri] :as comp} (dsm/get-component-by-uri dataset-mapping dm-uri)
               direction (get order_spec uri :ASC)]
           [(dsm/component-mapping->component comp) direction]))
       order))

(defn map-dimension-filter [dimensions dataset-mapping]
  (into {} (map (fn [{:keys [uri dimension] :as dim-mapping}]
                  [dimension (get dimensions uri)])
                (dsm/dimensions dataset-mapping))))

(defn map-dataset-observation-args [{:keys [dimensions order order_spec]} dataset-mapping]
  (let [mapped-dimensions (into {} (map (fn [[field-name value]]
                                          (let [{:keys [uri type]} (ds-mapping/get-dimension-by-field-name dataset-mapping field-name)]
                                            [uri (mapping/transform-argument type value)]))
                                        dimensions))
        mapped-order (mapv (fn [component-enum]
                             (:uri (ds-mapping/get-component-by-enum-name dataset-mapping component-enum)))
                           order)
        mapped-order-spec (into {} (map (fn [[field-name dir]]
                                          [(:uri (ds-mapping/get-component-by-field-name dataset-mapping field-name)) dir])
                                        order_spec))]
    {:dimensions mapped-dimensions
     :order      mapped-order
     :order_spec mapped-order-spec}))

(defn create-observation-resolver [dataset-mapping]
  (fn [context args field]
    (let [{:keys [dimensions] :as mapped-args} (map-dataset-observation-args args dataset-mapping)
          updated-args {::resolvers/dimensions-filter (map-dimension-filter dimensions dataset-mapping)
                        ::resolvers/order-by (get-order-by mapped-args dataset-mapping)}]
      (resolvers/resolve-observations context updated-args field))))

(defn get-observation-result [dataset-model bindings configuration]
  (let [field-results (map (fn [{:keys [field-name type] :as component-mapping}]
                             (let [comp (ds-mapping/component-mapping->component component-mapping)
                                   result (types/project-result comp bindings)]
                               [field-name (mapping/transform-result type result)]))
                           (dsm/components dataset-model))]
    (into {:uri (:obs bindings)} field-results)))

(defn create-aggregation-field [dataset-mapping field-name aggregation-measures-enum-mapping aggregation-fn]
  {field-name
   {:type    'Float
    :args    {:measure {:type (sm/non-null aggregation-measures-enum-mapping) :description "The measure to aggregate"}}
    :resolve (create-aggregation-resolver dataset-mapping aggregation-fn aggregation-measures-enum-mapping)}})

(defn get-aggregations-schema-model [dataset-mapping aggregation-measures-enum-mapping]
  {:type
   {:fields
    (merge
      (create-aggregation-field dataset-mapping :max aggregation-measures-enum-mapping :max)
      (create-aggregation-field dataset-mapping :min aggregation-measures-enum-mapping :min)
      (create-aggregation-field dataset-mapping :sum aggregation-measures-enum-mapping :sum)
      (create-aggregation-field dataset-mapping :average aggregation-measures-enum-mapping :avg))}})

(defn dataset-observation-dimensions-input-schema-model [dataset-mapping]
  (into {} (map (fn [{:keys [field-name type] :as dim}]
                  [field-name {:type (->input-type-name type)}])
                (dsm/dimensions dataset-mapping))))

(defn dataset-observation-schema-model [dataset-mapping]
  (let [field-types (map (fn [{:keys [field-name type] :as comp}]
                           [field-name {:type (->output-type-name type)}])
                         (dsm/components dataset-mapping))]
    (into {:uri {:type :uri}}
          field-types)))

(defn dataset-order-spec-schema-model [dataset-mapping]
  (into {} (map (fn [{:keys [field-name] :as comp}]
                  [field-name {:type :sort_direction}]))
        (dsm/components dataset-mapping)))

(defn wrap-observations-mapping [inner-resolver dataset-mapping]
  (fn [context args observations-field]
    (let [result (inner-resolver context args observations-field)
          config (context/get-configuration context)
          mapped-result (mapv (fn [obs-bindings]
                                (get-observation-result dataset-mapping obs-bindings config))
                              (::resolvers/observation-results result))]
      (assoc result :observations mapped-result))))

(defn create-dataset-observations-page-resolver [dataset-mapping]
  (wrap-observations-mapping
    resolvers/resolve-observations-page
    dataset-mapping))

(defn get-observation-schema-model [dataset-mapping]
  (let [dimensions-measures-enum-mapping (mapping/dataset-dimensions-measures-enum-group dataset-mapping)
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
                        :observations {:type [{:fields (dataset-observation-schema-model dataset-mapping)}] :description "List of observations on this page"}}}
                      :args        {:after {:type :SparqlCursor}
                                    :first {:type 'Int}}
                      :description "Page of results to retrieve."
                      :resolve     (create-dataset-observations-page-resolver dataset-mapping)}
                     :total_matches {:type 'Int}}}
                   :args
                   {:dimensions {:type {:fields (dataset-observation-dimensions-input-schema-model dataset-mapping)}}
                    :order      {:type [dimensions-measures-enum-mapping]}
                    :order_spec {:type {:fields (dataset-order-spec-schema-model dataset-mapping)}}}
                   :resolve (resolvers/wrap-options (create-observation-resolver dataset-mapping))}
        aggregation-measures-enum-mapping (mapping/dataset-aggregation-measures-enum-group dataset-mapping)]
    (if (nil? aggregation-measures-enum-mapping)
      obs-model
      (let [aggregation-fields (get-aggregations-schema-model dataset-mapping aggregation-measures-enum-mapping)]
        (assoc-in obs-model [:type :fields :aggregations] aggregation-fields)))))

;;TODO: move? replace with protocol?
(defn- is-enum-type? [type]
  (instance? MappedEnumType type))

(defn- annotate-dimension-values [{:keys [type] :as dimension-mapping} dimension-values]
  (when dimension-values
    (if (is-enum-type? type)
      (mapv (fn [{:keys [uri] :as enum-value}]
              (let [enum-item (util/find-first #(= uri (:value %)) (:items type))]
                (-> enum-value
                    (assoc :enum_name (name (:name enum-item)))
                    (ls/tag-with-type :enum_dim_value))))
            dimension-values)
      (mapv #(ls/tag-with-type % :unmapped_dim_value) dimension-values))))

(defn annotate-dataset-dimensions [dataset-mapping dimensions]
  (mapv (fn [{:keys [uri] :as dim}]
          (let [{:keys [enum-name] :as dim-mapping} (dsm/get-dimension-by-uri dataset-mapping uri)]
            (-> dim
                (assoc :enum_name (name enum-name))
                (update :values #(annotate-dimension-values dim-mapping %)))))
        dimensions))

(defn create-dataset-dimensions-resolver [dataset-mapping]
  (fn [context args field]
    (let [inner-resolver (resolvers/create-dataset-dimensions-resolver dataset-mapping)
          results (inner-resolver context args field)]
      (annotate-dataset-dimensions dataset-mapping results))))

(defn create-global-dataset-dimensions-resolver [all-dataset-mappings]
  (let [uri->dataset-mapping (util/strict-map-by :uri all-dataset-mappings)]
    (fn [context args {:keys [uri] :as dataset-mapping-field}]
      (let [dataset-mapping (util/strict-get uri->dataset-mapping uri)
            results (resolvers/dataset-dimensions-resolver context args dataset-mapping-field)]
        (annotate-dataset-dimensions dataset-mapping results)))))

(defn map-dataset-measure-results [dataset-mapping results]
  (mapv (fn [{:keys [uri] :as result}]
          (let [measure-mapping (dsm/get-measure-by-uri dataset-mapping uri)]
            (assoc result :enum_name (name (:enum-name measure-mapping)))))
        results))

(defn create-dataset-measures-resolver [dataset-mapping]
  (fn [context args field]
    (let [inner-resolver (resolvers/create-dataset-measures-resolver dataset-mapping)
          results (inner-resolver context args field)]
      (map-dataset-measure-results dataset-mapping results))))

;;TODO: refactor with create-dataset-measures-resolver
(defn global-dataset-measures-resolver [context args {:keys [uri] :as dataset-field}]
  (let [dataset-mapping (context/get-dataset-mapping context uri)
        inner-resolver (resolvers/create-dataset-measures-resolver dataset-mapping)
        results (inner-resolver context args dataset-mapping)]
    (map-dataset-measure-results dataset-mapping results)))

(defn create-dataset-resolver [dataset-mapping]
  (resolvers/wrap-options
    ;;TODO: add spec for resolver result?
    ;;should contain keys defined in dataset schema
    (resolvers/dataset-resolver dataset-mapping)))

(defn get-query-schema-model [{:keys [schema] :as dataset-mapping}]
  (let [observations-model (get-observation-schema-model dataset-mapping)]
    {schema
     {:type
               {:implements  [:dataset_meta]
                :fields      {:uri          {:type :uri :description "Dataset URI"}
                              :title        {:type 'String :description "Dataset title"}
                              :description  {:type ['String] :description "Dataset descriptions"}
                              :licence      {:type [:uri] :description "URIs of the licences the dataset is published under"}
                              :issued       {:type [:DateTime] :description "When the dataset was issued"}
                              :modified     {:type :DateTime :description "When the dataset was last modified"}
                              :publisher    {:type [:uri] :description "URIs of the publishers of the dataset"}
                              :schema       {:type 'String :description "Name of the GraphQL query root field corresponding to this dataset"}
                              :dimensions   {:type        [:dim]
                                             :resolve     (create-dataset-dimensions-resolver dataset-mapping)
                                             :description "Dimensions within the dataset"}
                              :measures     {:type        [:measure]
                                             :description "Measure types within the dataset"
                                             :resolve     (create-dataset-measures-resolver dataset-mapping)}
                              :observations observations-model}
                :description (dsm/description dataset-mapping)}
      :resolve (create-dataset-resolver dataset-mapping)}}))

(defn get-qb-fields-schema [dataset-mappings]
  (reduce (fn [{:keys [qb-fields] :as acc} dsm]
            (let [m (get-query-schema-model dsm)
                  [field-name field] (first m)
                  field-schema (sm/visit-field [field-name] field-name field :objects)
                  ds-enums-schema {:enums (mapping/dataset-enum-types-schema dsm)}
                  field (::sm/field field-schema)
                  schema (::sm/schema field-schema)
                  schema (sm/merge-schemas schema ds-enums-schema)]
              {:qb-fields (assoc qb-fields field-name field)
               :schema (sm/merge-schemas (:schema acc) schema)}))
          {:qb-fields {} :schema {}}
          dataset-mappings))
