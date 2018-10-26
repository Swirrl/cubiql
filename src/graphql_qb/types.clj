(ns graphql-qb.types
  "Functions for mapping DSD elements to/from GraphQL types"
  (:require [graphql-qb.query-model :as qm]
            [graphql-qb.vocabulary :refer [time:hasBeginning time:hasEnd time:inXSDDateTime rdfs:label]]
            [graphql-qb.types.scalars :refer [grafter-date->datetime]]
            [graphql-qb.util :as util]
            [graphql-qb.config :as config]))

(defprotocol SparqlFilterable
  (apply-filter [this model graphql-value]))

(defprotocol SparqlQueryable
  (apply-order-by [this model direction config]))

(defprotocol SparqlTypeProjection
  "Protocol for projecting a set of observation SPARQL bindings into a data structure
   for the type"
  (project-type-result [type root-key bindings]
    "Extracts any values associated with the the dimensions key from a map of SPARQL bindings and returns
     a data structure representing this type."))

(defrecord RefAreaType [])
(defrecord RefPeriodType [])
(defrecord EnumType [])
(defrecord DecimalType [])
(defrecord StringType [])
(defrecord MeasureDimensionType [])
(defrecord UnmappedType [type-uri])

(defn find-item-by-name [name items]
  (util/find-first #(= name (:name %)) items))

(defn find-item-by-value [value items]
  )

(defrecord EnumMappingItem [name value label])

(defrecord MappedEnumType [enum-type-name type doc items])

(defrecord GroupMapping [name items])

(def ref-area-type (->RefAreaType))
(def ref-period-type (->RefPeriodType))
(def decimal-type (->DecimalType))
(def string-type (->StringType))
(def enum-type (->EnumType))
(def measure-dimension-type (->MeasureDimensionType))

(defn maybe-add-period-filter [model dim-key dim-uri interval-key filter-fn dt]
  (if (some? dt)
    (let [key-path [[dim-key dim-uri] interval-key [:time time:inXSDDateTime]]]
      (-> model
          (qm/add-binding key-path ::qm/var)
          (qm/add-filter (map first key-path) [filter-fn dt])))
    model))

(defn apply-ref-period-filter [model dim-key dim-uri {:keys [uri starts_before starts_after ends_before ends_after] :as filter}]
  (if (nil? filter)
    (qm/add-binding model [[dim-key dim-uri]] ::qm/var)
    (let [model (if (some? uri) (qm/add-binding model [[dim-key dim-uri]] uri) model)]
      (if (and (nil? starts_before) (nil? starts_after) (nil? ends_before) (nil? ends_after))
        (qm/add-binding model [[dim-key dim-uri]] ::qm/var)
        ;; add bindings/filter for each filter
        (-> model
            (maybe-add-period-filter dim-key dim-uri [:begin time:hasBeginning] '<= starts_before)
            (maybe-add-period-filter dim-key dim-uri [:begin time:hasBeginning] '>= starts_after)
            (maybe-add-period-filter dim-key dim-uri [:end time:hasEnd] '<= ends_before)
            (maybe-add-period-filter dim-key dim-uri [:end time:hasEnd] '>= ends_after))))))

(defprotocol SparqlResultProjector
  (apply-projection [this model selections config])
  (project-result [this sparql-binding]))

(extend-protocol SparqlTypeProjection
  RefPeriodType
  (project-type-result [_type dim-key bindings]
    {:uri   (get bindings dim-key)
     :label (get bindings (qm/key-path->var-key [dim-key :label]))
     :start (some-> (get bindings (qm/key-path->var-key [dim-key :begin :time])) grafter-date->datetime)
     :end   (some-> (get bindings (qm/key-path->var-key [dim-key :end :time])) grafter-date->datetime)})

  RefAreaType
  (project-type-result [_type dim-key bindings]
    {:uri   (get bindings dim-key)
     :label (get bindings (qm/key-path->var-key [dim-key :label]))})

  EnumType
  (project-type-result [_type dim-key bindings]
    (get bindings dim-key))

  DecimalType
  (project-type-result [_type dim-key bindings]
    (get bindings dim-key))

  StringType
  (project-type-result [_type dim-key bindings]
    (get bindings dim-key))

  MeasureDimensionType
  (project-type-result [_type dim-key bindings]
    (get bindings dim-key))

  UnmappedType
  (project-type-result [_type dim-key bindings]
    (some-> (get bindings dim-key) str)))

(defprotocol TypeResultProjector
  (apply-type-projection [type dim-key uri model field-selections configuration]))

(extend-protocol TypeResultProjector
  RefPeriodType
  (apply-type-projection [_type dim-key uri model field-selections configuration]
    (let [codelist-label (config/dataset-label configuration)
          model (qm/add-binding model [[dim-key uri]] ::qm/var)
          model (if (contains? field-selections :label)
                  (qm/add-binding model [[dim-key uri] [:label codelist-label]] ::qm/var)
                  model)
          model (if (contains? field-selections :start)
                  (qm/add-binding model [[dim-key uri] [:begin time:hasBeginning] [:time time:inXSDDateTime]] ::qm/var)
                  model)]
      (if (contains? field-selections :end)
        (qm/add-binding model [[dim-key uri] [:end time:hasEnd] [:time time:inXSDDateTime]] ::qm/var)
        model)))

  RefAreaType
  (apply-type-projection [_type dim-key uri model field-selections configuration]
    (let [label-selected? (contains? field-selections :label)
          codelist-label (config/dataset-label configuration)]
      (if label-selected?
        (qm/add-binding model [[dim-key uri] [:label codelist-label]] ::qm/var)
        model)))

  EnumType
  (apply-type-projection [_type _dim-key _uri model _field-selections _configuration]
    model)

  DecimalType
  (apply-type-projection [_type _dim-key _uri model _field-selections _configuration]
    model)

  StringType
  (apply-type-projection [_type _dim-key _uri model _field-selections _configuration]
    model)

  MeasureDimensionType
  (apply-type-projection [_type _dim-key _uri model _field-selections _configuration]
    model)

  UnmappedType
  (apply-type-projection [_type _dim-key _uri model _field-selections _configuration]
    model))

(defprotocol TypeOrderBy
  (apply-type-order-by [type dim-key dimension-uri model direction configuration]))

(defn- default-type-order-by [_type dim-key _dimension-uri model direction _configuration]
  ;;NOTE: binding should have already been added
  (qm/add-order-by model {direction [dim-key]}))

(def default-type-order-by-impl {:apply-type-order-by default-type-order-by})

(defn- ref-area-order-by [type dim-key dimension-uri model direction configuration]
  (let [codelist-label (config/dataset-label configuration)]
    (-> model
        (qm/add-binding [[dim-key dimension-uri] [:label codelist-label]] ::qm/var)
        (qm/add-order-by {direction [dim-key :label]}))))

(extend RefAreaType TypeOrderBy {:apply-type-order-by ref-area-order-by})
(extend RefPeriodType TypeOrderBy default-type-order-by-impl)
(extend EnumType TypeOrderBy default-type-order-by-impl)
(extend DecimalType TypeOrderBy default-type-order-by-impl)
(extend StringType TypeOrderBy default-type-order-by-impl)
(extend MeasureDimensionType TypeOrderBy default-type-order-by-impl)
(extend UnmappedType TypeOrderBy default-type-order-by-impl)

(defprotocol TypeFilter
  (apply-type-filter [type dim-key dimension-uri model sparql-value]))

(defn default-type-filter [_type dim-key dimension-uri model sparql-value]
  (let [value (or sparql-value ::qm/var)]
    (qm/add-binding model [[dim-key dimension-uri]] value)))

(defn- ref-period-type-filter [_type dim-key dimension-uri model sparql-value]
  (apply-ref-period-filter model dim-key dimension-uri sparql-value))

(def default-type-filter-impl {:apply-type-filter default-type-filter})

(extend RefAreaType TypeFilter default-type-filter-impl)
(extend RefPeriodType TypeFilter {:apply-type-filter ref-period-type-filter})
(extend EnumType TypeFilter default-type-filter-impl)
(extend DecimalType TypeFilter default-type-filter-impl)
(extend StringType TypeFilter default-type-filter-impl)
(extend MeasureDimensionType TypeFilter default-type-filter-impl)
(extend UnmappedType TypeFilter default-type-filter-impl)

;;measure types
;;TODO: combine with dimension types?
(defrecord FloatMeasureType [])
(defrecord StringMeasureType [])

(def float-measure-type (->FloatMeasureType))
(def string-measure-type (->StringMeasureType))

(defrecord Dimension [uri order type]
  SparqlQueryable
  (apply-order-by [_this model direction configuration]
    (let [dim-key (keyword (str "dim" order))]
      (apply-type-order-by type dim-key uri model direction configuration)))

  SparqlFilterable
  (apply-filter [this model sparql-value]
    (let [dim-key (keyword (str "dim" order))]
      (apply-type-filter type dim-key uri model sparql-value)))

  SparqlResultProjector
  (apply-projection [this model observation-selections configuration]
    (let [dim-key (keyword (str "dim" order))
          ;;TODO: move field selections into caller?
          field-selections (get observation-selections uri)]
      (apply-type-projection type dim-key uri model field-selections configuration)))

  (project-result [_this bindings]
    (let [dim-key (keyword (str "dim" order))]
      (project-type-result type dim-key bindings))))

(defrecord MeasureType [uri order is-numeric?]
  SparqlQueryable
  (apply-order-by [_this model direction _configuration]
    (qm/add-order-by model {direction [(keyword (str "mv"))]}))

  SparqlResultProjector
  (apply-projection [_this model selections config]
    model)

  (project-result [_this binding]
    (when (= uri (get binding :mp))
      (get binding :mv))))

(defrecord Dataset [uri name dimensions measures])

(defn dataset-aggregate-measures [{:keys [measures] :as ds}]
  (filter :is-numeric? measures))

(defn dataset-dimension-measures [{:keys [dimensions measures] :as ds}]
  (concat dimensions measures))

(defn dataset-dimensions [ds]
  (:dimensions ds))

(defn dataset-measures [ds]
  (:measures ds))

(defn is-numeric-measure? [m]
  (:is-numeric? m))

(defn get-dataset-dimension-measure-by-uri [dataset uri]
  (util/find-first #(= uri (:uri %)) (dataset-dimension-measures dataset)))

(defn get-dataset-dimension-by-uri [dataset dimension-uri]
  (util/find-first (fn [dim] (= dimension-uri (:uri dim))) (dataset-dimensions dataset)))

(defn get-dataset-measure-by-uri [{:keys [measures] :as dataset} uri]
  (util/find-first #(= uri (:uri %)) measures))

