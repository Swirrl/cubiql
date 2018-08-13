(ns graphql-qb.types
  "Functions for mapping DSD elements to/from GraphQL types"
  (:require [clojure.string :as string]
            [graphql-qb.query-model :as qm]
            [graphql-qb.vocabulary :refer [time:hasBeginning time:hasEnd time:inXSDDateTime rdfs:label]]
            [graphql-qb.types.scalars :refer [grafter-date->datetime]]
            [graphql-qb.util :as util]
            [graphql-qb.config :as config])
  (:import [clojure.lang Keyword IPersistentMap]))

(defn get-identifier-segments [label]
  (let [segments (re-seq #"[a-zA-Z0-9]+" (str label))]
    (if (empty? segments)
      (throw (IllegalArgumentException. (format "Cannot construct identifier from label '%s'" label)))
      (let [first-char (ffirst segments)]
        (if (Character/isDigit first-char)
          (cons "a" segments)
          segments)))))

(defn- segments->schema-key [segments]
  (->> segments
       (map string/lower-case)
       (string/join "_")
       (keyword)))

(defn dataset-name->schema-name [label]
  (segments->schema-key (cons "dataset" (get-identifier-segments label))))

(defn label->field-name [label]
  (segments->schema-key (get-identifier-segments label)))

(defn ->field-name [{:keys [label]}]
  (label->field-name label))

(defn field-name->type-name [field-name ds-schema]
  (keyword (str (name ds-schema) "_" (name field-name) "_type")))

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

(defrecord EnumType [enum-name values])

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

(extend-protocol SparqlResultProjector
  Keyword
  (apply-projection [kw model selections config]
    model)

  IPersistentMap
  (apply-projection [m model selections config]
    (reduce (fn [acc [k inner-selections]]
              (let [proj (get m k)]
                (apply-projection proj acc inner-selections config)))
            model
            selections)))

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
    (get bindings dim-key)))

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

(defprotocol TypeFilter
  (apply-type-filter [type dim-key dimension-uri model sparql-value]))

(defn default-type-filter [_type dim-key dimension-uri model sparql-value]
  (let [value (or sparql-value ::qm/var)]
    (qm/add-binding model [[dim-key dimension-uri]] value)))

(defn- ref-period-type-filter [_type dim-key dimension-uri model sparql-value]
  (apply-ref-period-filter model dim-key dimension-uri sparql-value))

(def default-type-filter-impl {:apply-type-filter default-type-filter})

(extend RefAreaType TypeFilter {:apply-type-filter ref-period-type-filter})
(extend RefPeriodType TypeFilter default-type-filter-impl)
(extend EnumType TypeFilter default-type-filter-impl)

(defrecord Dimension [uri label order type]
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
          field-name (->field-name this)
          field-selections (get observation-selections field-name)]
      (apply-type-projection type dim-key uri model field-selections configuration)))

  (project-result [_this bindings]
    (let [dim-key (keyword (str "dim" order))]
      (project-type-result type dim-key bindings))))

(defrecord MeasureType [uri label order is-numeric?]
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

(defn dataset-schema [ds]
  (keyword (dataset-name->schema-name (:name ds))))

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

(defn get-observation-result [dataset bindings config]
  (let [field-results (map (fn [component]
                             [(->field-name component) (project-result component bindings)])
                           (dataset-dimension-measures dataset))]
    (into {:uri (:obs bindings)} field-results)))
