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

(defrecord RefAreaType [])

(defrecord RefPeriodType [])

(defrecord EnumType [enum-name values])

(defn is-enum-type? [type]
  (instance? EnumType type))

(defn is-ref-area-type? [type]
  (instance? RefAreaType type))

(defn is-ref-period-type? [type]
  (instance? RefPeriodType type))

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
  (project-result [this sparql-binding])
  (get-result-projection [this sparql-binding config]))

(extend-protocol SparqlResultProjector
  Keyword
  (apply-projection [kw model selections config]
    model)
  (project-result [kw sparql-binding]
    (get sparql-binding kw))

  IPersistentMap
  (apply-projection [m model selections config]
    (reduce (fn [acc [k inner-selections]]
              (let [proj (get m k)]
                (apply-projection proj acc inner-selections config)))
            model
            selections))

  (project-result [m sparql-binding]
    (into {} (map (fn [[k projector]]
                    [k (project-result projector sparql-binding)])
                  m))))

(defrecord PathProjection [path optional? transform-f]
  SparqlResultProjector
  (apply-projection [_this model selections config]
    (qm/add-binding model path ::qm/var :optional? optional?))
  (project-result [_this sparql-binding]
    (let [key-path (mapv first path)
          var-key (keyword (qm/key-path->var-name key-path))
          sparql-value (get sparql-binding var-key)]
      (if (and optional? (nil? sparql-value))
        nil
        (transform-f sparql-value)))))

(defrecord Dimension [uri label order type]
  SparqlQueryable

  (apply-order-by [_this model direction configuration]
    (let [codelist-label (config/dataset-label configuration)
          dim-key (keyword (str "dim" order))]
      (if (is-ref-area-type? type)
        (-> model
            (qm/add-binding [[dim-key uri] [:label codelist-label]] ::qm/var)
            (qm/add-order-by {direction [dim-key :label]}))
        ;;NOTE: binding should have already been added
        (qm/add-order-by model {direction [dim-key]}))))

  SparqlFilterable
  (apply-filter [this model sparql-value]
    (let [dim-key (keyword (str "dim" order))]
      (if (is-ref-period-type? type)
        (apply-ref-period-filter model dim-key uri sparql-value)
        (let [value (or sparql-value ::qm/var)]
          (qm/add-binding model [[dim-key uri]] value)))))

  SparqlResultProjector
  (apply-projection [this model observation-selections configuration]
    (let [dim-key (keyword (str "dim" order))
          field-name (->field-name this)
          field-selections (get observation-selections field-name)
          codelist-label (config/dataset-label configuration)]
      (cond
        (is-ref-period-type? type)
        (let [model (qm/add-binding model [[dim-key uri]] ::qm/var)
              model (if (contains? field-selections :label)
                      (qm/add-binding model [[dim-key uri] [:label codelist-label]] ::qm/var)
                      model)
              model (if (contains? field-selections :start)
                      (qm/add-binding model [[dim-key uri] [:begin time:hasBeginning] [:time time:inXSDDateTime]] ::qm/var)
                      model)]
          (if (contains? field-selections :end)
            (qm/add-binding model [[dim-key uri] [:end time:hasEnd] [:time time:inXSDDateTime]] ::qm/var)
            model))

        (is-ref-area-type? type)
        (let [label-selected? (contains? field-selections :label)]
          (if label-selected?
            (qm/add-binding model [[dim-key uri] [:label codelist-label]] ::qm/var)
            model))

        :else model)))

  (project-result [_this bindings]
    (let [dim-key (keyword (str "dim" order))]
      (cond
        (is-ref-period-type? type)
        {:uri   (get bindings dim-key)
         :label (get bindings (keyword (qm/key-path->var-name [dim-key :label])))
         :start (some-> (get bindings (keyword (qm/key-path->var-name [dim-key :begin :time]))) grafter-date->datetime)
         :end   (some-> (get bindings (keyword (qm/key-path->var-name [dim-key :end :time]))) grafter-date->datetime)}

        (is-ref-area-type? type)
        {:uri   (get bindings dim-key)
         :label (get bindings (keyword (qm/key-path->var-name [dim-key :label])))}

        :else (get bindings dim-key))))

  (get-result-projection [_this bindings config]
    (let [dim-key (keyword (str "dim" order))
          codelist-label (config/dataset-label config)]
      (cond
        (is-ref-period-type? type)
        {:uri   (->PathProjection [[dim-key uri]] false identity)
         :label (->PathProjection [[dim-key uri][:label codelist-label]] true identity)
         :start (->PathProjection [[dim-key uri] [:begin time:hasBeginning] [:time time:inXSDDateTime]] true grafter-date->datetime)
         :end   (->PathProjection [[dim-key uri] [:end time:hasEnd] [:time time:inXSDDateTime]] true grafter-date->datetime)}
        (is-ref-area-type? type)
        {:uri   (->PathProjection [[dim-key uri]] false identity)
         :label (->PathProjection [[dim-key uri] [:label codelist-label]] true identity)}

        :else
        (->PathProjection [[dim-key uri]] false identity)))))

(defrecord MeasureType [uri label order is-numeric?]
  SparqlQueryable
  (apply-order-by [_this model direction _configuration]
    (qm/add-order-by model {direction [(keyword (str "mv"))]}))

  SparqlResultProjector
  (apply-projection [_this model selections config]
    model)

  (project-result [_this binding]
    (if (= uri (get binding :mp))
             (get binding :mv)
             nil))

  (get-result-projection [_this bindings _config]
    (if (= uri (get bindings :mp))
      (let [dim-key (keyword (str "mv"))]
        (->PathProjection [[dim-key uri]] true identity))
      (->PathProjection nil true identity))))

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

(defn get-dataset-dimension-measure-by-uri [dataset uri]
  (util/find-first #(= uri (:uri %)) (dataset-dimension-measures dataset)))

(defn get-dataset-dimension-by-uri [dataset dimension-uri]
  (util/find-first (fn [dim] (= dimension-uri (:uri dim))) (dataset-dimensions dataset)))

(defn get-dataset-measure-by-uri [{:keys [measures] :as dataset} uri]
  (util/find-first #(= uri (:uri %)) measures))

(defn dataset-result-projection [dataset bindings config]
  (into {} (map (fn [ft]
                  [(->field-name ft) (get-result-projection ft bindings config)])
                (dataset-dimension-measures dataset))))
