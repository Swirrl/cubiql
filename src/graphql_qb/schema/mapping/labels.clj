(ns graphql-qb.schema.mapping.labels
  "Creates GraphQL schema mappings from the labels associated with types and values"
  (:require [clojure.spec.alpha :as s]
            [graphql-qb.util :as util]
            [clojure.string :as string]
            [graphql-qb.types :as types]
            [graphql-qb.config :as config]
            [graphql-qb.queries :as queries]
            [graphql-qb.schema.mapping.dataset :as dsm])
  (:import [graphql_qb.types RefPeriodType RefAreaType]
           [graphql_qb.types FloatMeasureType StringMeasureType]))

;;TODO: add/use spec for graphql enum values
(s/def ::graphql-enum keyword?)

(defn find-item-by-name [name items]
  (util/find-first #(= name (:name %)) items))

;;TODO: move and implement for all types
(defprotocol ArgumentTransform
  (transform-argument [this graphql-value]))

(defprotocol ResultTransform
  (transform-result [this inner-value]))

(defrecord EnumMappingItem [name value label])

(defrecord MappedEnumType [enum-type-name type doc items]
  ArgumentTransform
  (transform-argument [_this graphql-value]
    (:value (find-item-by-name graphql-value items)))

  ResultTransform
  (transform-result [_this result]
    (->> items
         (util/find-first (fn [{:keys [value]}]
                            (= value result)))
         (:name))))

(extend-protocol ResultTransform
  RefAreaType
  (transform-result [_ref-area-type result] result)

  RefPeriodType
  (transform-result [_ref-period-type result] result))

(extend-type FloatMeasureType
  ResultTransform
  (transform-result [_this r] (some-> r double)))

(extend-type StringMeasureType
  ResultTransform
  (transform-result [_this r] (str r)))

;;TODO: move/remove types/get-identifier-segments
(defn get-identifier-segments [label]
  (let [segments (re-seq #"[a-zA-Z0-9]+" (str label))]
    (if (empty? segments)
      (throw (IllegalArgumentException. (format "Cannot construct identifier from label '%s'" label)))
      (let [first-char (ffirst segments)]
        (if (Character/isDigit first-char)
          (cons "a" segments)
          segments)))))

(defn- segments->enum-value [segments]
  (->> segments
       (map string/upper-case)
       (string/join "_")
       (keyword)))

(defn label->enum-name
  ([label]
   (segments->enum-value (get-identifier-segments label)))
  ([label n]
   (let [label-segments (get-identifier-segments label)]
     (segments->enum-value (concat label-segments [(str n)])))))

(defrecord GroupMapping [name items]
  ArgumentTransform
  (transform-argument [_this graphql-value]
    (:value (find-item-by-name graphql-value items))))

(defn create-group-mapping
  ([name mappings] (create-group-mapping name mappings identity))
  ([name mappings val-f]
    ;;TODO: handle multiple mappings to the same label
   (let [items (mapv (fn [{:keys [label] :as mapping}]
                       (->EnumMappingItem (label->enum-name label) (val-f mapping) label))
                     mappings)]
     (->GroupMapping name items))))

(defn dataset-dimensions-measures-enum-group [dataset-mapping]
  (let [schema (dsm/schema dataset-mapping)
        mapping-name (keyword (str (name schema) "_dimension_measures"))]
    (create-group-mapping mapping-name (dsm/components dataset-mapping) :uri)))

(defn dataset-aggregation-measures-enum-group [dataset-mapping]
  (if-let [aggregation-measures (dsm/numeric-measure-mappings dataset-mapping)]
    (let [schema (dsm/schema dataset-mapping)
          mapping-name (keyword (str (name schema) "_aggregation_measures"))]
      (create-group-mapping mapping-name aggregation-measures :uri))))

(defn create-enum-mapping [enum-label enum-doc code-list]
  (let [by-enum-name (group-by #(label->enum-name (:label %)) code-list)
        items (mapcat (fn [[enum-name item-results]]
                        (if (= 1 (count item-results))
                          (map (fn [{:keys [member label]}]
                                 (->EnumMappingItem enum-name member label))
                               item-results)
                          (map-indexed (fn [n {:keys [member label]}]
                                         (->EnumMappingItem (label->enum-name label (inc n)) member label))
                                       item-results)))
                      by-enum-name)]
    {:label enum-label :doc enum-doc :items (vec items)}))

(defn- get-measure-type [m]
  (types/is-numeric-measure? m) (types/->FloatMeasureType) (types/->StringMeasureType))

(defn get-dimension-codelist [dimension-member-bindings configuration]
  (map (fn [[member-uri member-bindings]]
         (let [labels (map :vallabel member-bindings)]
           {:member member-uri :label (util/find-best-language labels (config/schema-label-language configuration))}))
       (group-by :member dimension-member-bindings)))

(defn- get-dataset-enum-mappings [dataset dataset-member-bindings configuration]
  (let [dimension-member-bindings (group-by :dim dataset-member-bindings)
        field-mappings (map (fn [[dim-uri dim-members]]
                              (let [dimension (types/get-dataset-dimension-by-uri dataset dim-uri)
                                    dim-label (:label dimension)
                                    enum-doc ""                                ;;TODO: get optional comment for dimension
                                    codelist (get-dimension-codelist dim-members configuration)]
                                [dim-uri (create-enum-mapping dim-label enum-doc codelist)]))
                            dimension-member-bindings)]
    (into {} field-mappings)))

(defn get-datasets-enum-mappings [datasets codelist-member-bindings configuration]
  (let [ds-members (group-by :ds codelist-member-bindings)
        dataset-mappings (map (fn [dataset]
                                (let [ds-uri (:uri dataset)
                                      ds-codelist-member-bindings (get ds-members ds-uri)]
                                  [ds-uri (get-dataset-enum-mappings dataset ds-codelist-member-bindings configuration)]))
                              datasets)]
    (into {} dataset-mappings)))

;;schema mappings

(defn dimension->enum-schema [{:keys [type] :as dim}]
  (when (instance? MappedEnumType type)
    (let [{:keys [enum-type-name doc items]} type]
      (if (some? doc)
        {enum-type-name {:values (mapv :name items) :description doc}}
        {enum-type-name {:values (mapv :name items)}}))))

(defn dataset-enum-types-schema [dataset-mapping]
  (apply merge (map (fn [dim]
                      (dimension->enum-schema dim))
                    (dsm/dimensions dataset-mapping))))

(defn get-all-enum-mappings [repo datasets config]
  (let [enum-dimension-values-query (queries/get-all-enum-dimension-values config)
        results (util/eager-query repo enum-dimension-values-query)
        dataset-enum-values (map (util/convert-binding-labels [:vallabel]) results)]
    (get-datasets-enum-mappings datasets dataset-enum-values config)))

(defn field-name->type-name [field-name ds-schema]
  (keyword (str (name ds-schema) "_" (name field-name) "_type")))

(defn- get-dimension-thingy [schema {:keys [uri label] :as dimension} ds-enum-mappings]
  ;;TODO: change ds-enum-mappings keys to URIs instead of field names
  (let [dimension-type (:type dimension)
        field-name (types/->field-name dimension)
        mapped-type (if (contains? ds-enum-mappings uri)
                      (let [enum-mapping (get ds-enum-mappings uri)
                            enum-name (field-name->type-name field-name schema)]
                        (->MappedEnumType enum-name dimension-type (:doc enum-mapping) (:items enum-mapping)))
                      dimension-type)]
    {:uri        uri
     :label      label
     :doc        nil                                        ;;TODO: fetch dimension comment
     :field-name field-name
     :enum-name (label->enum-name label)
     :type       mapped-type
     :dimension  dimension}))

(defn- get-measure-mapping [{:keys [uri label is-numeric?] :as measure}]
  ;;TODO: remove label from MeasureType
  {:uri uri
   :label label
   :field-name (types/->field-name measure)
   :enum-name (label->enum-name label)
   :type (get-measure-type measure)
   :is-numeric? is-numeric?
   :measure measure})

(defn dataset-name->schema-name [label]
  (types/segments->schema-key (cons "dataset" (get-identifier-segments label))))

(defn dataset-schema [ds]
  (keyword (dataset-name->schema-name (:name ds))))

(defn build-dataset-mapping-model [{:keys [uri label] :as dataset} ds-enum-mappings]
  (let [schema (dataset-schema dataset)
        dimensions (mapv #(get-dimension-thingy schema % ds-enum-mappings) (types/dataset-dimensions dataset))
        measures (mapv get-measure-mapping (types/dataset-measures dataset))]
    {:uri uri
     :schema schema
     :dimensions dimensions
     :measures measures}))

(defn get-dataset-mapping-models [repo datasets configuration]
  (let [enum-mappings (get-all-enum-mappings repo datasets configuration)]
    (mapv (fn [{:keys [uri] :as ds}]
            (let [ds-enum-mappings (get enum-mappings uri {})]
              (build-dataset-mapping-model ds ds-enum-mappings)))
          datasets)))
