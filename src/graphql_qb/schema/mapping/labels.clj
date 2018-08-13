(ns graphql-qb.schema.mapping.labels
  "Creates GraphQL schema mappings from the labels associated with types and values"
  (:require [clojure.spec.alpha :as s]
            [graphql-qb.util :as util]
            [clojure.string :as string]
            [clojure.pprint :as pp]
            [graphql-qb.types :as types]
            [com.walmartlabs.lacinia.schema :as ls]
            [graphql-qb.config :as config])
  (:import [graphql_qb.types RefPeriodType RefAreaType EnumType]))

;;TODO: add/use spec for graphql enum values
(s/def ::graphql-enum keyword?)

(defprotocol ArgumentTransform
  (transform-argument [this graphql-value]))

(defprotocol ResultTransform
  (transform-result [this inner-value]))

(defrecord EnumMappingItem [name value label])

(defn find-item-by-name [name items]
  (util/find-first #(= name (:name %)) items))

(defrecord EnumMapping [label doc items]
  ArgumentTransform
  (transform-argument [_this graphql-value]
    (:value (find-item-by-name graphql-value items)))

  ResultTransform
  (transform-result [_this result]
    (->> items
         (util/find-first (fn [{:keys [value]}]
                            (= value result)))
         (:name))))

(defrecord FloatMeasureMapping []
  ResultTransform
  (transform-result [_this r] (some-> r double)))

(defrecord StringMeasureMapping []
  ResultTransform
  (transform-result [_this r] (str r)))

(defn map-transform [tm m trans-fn]
  (into {} (map (fn [[k transform]]
                  (let [v (get m k)]
                    [k (trans-fn transform v)]))
                tm)))

(defrecord FnTransform [f]
  ArgumentTransform
  (transform-argument [_this v] (f v))

  ResultTransform
  (transform-result [_this r] (f r)))

(defn ftrans [f] (->FnTransform f))
(def idtrans (->FnTransform identity))

(defn apply-map-argument-transform [tm m]
  (map-transform tm m #(transform-argument %1 %2)))

(defn apply-map-result-transform [rm m]
  (map-transform rm m #(transform-result %1 %2)))

(defrecord MapTransform [tm]
  ArgumentTransform
  (transform-argument [_this m] (apply-map-argument-transform tm m))
  ResultTransform
  (transform-result [_this r] (apply-map-result-transform tm r)))

(defrecord SeqTransform [item-transform]
  ArgumentTransform
  (transform-argument [_this v]
    (mapv #(transform-argument item-transform %) v))

  ResultTransform
  (transform-result [_this r]
    (mapv #(transform-result item-transform %) r)))

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

(defn dataset-dimensions-measures-enum-group [dataset]
  (let [schema (types/dataset-schema dataset)
        mapping-name (keyword (str (name schema) "_dimension_measures"))]
    (create-group-mapping mapping-name (types/dataset-dimension-measures dataset) :uri)))

(defn dataset-aggregation-measures-enum-group [dataset]
  (if-let [aggregation-measures (seq (types/dataset-aggregate-measures dataset))]
    (let [schema (types/dataset-schema dataset)
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
    (->EnumMapping enum-label enum-doc (vec items))))

;;TODO: replace this with something better
(defprotocol TypeDimensionMapping
  (get-dimension-mapping [type field-name field-enum-mappings]))

(extend-protocol TypeDimensionMapping
  RefPeriodType
  (get-dimension-mapping [_type _field-name _field-enum-mappings]
    idtrans)

  RefAreaType
  (get-dimension-mapping [_type _field-name _field-enum-mappings]
    idtrans)

  EnumType
  (get-dimension-mapping [_type field-name field-enum-mappings]
    (get field-enum-mappings field-name)))

(defn get-dataset-dimensions-mapping [dataset field-enum-mappings]
  (let [dim-mapping (map (fn [{:keys [type] :as dim}]
                           (let [field-name (types/->field-name dim)]
                             [field-name (get-dimension-mapping type field-name field-enum-mappings)]))
                         (types/dataset-dimensions dataset))]
    (into {} dim-mapping)))

(defn get-dataset-observations-argument-mapping [dataset field-enum-mappings]
  (let [dimensions-mapping (get-dataset-dimensions-mapping dataset field-enum-mappings)
        dim-measures-enum (dataset-dimensions-measures-enum-group dataset)]
    (->MapTransform {:dimensions (->MapTransform dimensions-mapping)
                     :order      (->SeqTransform dim-measures-enum)
                     :order_spec idtrans})))


(defn- get-measure-mapping [m]
  (types/is-numeric-measure? m) (->FloatMeasureMapping) (->StringMeasureMapping))

(defn get-dataset-observations-result-mapping [dataset field-enum-mappings]
  (let [dim-mapping (get-dataset-dimensions-mapping dataset field-enum-mappings)
        measure-mapping (map (fn [measure]
                               [(types/->field-name measure) (get-measure-mapping measure)])
                             (types/dataset-measures dataset))
        obs-mapping (merge (into {:uri idtrans} measure-mapping)
                           dim-mapping)]
    (->MapTransform obs-mapping)))

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
                                    codelist (get-dimension-codelist dim-members configuration)
                                    field-name (types/->field-name dimension)]
                                [field-name (create-enum-mapping dim-label enum-doc codelist)]))
                            dimension-member-bindings)]
    (into {} field-mappings)))

;;TODO: move to core namespace? parameterise by available mapping handlers?
(defn get-datasets-enum-mappings [datasets codelist-member-bindings configuration]
  (let [ds-members (group-by :ds codelist-member-bindings)
        dataset-mappings (map (fn [dataset]
                                (let [ds-uri (:uri dataset)
                                      ds-codelist-member-bindings (get ds-members ds-uri)]
                                  [ds-uri (get-dataset-enum-mappings dataset ds-codelist-member-bindings configuration)]))
                              datasets)]
    (into {} dataset-mappings)))

;;schema mappings

(defn enum-type-name [dataset field-name]
  (types/field-name->type-name field-name (types/dataset-schema dataset)))

(defn enum-mapping->schema [dataset field-name {:keys [doc items] :as enum-mapping}]
  (let [type-name (enum-type-name dataset field-name)]
    (if (some? doc)
      {type-name {:values (mapv :name items) :description doc}}
      {type-name {:values (mapv :name items)}})))

(defn enum-mapping-item->dimension-value [codelist-item->label {:keys [name value]}]
  (ls/tag-with-type
    {:uri value :label (get codelist-item->label value) :enum_name (clojure.core/name name)}
    :enum_dim_value))

(defn non-enum-item->dimension-value [{:keys [member label]}]
  (ls/tag-with-type
    {:uri member :label label}
    :unmapped_dim_value))

(defn dataset-enum-types-schema [dataset enum-mappings]
  (apply merge (map (fn [[field-name enum-mapping]]
                      (enum-mapping->schema dataset field-name enum-mapping))
                    enum-mappings)))

(defn get-measure-mappings [datasets]
  (let [ds-measures (map (fn [dataset]
                           (let [measures (mapv (fn [{:keys [uri label] :as m}]
                                                  {:uri uri :label label :enum_name (name (label->enum-name (str label)))})
                                                (types/dataset-measures dataset))]
                             [(:uri dataset) measures]))
                         datasets)]
    (into {} ds-measures)))

;;TODO: add protocol instead of using type switch
(defn- is-enum-type? [type]
  (instance? EnumType type))

(defn format-dataset-dimension-values [dataset enum-dimension-mappings dimension-uri->codelist]
  (mapv (fn [{:keys [uri type] :as dim}]
          (let [dim-codelist (get dimension-uri->codelist uri)
                field-name (types/->field-name dim)]
            ;;TODO: remove type switch
            (if (is-enum-type? type)
              (let [{:keys [label items] :as enum-mapping} (get enum-dimension-mappings field-name)
                    codelist-item->label (into {} (map (juxt :member :label) dim-codelist))]
                {:uri       uri
                 :enum_name (name (label->enum-name label))
                 :values    (mapv #(enum-mapping-item->dimension-value codelist-item->label %) items)})
              {:uri uri
               :enum_name (name (label->enum-name (:label dim)))
               :values (mapv non-enum-item->dimension-value dim-codelist)})))
        (types/dataset-dimensions dataset)))