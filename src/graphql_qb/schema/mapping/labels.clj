(ns graphql-qb.schema.mapping.labels
  "Creates GraphQL schema mappings from the labels associated with types and values"
  (:require [clojure.spec.alpha :as s]
            [graphql-qb.util :as util]
            [clojure.string :as string]
            [clojure.pprint :as pp]
            [graphql-qb.types :as types]
            [com.walmartlabs.lacinia.schema :as ls]))

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

(defrecord MeasureMapping []
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
  (if-let [aggregation-measures (types/dataset-aggregate-measures dataset)]
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

(defn get-dataset-dimensions-mapping [dataset field-enum-mappings]
  (let [dim-mapping (map (fn [{:keys [field-name type] :as dim}]
                           (cond
                             (types/is-enum-type? type)
                             [field-name (get field-enum-mappings field-name)]
                             (types/is-ref-area-type? type)
                             [field-name idtrans]
                             (types/is-ref-period-type? type)
                             [field-name idtrans]))
                         (:dimensions dataset))]
    (into {} dim-mapping)))

(defn get-dataset-observations-argument-mapping [dataset field-enum-mappings]
  (let [dimensions-mapping (get-dataset-dimensions-mapping dataset field-enum-mappings)
        dim-measures-enum (dataset-dimensions-measures-enum-group dataset)]
    (->MapTransform {:dimensions (->MapTransform dimensions-mapping)
                     :order      (->SeqTransform dim-measures-enum)
                     :order_spec idtrans})))

(defn get-dataset-observations-result-mapping [dataset field-enum-mappings]
  (let [dim-mapping (get-dataset-dimensions-mapping dataset field-enum-mappings)
        measure-mapping (map (fn [{:keys [field-name] :as measure}]
                               [field-name (->MeasureMapping)])
                             (:measures dataset))
        obs-mapping (merge (into {:uri idtrans} measure-mapping)
                           dim-mapping)]
    (->MapTransform obs-mapping)))

(defn get-dataset-enum-mappings [enum-values]
  (let [dim-enums (group-by :dim enum-values)
        m (map (fn [[dim values]]
                 (let [enum-label (str (:label (first values)))
                       enum-doc (some-> (:doc (first values)) str)
                       field-name (types/label->field-name enum-label)
                       code-list (map #(util/rename-key % :vallabel :label) values)]
                   [field-name (create-enum-mapping enum-label enum-doc code-list)]))
               dim-enums)]
    (into {} m)))

;;TODO: move to core namespace? parameterise by available mapping handlers?
(defn get-datasets-enum-mappings [all-enum-values]
  (let [ds-enums (group-by :ds all-enum-values)]
    (util/map-values get-dataset-enum-mappings ds-enums)))

;;schema mappings

;;TODO: remove schema/enum-type-name
(defn enum-type-name [dataset field-name]
  (types/field-name->type-name field-name (types/dataset-schema dataset)))

(defn enum-mapping->schema [dataset field-name {:keys [doc items] :as enum-mapping}]
  (let [type-name (enum-type-name dataset field-name)]
    (if (some? doc)
      {type-name {:values (mapv :name items) :description doc}}
      {type-name {:values (mapv :name items)}})))

(defn enum-mapping-item->dimension-value [{:keys [name value label]}]
  (ls/tag-with-type
    {:uri value :label label :enum_name (clojure.core/name name)}
    :enum_dim_value))

(defn non-enum-item->dimension-value [{:keys [member label]}]
  (ls/tag-with-type
    {:uri member :label label}
    :unmapped_dim_value))

(defn dataset-enum-types-schema [dataset enum-mappings]
  (apply merge (map (fn [[field-name enum-mapping]]
                      (enum-mapping->schema dataset field-name enum-mapping))
                    enum-mappings)))

(defn get-measure-mappings [all-measure-values]
  (util/map-values
    (fn [ds-measures]
      (mapv (fn [{:keys [mt label]}]
              {:uri mt :label label :enum_name (name (label->enum-name (str label)))})
            ds-measures))
    (group-by :ds all-measure-values)))


(defn format-dataset-dimension-values [dataset enum-dimension-mappings non-enum-dimension-mappings]
  (mapv
    (fn [{:keys [uri type field-name] :as dim}]
      (if (types/is-enum-type? type)
        (let [{:keys [label items] :as enum-mapping} (get enum-dimension-mappings field-name)]
          {:uri uri
           :enum_name (name (label->enum-name label))
           :values (mapv enum-mapping-item->dimension-value items)})
        (let [values (get non-enum-dimension-mappings uri)]
          ;;TODO: add mapping for non-enum dimensions containing the label
          {:uri uri
           :enum_name (name (label->enum-name (:label dim)))
           :values (mapv non-enum-item->dimension-value values)})))
    (types/dataset-dimensions dataset)))