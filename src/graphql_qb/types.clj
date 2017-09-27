(ns graphql-qb.types
  "Functions for mapping DSD elements to/from GraphQL types"
  (:require [clojure.string :as string]
            [graphql-qb.util :as util])
  (:import [java.net URI]
           [java.util Base64]))

(defn parse-year [year-str]
  (let [year (Integer/parseInt year-str)]
    (URI. (str "http://reference.data.gov.uk/id/year/" year))))

(defn parse-geography [geo-code]
  (URI. (str "http://statistics.gov.scot/id/statistical-geography/" geo-code)))

(defn uri->last-path-segment [uri]
  (last (string/split (.getPath uri) #"/")))

(def serialise-year uri->last-path-segment)
(def serialise-geography uri->last-path-segment)

(defn parse-sparql-cursor [base64-str]
  (let [bytes (.decode (Base64/getDecoder) base64-str)
        offset (util/bytes->long bytes)]
    (if (neg? offset)
      (throw (IllegalArgumentException. "Invalid cursor"))
      offset)))

(defn serialise-sparql-cursor [offset]
  {:pre [(>= offset 0)]}
  (let [bytes (util/long->bytes offset)
        enc (Base64/getEncoder)]
    (.encodeToString enc bytes)))

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

(defn- segments->enum-value [segments]
  (->> segments
       (map string/upper-case)
       (string/join "_")
       (keyword)))

(defn dataset-label->schema-name [label]
  (segments->schema-key (cons "dataset" (get-identifier-segments label))))

(defn label->field-name [label]
  (segments->schema-key (get-identifier-segments label)))

(defn ->field-name [{:keys [label]}]
  (label->field-name label))

(def label->enum-name label->field-name)

(defn enum-label->value-name [label]
  (segments->enum-value (get-identifier-segments label)))

(defn field-name->type-name [field-name ds-schema]
  (keyword (str (name ds-schema) "_" (name field-name) "_type")))

(defprotocol SparqlQueryable
  (->query-var-name [this])
  (->order-by-var-name [this])
  (get-order-by-bgps [this]))

(defprotocol SchemaType
  (type-name [this]))

(defprotocol EnumTypeSource
  (get-enums [this]))

(defprotocol SchemaElement
  (->schema-element [this]))

(defprotocol TypeMapper
  (from-graphql [this graphql-value])
  (to-graphql [this value]))

(defprotocol EnumValue
  (to-enum-value [this]))

(def id-mapper
  {:from-graphql (fn [_this v] v)
   :to-graphql (fn [_this v] v)})

(defrecord RefAreaType []
  EnumTypeSource
  (get-enums [_this] nil)
  
  SchemaType
  (type-name [_this] :ref_area))

(extend RefAreaType TypeMapper id-mapper)

(defrecord RefPeriodType []
  EnumTypeSource
  (get-enums [_this] nil)
  
  SchemaType
  (type-name [_this] :year))

(extend RefPeriodType TypeMapper id-mapper)

(defrecord EnumItem [value label name sort-priority])

(defrecord EnumType [schema enum-name values]
  SchemaType
  (type-name [this]
    (field-name->type-name enum-name schema))
  
  EnumTypeSource
  (get-enums [this] [this])
  
  TypeMapper
  (from-graphql [_this item-name]
    (if-let [item (first (filter #(= item-name (:name %)) values))]
      (:value item)))

  (to-graphql [_this value]
    (if-let [item (first (filter #(= value (:value %)) values))]
      (:name item))))

(defn is-enum-type? [type]
  (instance? EnumType type))

(defn is-ref-area-type? [type]
  (instance? RefAreaType type))

(defn is-ref-period-type? [type]
  (instance? RefPeriodType type))

(defrecord Dimension [uri ds-uri schema label doc order type]
  SparqlQueryable
  (->query-var-name [_this]
    (str "dim" order))
  
  (->order-by-var-name [this]
    (if (is-ref-area-type? type)
      (str "dim" order "label")
      (->query-var-name this)))

  (get-order-by-bgps [this]
    (if (is-ref-area-type? type)
      [(str "OPTIONAL { ?"(->query-var-name this) " rdfs:label ?" (->order-by-var-name this) " }")]
      []))

  TypeMapper
  (from-graphql [this graphql-value]
    (from-graphql type graphql-value))

  (to-graphql [this binding]
    (to-graphql type binding))

  SchemaType
  (type-name [this]
    (type-name type))

  SchemaElement
  (->schema-element [this]
    {(->field-name this) {:type (type-name this)
                          :description (some-> (or doc label) str)}})

  EnumTypeSource
  (get-enums [this]
    (get-enums type))

  EnumValue
  (to-enum-value [this]
    (->EnumItem this label (enum-label->value-name label) nil)))

(defrecord MeasureType [uri label order is-numeric?]
  SparqlQueryable
  (->query-var-name [_this]
    (str "mt" order))

  (->order-by-var-name [this]
    (->query-var-name this))

  (get-order-by-bgps [this]
    [])

  TypeMapper
  (from-graphql [this graphql-value]
    (throw (IllegalStateException. "Not implemented!")))
  (to-graphql [this binding]
    (some-> binding str))

  SchemaElement
  (->schema-element [this]
    {(->field-name this) {:type 'String}})

  EnumTypeSource
  (get-enums [_this] nil)

  EnumValue
  (to-enum-value [this]
    (->EnumItem this label (enum-label->value-name label) nil)))

(defrecord Dataset [uri title description dimensions measures]
  EnumTypeSource
  (get-enums [_this]
    (mapcat get-enums (concat dimensions measures))))

(defn dataset-schema [{:keys [title] :as ds}]
  (keyword (dataset-label->schema-name title)))

(defn dataset-aggregate-measures [{:keys [measures] :as ds}]
  (filter :is-numeric? measures))

(defn build-enum [schema enum-name values]
  (->EnumType schema enum-name (mapv to-enum-value values)))
