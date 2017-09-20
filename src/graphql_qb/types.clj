(ns graphql-qb.types
  "Functions for mapping DSD elements to/from GraphQL types"
  (:require [clojure.string :as string]
            [graphql-qb.util :as util]
            [clojure.set :as set])
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

(defn dataset-label->schema-name [label]
  (keyword (string/join "_" (cons "dataset" (map string/lower-case (string/split label #"\s+"))))))

(defn label->field-name [label]
  (keyword (string/join "_" (map string/lower-case (string/split (str label) #"\s+")))))

(defn ->field-name [{:keys [label]}]
  (label->field-name label))

(def label->enum-name label->field-name)

(defn has-valid-name-first-char? [name]
  (boolean (re-find #"^[_a-zA-Z]" name)))

(defn enum-label->value-name [label]
  (let [name (string/join "_" (map string/upper-case (string/split (str label) #"\s+")))
        valid-name (if (has-valid-name-first-char? name) name (str "a_" name))]
    (keyword valid-name)))

(defn field-name->type-name [field-name ds-schema]
  (keyword (str (name ds-schema) "_" (name field-name) "_type")))

(defn ->type-name [f ds-schema]
  (field-name->type-name (->field-name f) ds-schema))

(defprotocol SparqlQueryable
  (->query-var-name [this])
  (->order-by-var-name [this])
  (get-order-by-bgps [this]))

(defprotocol SchemaType
  (type-name [this]))

(defprotocol EnumTypeSource
  (get-enums [this]))

(defprotocol SchemaElement
  (field-name [this])
  (->schema-element [this]))

(defprotocol TypeMapper
  (graphql->sparql [this graphql-value])
  (sparql->graphql [this binding-value]))

(defprotocol EnumValue
  (to-enum-value [this]))

(def id-mapper
  {:graphql->sparql (fn [_this v] v)
   :sparql->graphql (fn [_this v] v)})

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

(defrecord EnumType [schema enum-name values->uri]
  SchemaType
  (type-name [this]
    (field-name->type-name enum-name schema))
  
  EnumTypeSource
  (get-enums [this] [this])
  
  TypeMapper
  (graphql->sparql [_this value]
    (get values->uri value))

  (sparql->graphql [_this binding]
    (let [m (set/map-invert values->uri)]
      (get m binding))))

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
  (graphql->sparql [this graphql-value]
    (graphql->sparql type graphql-value))

  (sparql->graphql [this binding]
    (sparql->graphql type binding))

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
  (to-enum-value [this] [(enum-label->value-name label) this]))

(defrecord MeasureType [uri label order is-numeric?]
  SparqlQueryable
  (->query-var-name [_this]
    (str "mt" order))

  (->order-by-var-name [this]
    (->query-var-name this))

  (get-order-by-bgps [this]
    [])

  TypeMapper
  (graphql->sparql [this graphql-value]
    (throw (IllegalStateException. "Not implemented!")))
  (sparql->graphql [this binding]
    (some-> binding str))

  SchemaElement
  (->schema-element [this]
    {(->field-name this) {:type 'String}})

  EnumTypeSource
  (get-enums [_this] nil)

  EnumValue
  (to-enum-value [this] [(enum-label->value-name label) this]))

(defrecord Dataset [uri title description dimensions measures]
  EnumTypeSource
  (get-enums [_this]
    (mapcat get-enums (concat dimensions measures))))

(defn dataset-schema [{:keys [title] :as ds}]
  (keyword (dataset-label->schema-name title)))

(defn dataset-aggregate-measures [{:keys [measures] :as ds}]
  (filter :is-numeric? measures))

(defn build-enum [schema enum-name values]
  (->EnumType schema enum-name (into {} (map to-enum-value values))))
