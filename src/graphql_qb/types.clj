(ns graphql-qb.types
  "Functions for mapping DSD elements to/from GraphQL types"
  (:require [clojure.string :as string]
            [graphql-qb.util :as util]
            [com.walmartlabs.lacinia.schema :as lschema])
  (:import [java.net URI]
           [java.util Base64 Date]
           [java.time.format DateTimeFormatter]
           [java.time ZonedDateTime ZoneOffset]
           (org.openrdf.model Literal)))

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

(defn date->datetime
  "Converts a java.util.Date to a java.time.ZonedDateTime."
  [date]
  (ZonedDateTime/ofInstant (.toInstant date) ZoneOffset/UTC))

(defn grafter-date->datetime
  "Converts all known date literal representations used by Grafter into the corresponding
   DateTime."
  [dt]
  (cond
    (instance? Date dt)
    (date->datetime dt)

    (instance? Literal dt)
    (let [date (.. dt (calendarValue) (toGregorianCalendar) (getTime))]
      (date->datetime date))

    :else
    (throw (IllegalArgumentException. (str "Unexpected date representation: " dt)))))

(defn parse-datetime [dt-string]
  (.parse DateTimeFormatter/ISO_OFFSET_DATE_TIME dt-string))

(defn serialise-datetime [dt]
  (.format DateTimeFormatter/ISO_OFFSET_DATE_TIME dt))

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

(defn enum-label->value-name
  ([label]
   (segments->enum-value (get-identifier-segments label)))
  ([label n]
    (let [label-segments (get-identifier-segments label)]
      (segments->enum-value (concat label-segments [(str n)])))))

(defn field-name->type-name [field-name ds-schema]
  (keyword (str (name ds-schema) "_" (name field-name) "_type")))

(defprotocol SparqlFilterable
  (get-filter-bgps [this graphql-filter]))

(defprotocol SparqlQueryable
  (->query-var-name [this])
  (->order-by-var-name [this])
  (get-order-by-bgps [this])
  (get-projection-bgps [this sparql-value]))

(defprotocol SchemaType
  (input-type-name [this])
  (type-name [this]))

(defprotocol EnumTypeSource
  (get-enums [this]))

(defprotocol SchemaElement
  (->input-schema-element [this])
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
  (input-type-name [this] (type-name this))
  (type-name [_this] :uri))

(extend RefAreaType TypeMapper id-mapper)

(defrecord RefPeriodType []
  EnumTypeSource
  (get-enums [_this] nil)
  
  SchemaType
  (input-type-name [_this] :ref_period_filter)
  (type-name [_this] :uri))

(extend RefPeriodType TypeMapper id-mapper)

(defrecord EnumItem [value label name sort-priority])

(defrecord EnumType [schema enum-name values]
  SchemaType
  (input-type-name [this] (type-name this))
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

(defn format-sparql-datetime [dt]
  (str "\"" (serialise-datetime dt) "\"^^xsd:dateTime"))

(defn get-ref-period-filter [obs-var-name dim-uri dim-var-name {:keys [uri starts_before starts_after ends_before ends_after] :as filter}]
  (if (nil? filter)
    ""
    (let [obs-var (str "?" obs-var-name)
          uri-tp (if uri (str obs-var " <" dim-uri "> <" uri "> ."))
          start-filter (if (and (nil? starts_before) (nil? starts_after) (nil? ends_before) (nil? ends_after))
                         ""
                         (let [period-var (str "?" dim-var-name "period")
                               begin-var (str period-var "begin")
                               end-var (str period-var "end")
                               begin-time-var (str period-var "time")
                               end-time-var (str end-var "time")
                               begin-filter (if (and (nil? starts_before) (nil? starts_after))
                                              ""
                                              (str period-var " time:hasBeginning " begin-var " ."
                                                   begin-var " time:inXSDDateTime " begin-time-var " ."
                                                   (when (some? starts_before)
                                                     (str "FILTER(" begin-time-var " <= " (format-sparql-datetime starts_before) ")"))
                                                   (when (some? starts_after)
                                                     (str "FILTER(" begin-time-var " >= " (format-sparql-datetime starts_after) ")"))))
                               end-filter (if (and (nil? ends_before) (nil? ends_after))
                                            ""
                                            (str period-var " time:hasEnd " end-var " ."
                                                 end-var " time:inXSDDateTime " end-time-var " ."
                                                 (when (some? ends_before)
                                                   (str "FILTER(" end-time-var " <= " (format-sparql-datetime ends_before) ")"))
                                                 (when (some? ends_after)
                                                   (str "FILTER(" end-time-var " >= " (format-sparql-datetime ends_after) ")"))))]
                           (str obs-var " <" dim-uri "> " period-var " ." begin-filter end-filter)))]
      (str uri-tp start-filter))))

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

  (get-projection-bgps [this sparql-value]
    (cond
      (is-enum-type? type)
      (str "BIND(<" (from-graphql type sparql-value) "> as ?" (->query-var-name this) ")")

      (is-ref-area-type? type)
      (str "BIND(<" (from-graphql type sparql-value) "> as ?" (->query-var-name this) ")")

      (is-ref-period-type? type)
      (str "?obs <" uri "> ?" (->query-var-name this) " .")))

  SparqlFilterable
  (get-filter-bgps [this graphql-value]
    (cond
      (is-enum-type? type)
      (str "?obs <" uri "> <" (from-graphql this graphql-value) "> .")

      (is-ref-area-type? type)
      (str "?obs <" uri "> <" (from-graphql this graphql-value) "> .")

      (is-ref-period-type? type)
      (get-ref-period-filter "obs" uri (->query-var-name this) graphql-value)))

  TypeMapper
  (from-graphql [this graphql-value]
    (from-graphql type graphql-value))

  (to-graphql [this binding]
    (to-graphql type binding))

  SchemaType
  (input-type-name [this] (input-type-name type))
  (type-name [_this]
    (type-name type))

  SchemaElement
  (->input-schema-element [this]
    {(->field-name this) {:type (input-type-name this)
                          :description (some-> (or doc label) str)}})
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

(defn graphql-enum->dimension-measure [{:keys [dimensions measures] :as dataset} enum]
  (let [dm-enum (build-enum :ignored :ignored (concat dimensions measures))]
    (from-graphql dm-enum enum)))

(def custom-scalars
  {:SparqlCursor
        {:parse     (lschema/as-conformer parse-sparql-cursor)
         :serialize (lschema/as-conformer serialise-sparql-cursor)}

   :uri {:parse     (lschema/as-conformer #(URI. %))
         :serialize (lschema/as-conformer str)}

   :DateTime
        {:parse     (lschema/as-conformer parse-datetime)
         :serialize (lschema/as-conformer serialise-datetime)}})