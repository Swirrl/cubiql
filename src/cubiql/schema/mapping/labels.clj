(ns cubiql.schema.mapping.labels
  "Creates GraphQL schema mappings from the labels associated with types and values"
  (:require [clojure.spec.alpha :as s]
            [cubiql.util :as util]
            [clojure.string :as string]
            [cubiql.types :as types]
            [cubiql.config :as config]
            [cubiql.queries :as queries]
            [cubiql.dataset-model :as dsm]))

;;TODO: add/use spec for graphql enum values
(s/def ::graphql-enum keyword?)

(defn get-identifier-segments [label]
  (let [segments (re-seq #"[a-zA-Z0-9]+" (str label))]
    (if (empty? segments)
      (throw (IllegalArgumentException. (format "Cannot construct identifier from label '%s'" label)))
      (let [^Character first-char (ffirst segments)]
        (if (Character/isDigit first-char)
          (cons "a" segments)
          segments)))))

(defn- segments->enum-value [segments]
  (->> segments
       (map string/upper-case)
       (string/join "_")
       (keyword)))

(defn segments->schema-key [segments]
  (->> segments
       (map string/lower-case)
       (string/join "_")
       (keyword)))

(defn label->field-name [label]
  (segments->schema-key (get-identifier-segments label)))

(defn label->enum-name
  ([label]
   (segments->enum-value (get-identifier-segments label)))
  ([label n]
   (let [label-segments (get-identifier-segments label)]
     (segments->enum-value (concat label-segments [(str n)])))))

(defn create-group-mapping
  ([name mappings] (create-group-mapping name mappings identity))
  ([name mappings val-f]
    ;;TODO: handle multiple mappings to the same label
   (let [items (mapv (fn [{:keys [label] :as mapping}]
                       (types/->EnumMappingItem (label->enum-name label) (val-f mapping) label))
                     mappings)]
     (types/->GroupMapping name items))))

(defn components-enum-group [schema components]
  (let [mapping-name (keyword (str (name schema) "_dimension_measures"))]
    (create-group-mapping mapping-name components :uri)))

(defn aggregation-measures-enum-group [schema measures]
  (if-let [aggregation-measures (seq (filter :is-numeric? measures))]
    (let [mapping-name (keyword (str (name schema) "_aggregation_measures"))]
      (create-group-mapping mapping-name aggregation-measures :uri))))

(defn create-enum-mapping [enum-label enum-doc code-list]
  (let [by-enum-name (group-by #(label->enum-name (:label %)) code-list)
        items (mapcat (fn [[enum-name item-results]]
                        (if (= 1 (count item-results))
                          (map (fn [{:keys [member label]}]
                                 (types/->EnumMappingItem enum-name member label))
                               item-results)
                          (map-indexed (fn [n {:keys [member label]}]
                                         (types/->EnumMappingItem (label->enum-name label (inc n)) member label))
                                       item-results)))
                      by-enum-name)]
    {:label enum-label :doc (or enum-doc "") :items (vec items)}))

(defn get-measure-type
  "Returns the mapped datatype for the given measure"
  [m]
  (if (types/is-numeric-measure? m)
    types/float-measure-type
    types/string-measure-type))

(defn get-dimension-codelist [dimension-member-bindings configuration]
  (map (fn [[member-uri member-bindings]]
         (let [labels (map :vallabel member-bindings)]
           {:member member-uri :label (util/find-best-language labels (config/schema-label-language configuration))}))
       (group-by :member dimension-member-bindings)))

(defn- get-dataset-enum-mappings [dataset-member-bindings dimension-labels configuration]
  (let [dimension-member-bindings (group-by :dim dataset-member-bindings)
        field-mappings (map (fn [[dim-uri dim-members]]
                              (let [{dim-label :label enum-doc :doc} (get dimension-labels dim-uri)
                                    codelist (get-dimension-codelist dim-members configuration)]
                                [dim-uri (create-enum-mapping dim-label enum-doc codelist)]))
                            dimension-member-bindings)]
    (into {} field-mappings)))

(defn get-datasets-enum-mappings [datasets codelist-member-bindings dimension-labels configuration]
  (let [ds-members (group-by :ds codelist-member-bindings)
        dataset-mappings (map (fn [dataset]
                                (let [ds-uri (:uri dataset)
                                      ds-codelist-member-bindings (get ds-members ds-uri)]
                                  [ds-uri (get-dataset-enum-mappings ds-codelist-member-bindings dimension-labels configuration)]))
                              datasets)]
    (into {} dataset-mappings)))

;;schema mappings

(defn get-all-enum-mappings [repo datasets dimension-labels config]
  (let [enum-dimension-values-query (queries/get-all-enum-dimension-values config)
        results (util/eager-query repo enum-dimension-values-query)
        dataset-enum-values (map (util/convert-binding-labels [:vallabel]) results)]
    (get-datasets-enum-mappings datasets dataset-enum-values dimension-labels config)))

(defn field-name->type-name [field-name ds-schema]
  (keyword (str (name ds-schema) "_" (name field-name) "_type")))

(defn identify-dimension-labels [dimension-bindings configuration]
  (util/map-values (fn [bindings]
                     (let [{:keys [label doc]} (util/to-multimap bindings)]
                       {:label (util/find-best-language label (config/schema-label-language configuration))
                        :doc   (util/find-best-language doc (config/schema-label-language configuration))}))
                   (group-by :dim dimension-bindings)))

(defn identify-measure-labels [measure-bindings configuration]
  (util/map-values (fn [bindings]
                     (let [labels (map :label bindings)]
                       (util/find-best-language labels (config/schema-label-language configuration))))
                   (group-by :measure measure-bindings)))

(defn find-dimension-labels [repo configuration]
  (let [q (queries/get-dimension-labels-query configuration)
        results (util/eager-query repo q)]
    (identify-dimension-labels results configuration)))

(defn find-measure-labels [repo configuration]
  (let [q (queries/get-measure-labels-query configuration)
        results (util/eager-query repo q)]
    (identify-measure-labels results configuration)))

(defn- measure->enum-item [{:keys [label enum-name uri] :as measure}]
  (types/->EnumMappingItem enum-name uri label))

(defn measures-enum-type [enum-name measures doc]
  (let [items (mapv measure->enum-item measures)]
    (types/->MappedEnumType enum-name types/measure-dimension-type doc items)))

(defn- get-dimension-mapping [schema {:keys [uri] :as dimension} ds-enum-mappings {:keys [label doc]} measures]
  (let [dimension-type (:type dimension)
        field-name (label->field-name label)
        enum-name (field-name->type-name field-name schema)
        mapped-type (cond
                      (contains? ds-enum-mappings uri)
                      (let [enum-mapping (get ds-enum-mappings uri)]
                        (types/->MappedEnumType enum-name dimension-type (:doc enum-mapping) (:items enum-mapping)))

                      (dsm/is-measure-type-dimension? dimension)
                      (measures-enum-type enum-name measures doc)

                      :else
                      dimension-type)]
    {:uri        uri
     :label      label
     :doc        doc
     :field-name field-name
     :enum-name (label->enum-name label)
     :type       mapped-type
     :dimension  dimension}))

(defn- get-measure-mapping [{:keys [uri is-numeric?] :as measure} label]
  {:uri uri
   :label label
   :field-name (label->field-name label)
   :enum-name (label->enum-name label)
   :type (get-measure-type measure)
   :is-numeric? is-numeric?
   :measure measure})

(defn dataset-name->schema-name [label]
  (segments->schema-key (cons "dataset" (get-identifier-segments label))))

(defn dataset-schema [ds]
  (keyword (dataset-name->schema-name (:name ds))))

(defn resolve-dimension-labels [{:keys [uri] :as dimension} dimension-uri->labels]
  (if (dsm/is-measure-type-dimension? dimension)
    (merge (get dimension-uri->labels uri) {:label "Measure type" :doc "Generic measure type dimension"})
    (util/strict-get dimension-uri->labels uri)))

(defn build-dataset-mapping-model [{:keys [uri] :as dataset} ds-enum-mappings dimension-labels measure-labels]
  (let [schema (dataset-schema dataset)
        measures (mapv (fn [{:keys [uri] :as measure}]
                         (let [label (util/strict-get measure-labels uri)]
                           (get-measure-mapping measure label)))
                       (types/dataset-measures dataset))
        dimensions (mapv (fn [dim]
                           (let [labels (resolve-dimension-labels dim dimension-labels)]
                             (get-dimension-mapping schema dim ds-enum-mappings labels measures)))
                         (types/dataset-dimensions dataset))]
    {:uri uri
     :schema schema
     :dimensions dimensions
     :measures measures
     :components-enum (components-enum-group schema (concat dimensions measures))
     :aggregation-measures-enum (aggregation-measures-enum-group schema measures)}))

(defn get-dataset-mapping-models [repo datasets configuration]
  (let [dimension-labels (find-dimension-labels repo configuration)
        measure-labels (find-measure-labels repo configuration)
        enum-mappings (get-all-enum-mappings repo datasets dimension-labels configuration)]
    (mapv (fn [{:keys [uri] :as ds}]
            (let [ds-enum-mappings (get enum-mappings uri {})]
              (build-dataset-mapping-model ds ds-enum-mappings dimension-labels measure-labels)))
          datasets)))
