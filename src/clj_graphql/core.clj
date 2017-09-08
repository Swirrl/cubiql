(ns clj-graphql.core
  (:require [grafter.rdf.repository :as repo]
            [clojure.string :as string]
            [com.walmartlabs.lacinia.schema :as schema]
            [com.walmartlabs.lacinia.util :refer [attach-resolvers]]
            [com.walmartlabs.lacinia :refer [execute]]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [clojure.set :as set]
            [clj-graphql.util :refer [read-edn-resource rename-key]])
  (:import [java.net URI]
           [java.io PushbackReader]))

(defn get-dimensions-query [ds-uri]
  (str
   "PREFIX qb: <http://purl.org/linked-data/cube#>"
   "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
   "SELECT ?dim ?order ?label ?doc ?ds WHERE {"
   "  <" ds-uri "> a qb:DataSet ."
   "  <" ds-uri "> qb:structure ?struct ."
   "  ?struct a qb:DataStructureDefinition ."
   "  ?struct qb:component ?comp ."
   "  ?comp a qb:ComponentSpecification ."
   "  ?comp qb:order ?order ."
   "  ?comp qb:dimension ?dim ."
   "  ?dim rdfs:label ?label ."
   "  OPTIONAL { ?dim rdfs:comment ?doc }"
   "  BIND(<" ds-uri "> as ?ds) ."
   "}"))

(defn get-enum-values-query [ds-uri dim-uri]
  (str
   "PREFIX qb: <http://purl.org/linked-data/cube#>"
   "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"   
   "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
   "SELECT ?member ?label WHERE {"
   "  <" ds-uri "> a qb:DataSet ."
   "  <" ds-uri "> qb:structure ?struct ."
   "  ?struct a qb:DataStructureDefinition ."
   "  ?struct qb:component ?comp ."
   "  ?comp qb:dimension <" dim-uri "> ."
   "  ?comp qb:codeList ?list ."
   #_"  ?list a skos:Collection ."
   "  ?list skos:member ?member ."
   "  ?member rdfs:label ?label ."
   "}"))

(defn parse-year [year-str]
  (let [year (Integer/parseInt year-str)]
    (URI. (str "http://reference.data.gov.uk/id/year/" year))))

(defn uri->last-path-segment [uri]
  (last (string/split (.getPath uri) #"/")))

(defn label->field-name [label]
  (keyword (string/join "_" (map string/lower-case (string/split (str label) #"\s+")))))

(defn ->field-name [{:keys [label]}]
  (label->field-name label))

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

(defn get-enum-values [repo {:keys [ds-uri uri] :as dim}]
  (let [results (repo/query repo (get-enum-values-query ds-uri uri))]
    (map (fn [{:keys [label] :as m}]
           (assoc m :value (enum-label->value-name label)))
         results)))

(defn get-dimension-type [repo {:keys [uri label] :as dim} {:keys [schema] :as ds}]
  (cond
    (= (URI. "http://purl.org/linked-data/sdmx/2009/dimension#refArea") uri)
    {:type :ref_area
     :kind :scalar
     :parse (schema/as-conformer #(URI. (str "http://statistics.gov.scot/id/statistical-geography/" %)))
     :serialize (schema/as-conformer uri->last-path-segment)
     :value->dimension-uri identity
     :result-binding->value identity}
    
    (= (URI. "http://purl.org/linked-data/sdmx/2009/dimension#refPeriod") uri)
    {:type :year
     :kind :scalar
     :parse (schema/as-conformer parse-year)
     :serialize (schema/as-conformer uri->last-path-segment)
     :value->dimension-uri identity
     :result-binding->value identity}
    
    :else
    (let [values (get-enum-values repo dim)
          value->uri (into {} (map (juxt :value :member) values))]
      {:kind :enum
       :type (->type-name dim schema)
       :values values
       :value->dimension-uri value->uri
       :result-binding->value (set/map-invert value->uri)})))

(defn get-test-repo []
  (repo/fixture-repo
   "earnings.nt"
   "earnings_metadata.nt"
   "dimension_pos.nt"
   "member_labels.nt"
   "measure_properties.nt"
   "healthy_life_expectancy.nt"
   "healthy_life_expectancy_metadata.nt"))

(defn dimension->query-var-name [{:keys [order]}]
  (str "dim" order))

(defn get-dimensions
  [repo {:keys [uri] :as ds}]
  (let [base-dims (repo/query repo (get-dimensions-query uri))]
    (map (fn [bindings]
           (let [dim (-> bindings
                         (rename-key :dim :uri)
                         (rename-key :ds :ds-uri))]
             (-> dim
                 (merge (get-dimension-type repo dim ds))
                 (assoc :field-name (->field-name dim))
                 (assoc :->query-var-name dimension->query-var-name))))
         base-dims)))

(defn get-measure-types-query []
  (str
   "PREFIX qb: <http://purl.org/linked-data/cube#>"
   "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
   "SELECT ?mt ?label WHERE {"
   "  ?ds qb:structure ?struct ."
   "  ?struct qb:component ?comp ."
   "  ?comp qb:measure ?mt ."
   "  ?mt a qb:MeasureProperty ."
   "  ?mt rdfs:label ?label ."
   "}"))

(defn measure-type->query-var-name [{:keys [order]}]
  (str "mt" order))

(defn get-measure-types [repo]
  (let [q (get-measure-types-query)
        results (repo/query repo q)]
    (map-indexed (fn [idx bindings]
                   (-> bindings
                       (rename-key :mt :uri)
                       (assoc :field-name (->field-name bindings))
                       (assoc :order (inc idx))
                       (assoc :->query-var-name measure-type->query-var-name)
                       (assoc :result-binding->value #(some-> % str)))) results)))

(defn get-measure-type-schemas [repo]
  (let [measure-types (get-measure-types repo)]
    (map (fn [{:keys [field-name]}] {field-name {:type 'String}}) measure-types)))

(defn dimensions->obs-dim-schemas [dims]
  (map (fn [{:keys [field-name type] :as dim}]
         [field-name {:type type}])
       dims))

(defn dim-has-kind? [kind dim]
  (= kind (:kind dim)))

(defn is-enum? [dim]
  (dim-has-kind? :enum dim))

(defn is-scalar? [dim]
  (dim-has-kind? :scalar dim))

(defn dimensions->enums-schema [dims]
  (let [enum-dims (filter is-enum? dims)
        enum-defs (map (fn [{:keys [values type label] :as d}]
                         [type {:values (mapv :value values)
                                :description (str label)}])
                       enum-dims)]
    (into {} enum-defs)))

(defn dimensions->scalars-schema [dims]
  (let [scalar-dims (filter is-scalar? dims)]
    (into {} (map (fn [{:keys [type parse serialize]}]
                    [type {:parse parse :serialize serialize}])
                  scalar-dims))))

(defn resolve-dataset-dimensions [{:keys [repo] :as context} args {:keys [uri] :as ds-field}]
  ;;TODO: remove hard-coded schema name!
  (let [dims (get-dimensions repo {:uri uri :schema :dataset_earnings})]
    (map (fn [{:keys [uri values]}]
           {:uri (str uri)
            :values (map (fn [{:keys [member label]}]
                           {:uri (str member)
                            :label label})
                         values)})
         dims)))

(def dataset-query
  (str
   "SELECT ?uri ?title ?description WHERE {"
   "  ?ds a <http://publishmydata.com/def/dataset#Dataset> ."
   "  ?ds <http://purl.org/dc/terms/title> ?title ."
   "  ?ds <http://www.w3.org/2000/01/rdf-schema#comment> ?description ."
   "  BIND(?ds as ?uri)"
   "}"))

(defn get-dataset [repo]
  (first (repo/query repo dataset-query)))

(defn resolve-dataset-earnings [{:keys [repo] :as context} args field]
  (let [ds (get-dataset repo)
        dims (resolve-dataset-dimensions context args {:uri (URI. "http://statistics.gov.scot/data/earnings")})]
    (assoc ds :dimensions dims)))

(defn get-observation-query [ds-dimensions query-dimensions measure-types]
  (let [is-query-dimension? (fn [{:keys [field-name]}] (contains? query-dimensions field-name))
        constrained-dims (filter is-query-dimension? ds-dimensions)
        free-dims (remove is-query-dimension? ds-dimensions)
        constrained-patterns (map (fn [{:keys [field-name uri value->dimension-uri] :as dim}]
                                    (let [field-value (get query-dimensions field-name)
                                          val-uri (value->dimension-uri field-value)]
                                      (str "?obs <" (str uri) "> <" (str val-uri) "> .")))
                                  constrained-dims)
        measure-type-patterns (map (fn [{:keys [field-name uri] :as mt}]
                                     (str
                                      "  ?obs qb:measureType <" (str uri) "> . \n" 
                                      "  ?obs <" (str uri) "> ?" (measure-type->query-var-name mt) " ."))                                   
                                   measure-types)
        binds (map (fn [{:keys [field-name value->dimension-uri] :as dim}]
                     (let [field-value (get query-dimensions field-name)
                           val-uri (value->dimension-uri field-value)
                           var-name (dimension->query-var-name dim)]
                       (str "BIND(<" val-uri "> as ?" var-name ") .")))
                   constrained-dims)
        query-patterns (map (fn [{:keys [uri] :as dim}]
                              (let [var-name (dimension->query-var-name dim)]
                                (str "?obs <" uri "> ?" var-name " .")))
                            free-dims)]
    (str
     "PREFIX qb: <http://purl.org/linked-data/cube#>"
     "SELECT * WHERE {"
     "  ?obs a qb:Observation ."
     "  ?obs qb:dataSet <http://statistics.gov.scot/data/earnings> ."
     "  ?obs qb:measureType ?measureType ."
     "  ?obs ?measureType ?value ."
     (string/join "\n" measure-type-patterns)
     (string/join "\n" constrained-patterns)
     (string/join "\n" query-patterns)
     (string/join "\n" binds)
     "}")))

(defn resolve-observations [{:keys [repo dimensions measure-types] :as context} {query-dimensions :dimensions :as args} {:keys [uri] :as ds-field}]
  (let [query (get-observation-query dimensions query-dimensions measure-types)
        results (repo/query repo query)
        matches (mapv (fn [{:keys [obs] :as bindings}]
                        (let [field-values (map (fn [{:keys [field-name ->query-var-name result-binding->value] :as ft}]
                                                  (let [result-key (keyword (->query-var-name ft))
                                                        value (get bindings result-key)]
                                                    [field-name (result-binding->value value)]))
                                                (concat dimensions measure-types))]
                          (into {:uri obs} field-values)))
                      results)]
    {:matches matches
     :free_dimensions []}))

(defn get-dimensions-filter [{dims-and :and}]
  (if (empty? dims-and)
    ""
    (let [and-clauses (map-indexed (fn [idx uri]
                                     (let [comp-var (str "?comp" (inc idx))]
                                       (str
                                        "?struct qb:component " comp-var ". \n"
                                        comp-var " a qb:ComponentSpecification .\n"
                                        comp-var " qb:dimension <" (str uri) "> .\n")))
                            dims-and)]
      (str
       "  ?ds qb:structure ?struct ."
       "  ?struct a qb:DataStructureDefinition ."
       (string/join "\n" and-clauses)))))

(defn get-dimensions-or [{dims-or :or}]
  (if (empty? dims-or)
    "  ?ds a qb:DataSet ."
    (let [union-clauses (map (fn [dim]
                               (str "{ ?struct qb:component ?comp ."
                                    "  ?comp qb:dimension <" dim "> . }"))
                             dims-or)]
      (str
       "{ SELECT DISTINCT ?ds WHERE {"
       "  ?ds a qb:DataSet ."
       "  ?ds qb:structure ?struct ."
       "  ?struct a qb:DataStructureDefinition ."
       (string/join " UNION " union-clauses)
       "} }"))))

(defn get-datasets-query [dimensions measures]
  (str
   "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
   "PREFIX qb: <http://purl.org/linked-data/cube#>"
   "SELECT ?ds ?title ?description WHERE {"
   (get-dimensions-or dimensions)
   "  ?ds rdfs:label ?title ."
   "  ?ds rdfs:comment ?description ."
   (get-dimensions-filter dimensions)
   "}"))

(defn resolve-dataset [{:keys [repo]} {:keys [dimensions measures] :as args} _parent]
  (let [q (get-datasets-query dimensions measures)
        results (repo/query repo q)]
    (map #(rename-key % :ds :uri) results)))

(defn get-dataset-schema [repo {:keys [uri schema] :as ds}]
  (let [dims (get-dimensions repo ds)
        obs-dim-schemas (dimensions->obs-dim-schemas dims)
        measure-type-schemas (get-measure-type-schemas repo)
        observation-fields (into {:uri {:type :uri}} (concat obs-dim-schemas measure-type-schemas))
        enums-schema (dimensions->enums-schema dims)
        observation-result-type-name (field-name->type-name :observation_result schema)
        observation-type-name (field-name->type-name :observation schema)
        observation-dims-type-name (field-name->type-name :observation_dimensions schema)

        ;;TODO: custom scalars are global so move to top level!
        dim-scalars-schema (dimensions->scalars-schema dims)
        scalars-schema (assoc dim-scalars-schema :uri {:parse (schema/as-conformer #(URI. %))
                                                       :serialize (schema/as-conformer str)})]
    {:enums enums-schema
     :objects
     {:dataset_earnings
      {:fields
       {:uri {:type :uri}
        :title {:type 'String}
        :description {:type 'String}
        :dimensions {:type '(list :dim)}
        :observations {:type observation-result-type-name
                       :args {:dimensions {:type observation-dims-type-name}}
                       :resolve :resolve-observations}}}

      observation-result-type-name
      {:fields
       {:matches {:type (list 'list observation-type-name)}
        :free_dimensions {:type '(list :dim)}}}
      
      observation-type-name
      {:fields observation-fields}}

     :input-objects
     {observation-dims-type-name
      {:fields (into {} obs-dim-schemas)}}

     :scalars scalars-schema}))

(defn dataset-label->schema-name [label]
  (keyword (string/join "_" (cons "dataset" (map string/lower-case (string/split label #"\s+"))))))

(defn find-datasets [repo]
  (let [results (repo/query repo dataset-query)]
    (map (fn [{:keys [title] :as ds}]
           (assoc ds :schema (dataset-label->schema-name title)))
         results)))

(defn get-schema [repo]
  (let [base-schema (read-edn-resource "base-schema.edn")
        datasets (find-datasets repo)
        ds-schemas (map #(get-dataset-schema repo %) datasets)
        ;ds-schema (get-dataset-schema repo {:uri (URI. "http://statistics.gov.scot/data/earnings") :schema :dataset_earnings})
        ;combined-schema (merge-with merge base-schema ds-schema)
        combined-schema (reduce (fn [acc schema] (merge-with merge acc schema)) base-schema ds-schemas)]
    (attach-resolvers combined-schema {:resolve-dataset-earnings resolve-dataset-earnings
                                       :resolve-observations resolve-observations
                                       :resolve-dataset resolve-dataset
                                       :resolve-dataset-dimensions resolve-dataset-dimensions})))

(defn get-compiled-schema [repo]
  (schema/compile (get-schema repo)))
