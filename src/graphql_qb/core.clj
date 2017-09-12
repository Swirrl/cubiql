(ns graphql-qb.core
  (:require [grafter.rdf.repository :as repo]
            [grafter.rdf.sparql :as sp]
            [clojure.string :as string]
            [com.walmartlabs.lacinia.schema :as schema]
            [com.walmartlabs.lacinia.util :refer [attach-resolvers]]
            [com.walmartlabs.lacinia :refer [execute]]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [clojure.set :as set]
            [graphql-qb.util :refer [read-edn-resource rename-key]]
            [graphql-qb.types :refer :all :as types]
            [clojure.pprint :as pprint])
  (:import [java.net URI]))

(defn get-enum-values [repo {:keys [ds-uri uri] :as dim}]
  (let [results (sp/query "get-enum-values.sparql" {:ds ds-uri :dim uri} repo)]
    (map (fn [{:keys [label] :as m}]
           (assoc m :value (enum-label->value-name label)))
         results)))

(defn get-dimension-type [repo {:keys [uri label] :as dim} {:keys [schema] :as ds}]
  (cond
    (= (URI. "http://purl.org/linked-data/sdmx/2009/dimension#refArea") uri)
    {:type :ref_area
     :kind :scalar
     :parse (schema/as-conformer parse-geography)
     :serialize (schema/as-conformer serialise-geography)
     :value->dimension-uri identity
     :result-binding->value identity}
    
    (= (URI. "http://purl.org/linked-data/sdmx/2009/dimension#refPeriod") uri)
    {:type :year
     :kind :scalar
     :parse (schema/as-conformer parse-year)
     :serialize (schema/as-conformer serialise-year)
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

(defn dimension->query-var-name [{:keys [order]}]
  (str "dim" order))

(defn get-dimensions
  [repo {:keys [uri] :as ds}]
  (let [base-dims (sp/query "get-dimensions.sparql" {:ds uri} repo)]
    (map (fn [bindings]
           (let [dim (-> bindings
                         (assoc :ds-uri uri)
                         (rename-key :dim :uri))]
             (-> dim
                 (merge (get-dimension-type repo dim ds))
                 (assoc :field-name (->field-name dim))
                 (assoc :->query-var-name dimension->query-var-name))))
         base-dims)))

(defn measure-type->query-var-name [{:keys [order]}]
  (str "mt" order))

(defn get-measure-types [repo {:keys [uri] :as ds}]
  (let [results (sp/query "get-measure-types.sparql" {:ds uri} repo)]
    (map-indexed (fn [idx bindings]
                   (-> bindings
                       (rename-key :mt :uri)
                       (assoc :field-name (->field-name bindings))
                       (assoc :order (inc idx))
                       (assoc :->query-var-name measure-type->query-var-name)
                       (assoc :result-binding->value #(some-> % str)))) results)))

(defn measure-type->schema [{:keys [field-name]}]
  {field-name {:type 'String}})

(defn dimensions->obs-dim-schemas [dims]
  (map (fn [{:keys [field-name type label doc] :as dim}]
         [field-name {:type type
                      :description (some-> (or doc label) str)}])
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

(defn resolve-dataset-dimensions [{:keys [repo] :as context} args {:keys [uri] :as ds-field}]
  (let [dims (get-dimensions repo ds-field)]
    (map (fn [{:keys [uri values]}]
           {:uri (str uri)
            :values (map (fn [{:keys [member label]}]
                           {:uri (str member)
                            :label label})
                         values)})
         dims)))

(defn get-dataset [repo uri]
  (if-let [{:keys [title] :as ds} (first (sp/query "get-datasets.sparql" {:ds uri} repo))]
    (-> ds
        (assoc :schema (name (dataset-label->schema-name title))))))

(defn resolve-dataset [uri {:keys [repo] :as context} args field]
  (if-let [ds (get-dataset repo uri)]
    (assoc ds :dimensions (resolve-dataset-dimensions context args {:uri uri}))))

(defn get-observation-query [ds-uri ds-dimensions query-dimensions measure-types]
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
     "  ?obs qb:dataSet <" ds-uri "> ."
     "  ?obs qb:measureType ?measureType ."
     "  ?obs ?measureType ?value ."
     (string/join "\n" measure-type-patterns)
     (string/join "\n" constrained-patterns)
     (string/join "\n" query-patterns)
     (string/join "\n" binds)
     "}")))

(defn resolve-observations [{:keys [repo ds-uri->dims-measures] :as context} {query-dimensions :dimensions :as args} {:keys [uri] :as ds-field}]
  (let [{:keys [dimensions measure-types]} (get ds-uri->dims-measures uri)
        query (get-observation-query uri dimensions query-dimensions measure-types)
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
     :sparql (string/join (string/split query #"\n"))
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

(defn resolve-datasets [{:keys [repo]} {:keys [dimensions measures] :as args} _parent]
  (let [q (get-datasets-query dimensions measures)
        results (repo/query repo q)]
    (map (fn [{:keys [title] :as bindings}]
           (-> bindings
               (rename-key :ds :uri)
               (assoc :schema (name (dataset-label->schema-name title)))))
         results)))

(defn get-dataset-schema [{:keys [uri schema description] :as ds} dims measure-types]
  (let [obs-dim-schemas (dimensions->obs-dim-schemas dims)
        measure-type-schemas (map measure-type->schema measure-types)
        observation-fields (into {:uri {:type :uri}} (concat obs-dim-schemas measure-type-schemas))
        enums-schema (dimensions->enums-schema dims)
        observation-result-type-name (field-name->type-name :observation_result schema)
        observation-type-name (field-name->type-name :observation schema)
        observation-dims-type-name (field-name->type-name :observation_dimensions schema)
        resolver-name (keyword (str "resolve_" (name schema)))
        resolver-map {resolver-name (fn [context args field]
                                      (resolve-dataset uri context args field))}]
    {:enums enums-schema
     :objects
     {schema
      {:fields
       {:uri {:type :uri :description "Dataset URI"}
        :title {:type 'String :description "Dataset title"}
        :description {:type 'String :description "Dataset description"}
        :schema {:type 'String :description "Name of the GraphQL query root for this dataset"}
        :dimensions {:type '(list :dim) :description "Dimensions within the dataset"}
        :observations {:type observation-result-type-name
                       :args {:dimensions {:type observation-dims-type-name}
                              :after {:type :SparqlCursor}
                              :first {:type 'Int}}
                       :resolve :resolve-observations
                       :description "Observations matching the given criteria"}}
       :description description}

      observation-result-type-name
      {:fields
       {:sparql {:type 'String :description "SPARQL query used to retrieve matching observations."}
        :matches {:type (list 'list observation-type-name) :description "List of matching observations."}
        :free_dimensions {:type '(list :dim)}}}
      
      observation-type-name
      {:fields observation-fields}}

     :input-objects
     {observation-dims-type-name
      {:fields (into {} obs-dim-schemas)}}

     :queries
     {schema
      {:type schema
       :resolve resolver-name}}

     :resolvers resolver-map}))

(defn find-datasets [repo]
  (let [results (sp/query "get-datasets.sparql" repo)]
    (map (fn [{:keys [title] :as ds}]
           (assoc ds :schema (dataset-label->schema-name title)))
         results)))

(def custom-scalars
  {:SparqlCursor
   {:parse (schema/as-conformer types/parse-sparql-cursor)
    :serialize (schema/as-conformer types/serialise-sparql-cursor)}

   :year
   {:parse (schema/as-conformer types/parse-year)
    :serialize (schema/as-conformer types/serialise-year)}

   :ref_area
   {:parse (schema/as-conformer types/parse-geography)
    :serialize (schema/as-conformer types/serialise-geography)}

   :uri {:parse (schema/as-conformer #(URI. %))
         :serialize (schema/as-conformer str)}})

(defn get-schema [datasets ds-uri->dims-measures]
  (let [base-schema (read-edn-resource "base-schema.edn")
        base-schema (assoc base-schema :scalars custom-scalars)
        ds-schemas (map (fn [{:keys [uri] :as ds}]
                          (let [{:keys [dimensions measure-types]} (ds-uri->dims-measures uri)]
                            (get-dataset-schema ds dimensions measure-types)))
                        datasets)
        combined-schema (reduce (fn [acc schema] (merge-with merge acc schema)) base-schema ds-schemas)
        schema-resolvers (:resolvers combined-schema)
        query-resolvers (merge {:resolve-observations resolve-observations
                                :resolve-datasets resolve-datasets
                                :resolve-dataset-dimensions resolve-dataset-dimensions}
                               schema-resolvers)]
    (attach-resolvers combined-schema query-resolvers)))

(defn dump-schema [repo]
  (let [datasets (find-datasets repo)
        ds-uri->dims-measures (into {} (map (fn [{:keys [uri] :as ds}]
                                              [uri {:dimensions (get-dimensions repo ds)
                                                    :measure-types (get-measure-types repo ds)}])
                                            datasets))
        schema (get-schema datasets ds-uri->dims-measures)]
    (pprint/pprint schema)))

(defn get-compiled-schema [repo]
  (schema/compile (get-schema repo)))

(defn build-schema-context [repo]
  (let [datasets (find-datasets repo)
        ds-uri->dims-measures (into {} (map (fn [{:keys [uri] :as ds}]
                                              [uri {:dimensions (get-dimensions repo ds)
                                                    :measure-types (get-measure-types repo ds)}])
                                            datasets))
        schema (get-schema datasets ds-uri->dims-measures)]
    {:schema (schema/compile schema)
     :ds-uri->dims-measures ds-uri->dims-measures}))
