(ns graphql-qb.core
  (:require [grafter.rdf.repository :as repo]
            [grafter.rdf.sparql :as sp]
            [clojure.string :as string]
            [com.walmartlabs.lacinia.schema :as lschema]
            [com.walmartlabs.lacinia.util :refer [attach-resolvers]]
            [com.walmartlabs.lacinia :refer [execute]]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [clojure.set :as set]
            [graphql-qb.util :refer [read-edn-resource rename-key] :as util]
            [graphql-qb.types :refer :all :as types]
            [clojure.pprint :as pprint]
            [graphql-qb.schema :as schema])
  (:import [java.net URI]))

(defn get-enum-values [repo {:keys [ds-uri uri] :as dim}]
  (let [results (sp/query "get-enum-values.sparql" {:ds ds-uri :dim uri} repo)]
    (map (fn [{:keys [label] :as m}]
           (assoc m :value (enum-label->value-name label)))
         results)))

(defn get-dimension-type [repo {:keys [uri label] :as dim} {:keys [schema] :as ds}]
  (cond
    (= (URI. "http://purl.org/linked-data/sdmx/2009/dimension#refArea") uri)
    (types/->RefAreaType)
    
    (= (URI. "http://purl.org/linked-data/sdmx/2009/dimension#refPeriod") uri)
    (types/->RefPeriodType)
    
    :else
    (let [values (get-enum-values repo dim)
          value->uri (into {} (map (juxt :value :member) values))
          enum-name (types/label->enum-name label)]
      (types/->EnumType schema enum-name value->uri))))

(defn get-dimensions
  [repo {:keys [uri schema] :as ds}]
  (let [base-dims (sp/query "get-dimensions.sparql" {:ds uri} repo)]
    (map (fn [bindings]
           (let [dim (-> bindings
                         (assoc :ds-uri uri)
                         (assoc :schema schema)
                         (rename-key :dim :uri))
                 type (get-dimension-type repo dim ds)
                 dim-rec (types/map->Dimension (assoc dim :type type))]
             (assoc dim-rec :field-name (->field-name dim))))
         base-dims)))

(defn is-measure-numeric? [repo ds-uri measure-uri]
  (let [results (sp/query "sample-observation-measure.sparql" {:ds ds-uri :mt measure-uri} repo)]
    (number? (:value (first results)))))

(defn get-measure-types [repo {:keys [uri] :as ds}]
  (let [results (sp/query "get-measure-types.sparql" {:ds uri} repo)]
    (map-indexed (fn [idx {:keys [mt label] :as bindings}]
                   (let [measure-type (types/->MeasureType mt label (inc idx) (is-measure-numeric? repo uri mt))]
                     (assoc measure-type :field-name (->field-name bindings)))) results)))

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
    (assoc ds :dimensions (resolve-dataset-dimensions context args ds))))

(defn get-order-by [order-by-dim-measures]
  (if (empty? order-by-dim-measures)
    ""
    (let [orderings (map (fn [[dm sort-direction]]
                           (let [var (str "?" (types/->order-by-var-name dm))]
                             (if (= :DESC sort-direction)
                               (str "DESC(" var ")")
                               var)))
                         order-by-dim-measures)]
      (str "ORDER BY " (string/join " " orderings)))))

(defn get-observation-query-bgps [ds-uri ds-dimensions query-dimensions measure-types order-by-dims-measures]
  (let [is-query-dimension? (fn [{:keys [field-name]}] (contains? query-dimensions field-name))
        constrained-dims (filter is-query-dimension? ds-dimensions)
        free-dims (remove is-query-dimension? ds-dimensions)
        constrained-patterns (map (fn [{:keys [field-name uri] :as dim}]
                                    (let [field-value (get query-dimensions field-name)
                                          val-uri (types/graphql->sparql dim field-value)]
                                      (str "?obs <" uri "> <" val-uri "> .")))
                                  constrained-dims)
        measure-type-patterns (map (fn [{:keys [uri] :as mt}]
                                     (str
                                      "  ?obs qb:measureType <" (str uri) "> . \n" 
                                      "  ?obs <" (str uri) "> ?" (types/->query-var-name mt) " ."))                                   
                                   measure-types)
        binds (map (fn [{:keys [field-name] :as dim}]
                     (let [field-value (get query-dimensions field-name)                           
                           val-uri (types/graphql->sparql dim field-value)
                           var-name (types/->query-var-name dim)]
                       (str "BIND(<" val-uri "> as ?" var-name ") .")))
                   constrained-dims)
        query-patterns (map (fn [{:keys [uri] :as dim}]
                              (let [var-name (types/->query-var-name dim)]
                                (str "?obs <" uri "> ?" var-name " .")))
                            free-dims)
        order-by-patterns (mapcat (fn [[dm _]] (types/get-order-by-bgps dm)) order-by-dims-measures)]
    (str
     "  ?obs a qb:Observation ."
     "  ?obs qb:dataSet <" ds-uri "> ."
     (string/join "\n" measure-type-patterns)
     (string/join "\n" constrained-patterns)
     (string/join "\n" query-patterns)
     (string/join "\n" order-by-patterns)
     (string/join "\n" binds))))

(defn get-observation-query [ds-uri ds-dimensions query-dimensions measure-types order-by-dim-measures]
  (str
   "PREFIX qb: <http://purl.org/linked-data/cube#>"
   "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
   "SELECT * WHERE {"
   (get-observation-query-bgps ds-uri ds-dimensions query-dimensions measure-types order-by-dim-measures)
   "} " (get-order-by order-by-dim-measures)))

(defn get-observation-page-query [ds-uri ds-dimensions query-dimensions measure-types limit offset order-by-dim-measures]
  (str
   (get-observation-query ds-uri ds-dimensions query-dimensions measure-types order-by-dim-measures)
   " LIMIT " limit " OFFSET " offset))

(defn get-observation-count-query [ds-uri ds-dimensions query-dimensions measure-types]
  (str
   "PREFIX qb: <http://purl.org/linked-data/cube#>"
   "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
   "SELECT (COUNT(*) as ?c) WHERE {"
   (get-observation-query-bgps ds-uri ds-dimensions query-dimensions measure-types [])
   "}"))

(defn get-observation-count [repo ds-uri ds-dimensions query-dimensions measure-types]
  (let [query (get-observation-count-query ds-uri ds-dimensions query-dimensions measure-types)
        results (repo/query repo query)]
    (:c (first results))))

(def default-limit 10)
(def max-limit 1000)

(defn get-limit [args]
  (min (max 0 (or (:first args) default-limit)) max-limit))

(defn get-offset [args]
  (max 0 (or (:after args) 0)))

(defn calculate-next-page-offset [offset limit total-matches]
  (let [next-offset (+ offset limit)]
    (if (> total-matches next-offset)
      next-offset)))

(defn graphql-enum->dimension-measure [{:keys [dimensions measures] :as dataset} enum]
  (first (filter (fn [dm]
                   (= enum (first (types/to-enum-value dm))))
                 (concat dimensions measures))))

(defn get-dimension-measure-ordering [dataset sorts sort-spec]
  (map (fn [dm-enum]
         (let [dm (graphql-enum->dimension-measure dataset dm-enum)
               field (types/->field-name dm)
               sort-dir (get sort-spec field :ASC)]
           [dm sort-dir]))
       sorts))

(defn resolve-observations [{:keys [repo uri->dataset] :as context}
                            {query-dimensions :dimensions order-by :order order-spec :order_spec :as args}
                            {:keys [uri] :as ds-field}]
  (let [{:keys [dimensions measures] :as dataset} (get uri->dataset uri)
          total-matches (get-observation-count repo uri dimensions query-dimensions measures)
          ordered-dim-measures (get-dimension-measure-ordering dataset order-by order-spec)
          query (get-observation-query uri dimensions query-dimensions measures ordered-dim-measures)]
      {::query-dimensions query-dimensions
       ::order-by-dimension-measures ordered-dim-measures
       ::dataset ds-field
       :sparql (string/join (string/split query #"\n"))
       :total_matches total-matches
       :aggregations {:query-dimensions query-dimensions :ds-uri uri}
       :free_dimensions []}))

(defn resolve-observations-page [{:keys [repo uri->dataset] :as context} args observations-field]
  (let [query-dimensions (::query-dimensions observations-field)
        order-by-dim-measures (::order-by-dimension-measures observations-field)
        ds-uri (get-in observations-field [::dataset :uri])
        {:keys [dimensions measures]} (get uri->dataset ds-uri)
        limit (get-limit args)
        offset (get-offset args)
        total-matches (:total_matches observations-field)
        query (get-observation-page-query ds-uri dimensions query-dimensions measures limit offset order-by-dim-measures)
        results (repo/query repo query)
        matches (mapv (fn [{:keys [obs] :as bindings}]
                        (let [field-values (map (fn [{:keys [field-name] :as ft}]                                                    
                                                  (let [result-key (keyword (types/->query-var-name ft))
                                                        value (get bindings result-key)]
                                                    [field-name (types/sparql->graphql ft value)]))
                                                (concat dimensions measures))]
                          (into {:uri obs} field-values)))
                      results)
        next-page (calculate-next-page-offset offset limit total-matches)]
    {:next_page next-page
     :count (count matches)
     :result matches}))

(defn exec-observation-aggregation [repo uri->dataset measure query-dimensions ds-uri aggregation-fn]
  (let [{:keys [dimensions measures] :as dataset} (get uri->dataset ds-uri)
        agg-measure (graphql-enum->dimension-measure dataset measure)
        measure-var-name (types/->query-var-name agg-measure)
        bgps (get-observation-query-bgps ds-uri dimensions query-dimensions measures)
        sparql-fn (string/upper-case (name aggregation-fn))        
        
        q (str
           "PREFIX qb: <http://purl.org/linked-data/cube#>"
           "SELECT (" sparql-fn "(?" measure-var-name ") AS ?" (name aggregation-fn) ") WHERE {"
           bgps
           "}")
          results (repo/query repo q)]
    (get (first results) aggregation-fn)))

(defn resolve-observations-aggregation [aggregation-fn
                                        {:keys [repo uri->dataset] :as context}
                                        {:keys [measure] :as args}
                                        {:keys [query-dimensions ds-uri] :as aggregation-field}]
  (exec-observation-aggregation repo uri->dataset measure query-dimensions ds-uri aggregation-fn))

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

(defn get-datasets-query [dimensions measures uri]
  (str
   "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
   "PREFIX qb: <http://purl.org/linked-data/cube#>"
   "SELECT ?ds ?title ?description WHERE {"
   (get-dimensions-or dimensions)
   "  ?ds rdfs:label ?title ."
   "  ?ds rdfs:comment ?description ."
   (get-dimensions-filter dimensions)
   (if (some? uri)
     (str "FILTER(?ds = <" uri ">) ."))
   "}"))

(defn resolve-datasets [{:keys [repo]} {:keys [dimensions measures uri] :as args} _parent]
  (let [q (get-datasets-query dimensions measures uri)
        results (repo/query repo q)]
    (map (fn [{:keys [title] :as bindings}]
           (-> bindings
               (rename-key :ds :uri)
               (assoc :schema (name (dataset-label->schema-name title)))))
         results)))

(defn find-datasets [repo]
  (let [results (sp/query "get-datasets.sparql" repo)]
    (map (fn [{:keys [uri title description] :as ds}]
           (let [schema (dataset-label->schema-name title)
                 dims (get-dimensions repo {:uri uri :schema schema})
                 measures (get-measure-types repo {:uri uri})]
             (types/->Dataset uri title description dims measures)))
         results)))

(def custom-scalars
  {:SparqlCursor
   {:parse (lschema/as-conformer types/parse-sparql-cursor)
    :serialize (lschema/as-conformer types/serialise-sparql-cursor)}

   :year
   {:parse (lschema/as-conformer types/parse-year)
    :serialize (lschema/as-conformer types/serialise-year)}

   :ref_area
   {:parse (lschema/as-conformer types/parse-geography)
    :serialize (lschema/as-conformer types/serialise-geography)}

   :uri {:parse (lschema/as-conformer #(URI. %))
         :serialize (lschema/as-conformer str)}})

(defn get-schema [datasets]
  (let [base-schema (read-edn-resource "base-schema.edn")
        base-schema (assoc base-schema :scalars custom-scalars)
        ds-schemas (map schema/get-dataset-schema datasets)
        combined-schema (reduce (fn [acc schema] (merge-with merge acc schema)) base-schema ds-schemas)
        schema-resolvers (into {} (map (fn [{:keys [uri] :as ds}]
                                         [(schema/dataset-resolver ds) (fn [context args field]
                                                                         (resolve-dataset uri context args field))])
                                       datasets))
        query-resolvers (merge {:resolve-observations resolve-observations
                                :resolve-observations-page resolve-observations-page
                                :resolve-datasets resolve-datasets
                                :resolve-dataset-dimensions resolve-dataset-dimensions                                
                                :resolve-observations-min (partial resolve-observations-aggregation :min)
                                :resolve-observations-max (partial resolve-observations-aggregation :max)
                                :resolve-observations-sum (partial resolve-observations-aggregation :sum)
                                :resolve-observations-average (partial resolve-observations-aggregation :avg)}
                               schema-resolvers)]
    (attach-resolvers combined-schema query-resolvers)))

(defn dump-schema [repo]
  (let [datasets (find-datasets repo)
        schema (get-schema datasets)]
    (pprint/pprint schema)))

(defn build-schema-context [repo]
  (let [datasets (find-datasets repo)
        schema (get-schema datasets)]
    {:schema (lschema/compile schema)
     :datasets datasets}))

