(ns graphql-qb.queries
  (:require [clojure.string :as string]
            [graphql-qb.types :as types]
            [grafter.rdf.sparql :as sp]
            [graphql-qb.vocabulary :refer :all])
  (:import (java.net URI)))

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

(defn get-dimension-filter-bgps [ds-dimensions query-dimensions]
  (let [is-query-dimension? (fn [{:keys [field-name]}] (contains? query-dimensions field-name))
        constrained-dims (filter is-query-dimension? ds-dimensions)
        constrained-bgps (map (fn [{:keys [field-name] :as dim}]
                                (let [field-value (get query-dimensions field-name)]
                                  (types/get-filter-bgps dim field-value))) constrained-dims)]
    (string/join " " constrained-bgps)))

(defn get-observation-count-query [ds-uri ds-dimensions query-dimensions]
  (str
    "PREFIX qb: <http://purl.org/linked-data/cube#>"
    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
    "PREFIX time: <http://www.w3.org/2006/time#>"
    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>"
    "SELECT (COUNT(*) as ?c) WHERE {"
    "  ?obs a qb:Observation ."
    "  ?obs qb:dataSet <" ds-uri "> ."
    (get-dimension-filter-bgps ds-dimensions query-dimensions)
    "}"))

(defn get-observation-query-bgps [ds-uri ds-dimensions query-dimensions order-by-dims-measures]
  (let [is-query-dimension? (fn [{:keys [field-name]}] (contains? query-dimensions field-name))
        constrained-dims (filter is-query-dimension? ds-dimensions)
        free-dims (remove is-query-dimension? ds-dimensions)
        constrained-patterns (get-dimension-filter-bgps ds-dimensions query-dimensions)
        binds (map (fn [{:keys [field-name] :as dim}]
                     (let [field-value (get query-dimensions field-name)]
                       (types/get-projection-bgps dim field-value)))
                   constrained-dims)
        query-patterns (map (fn [{:keys [uri] :as dim}]
                              (let [var-name (types/->query-var-name dim)]
                                (str "?obs <" uri "> ?" var-name " .")))
                            free-dims)
        order-by-patterns (mapcat (fn [[dm _]] (types/get-order-by-bgps dm)) order-by-dims-measures)]
    (str
      (string/join "\n" binds)
      "  ?obs a qb:Observation ."
      "  ?obs qb:dataSet <" ds-uri "> ."
      constrained-patterns
      (string/join "\n" query-patterns)
      "  ?obs qb:measureType ?mp ."
      "  ?obs ?mp ?mv ."
      (string/join "\n" order-by-patterns))))

(defn get-observation-query [ds-uri ds-dimensions query-dimensions order-by-dim-measures]
  (str
    "PREFIX qb: <http://purl.org/linked-data/cube#>"
    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
    "PREFIX time: <http://www.w3.org/2006/time#>"
    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>"
    "SELECT * WHERE {"
    (get-observation-query-bgps ds-uri ds-dimensions query-dimensions order-by-dim-measures)
    "} " (get-order-by order-by-dim-measures)))

(defn get-observation-page-query [ds-uri ds-dimensions query-dimensions limit offset order-by-dim-measures]
  (str
    (get-observation-query ds-uri ds-dimensions query-dimensions order-by-dim-measures)
    " LIMIT " limit " OFFSET " offset))

(defn get-dimensions-or [{dims-or :or}]
  (if (empty? dims-or)
    (str "{ SELECT DISTINCT ?ds WHERE {"
         "  ?ds a qb:DataSet ."
         "} }")
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

(defn get-datasets-query [dimensions measures uri]
  (str
    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
    "PREFIX qb: <http://purl.org/linked-data/cube#>"
    "PREFIX dcterms: <http://purl.org/dc/terms/>"
    "SELECT ?ds ?title ?description ?licence ?issued ?modified ?publisher WHERE {"
    (get-dimensions-or dimensions)
    "  ?ds rdfs:label ?title ."
    "  OPTIONAL { ?ds rdfs:comment ?description . }"
    "  OPTIONAL { ?ds dcterms:license ?licence }"
    "  OPTIONAL { ?ds dcterms:issued ?issued }"
    "  OPTIONAL { ?ds dcterms:modified ?modified }"
    "  OPTIONAL { ?ds dcterms:publisher ?publisher }"
    (get-dimensions-filter dimensions)
    (if (some? uri)
      (str "FILTER(?ds = <" uri ">) ."))
    "}"))

(defn get-observation-aggregation-query [aggregation-fn aggregation-measure {ds-uri :uri dimensions :dimensions :as dataset} query-dimensions]
  (let [measure-var-name (types/->query-var-name aggregation-measure)
        dimension-bgps (get-dimension-filter-bgps dimensions query-dimensions)
        sparql-fn (string/upper-case (name aggregation-fn))]
    (str
      "PREFIX qb: <http://purl.org/linked-data/cube#>"
      "PREFIX time: <http://www.w3.org/2006/time#>"
      "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>"
      "SELECT (" sparql-fn "(?" measure-var-name ") AS ?" (name aggregation-fn) ") WHERE {"
      "  ?obs a qb:Observation ."
      "  ?obs qb:dataSet <" ds-uri "> ."
      dimension-bgps
      "  ?obs <" (:uri aggregation-measure) "> ?" measure-var-name " ."
      "}")))

(defn get-unmapped-dimension-values [repo {:keys [uri] :as dataset}]
  (let [results (vec (sp/query "get-unmapped-dimension-values.sparql" {:ds uri} repo))]
    (group-by :dim results)))

(defn get-datasets-containing-dimension [repo dimension-uri]
  (let [results (vec (sp/query "get-datasets-with-dimension.sparql" {:dim dimension-uri} repo))]
    (into #{} (map :ds results))))

(defn get-datasets-containing-ref-area-dimension [repo]
  (get-datasets-containing-dimension repo sdmx:refArea))

(defn get-datasets-containing-ref-period-dimension [repo]
  (get-datasets-containing-dimension repo sdmx:refPeriod))