(ns graphql-qb.queries
  (:require [clojure.string :as string]
            [graphql-qb.types :as types]
            [grafter.rdf.sparql :as sp]
            [graphql-qb.vocabulary :refer :all]
            [graphql-qb.query-model :as qm]
            [graphql-qb.config :as config]
            [graphql-qb.util :as util])
  (:import  [java.net URI]))

(defn get-observation-filter-model [dim-filter]
   (let [m (-> qm/empty-model
                    (qm/add-binding [[:mp (URI. "http://purl.org/linked-data/cube#measureType")]] ::qm/var)
                    (qm/add-binding [[:mv (qm/->QueryVar "mp")]] ::qm/var))]
       (reduce (fn [m [dim value]]
                       (types/apply-filter dim m value))
                     m
                     dim-filter)))

(defn apply-model-projections [filter-model dataset observation-selections]
  (reduce (fn [m dm]
            (types/apply-projection dm m observation-selections))
          filter-model
          (types/dataset-dimension-measures dataset)))

(defn apply-model-order-by [model order-by-dims-measures]
  (reduce (fn [m [dim-measure direction]]
            (types/apply-order-by dim-measure m direction))
          model
          order-by-dims-measures))

(defn filter-model->observations-query [filter-model dataset order-by observation-selections]
  (-> filter-model
      (apply-model-projections dataset observation-selections)
      (apply-model-order-by order-by)))

(defn get-observation-query [{ds-uri :uri :as dataset} filter-model order-by observation-selections]
  (let [model (filter-model->observations-query filter-model dataset order-by observation-selections)]
    (qm/get-query model "obs" ds-uri)))

(defn get-observation-page-query [dataset filter-model limit offset order-by-dim-measures observation-selections]
  (str
    (get-observation-query dataset filter-model order-by-dim-measures observation-selections)
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

(defn get-unmapped-dimension-values-query [uri]
  (let [configuration (config/read-config)
        area-dim (config/geo-dimension configuration)
        time-dim (config/time-dimension configuration)]
    (str
      "PREFIX qb: <http://purl.org/linked-data/cube#>"
      "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
      "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
      "PREFIX ui: <http://www.w3.org/ns/ui#>"
      "SELECT ?dim ?member ?label WHERE {"
      "<" (str uri) "> qb:structure ?struct ."
      "?struct a qb:DataStructureDefinition ."
      "?struct qb:component ?comp ."
      "VALUES ?dim { <" (str area-dim) "> <" (str time-dim) "> }"
      "?comp qb:dimension ?dim ."
      (config/codelist-source configuration) " qb:codeList ?list  ."
      "?list skos:member ?member ."
      "OPTIONAL { ?member rdfs:label ?label . }"
      "}")))

(defn get-unmapped-dimension-values [repo {:keys [uri] :as dataset}]
  (let [dimvalues-query (get-unmapped-dimension-values-query uri)
        results (util/eager-query repo dimvalues-query)]
    (group-by :dim results)))

(defn get-datasets-containing-dimension [repo dimension-uri]
  (let [results (vec (sp/query "get-datasets-with-dimension.sparql" {:dim dimension-uri} repo))]
    (into #{} (map :ds results))))

(defn get-dimensions-query [dim-uris]
  (str
    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
    "PREFIX qb: <http://purl.org/linked-data/cube#>"
    "SELECT ?dim ?label ?comment WHERE {"
    "  VALUES ?dim { " (string/join " " (map #(str "<" % ">") dim-uris)) " }"
    "  ?dim a qb:DimensionProperty ."
    "  ?dim rdfs:label ?label ."
    "  OPTIONAL { ?dim rdfs:comment ?comment }"
    "}"))

(defn get-all-enum-dimension-values []
  (let [configuration (config/read-config)
        area-dim (config/geo-dimension configuration)
        time-dim (config/time-dimension configuration)]
    (str
      "PREFIX qb: <http://purl.org/linked-data/cube#>"
      "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
      "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
      "SELECT * WHERE {"
      "?ds qb:structure ?struct ."
      "?struct a qb:DataStructureDefinition ."
      "?struct qb:component ?comp ."
      "?comp a qb:ComponentSpecification ."
      "?comp qb:dimension ?dim ."
      "FILTER(?dim != <" (str area-dim) ">)"
      "FILTER(?dim != <" (str time-dim) ">)"
      "?dim rdfs:label ?label ."
      "OPTIONAL { ?dim rdfs:comment ?doc }"
      (config/codelist-source configuration) " qb:codeList ?list ."
      "?list skos:member ?member ."
      "OPTIONAL { ?member rdfs:label ?vallabel . }"
      "}")))