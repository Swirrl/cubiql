(ns graphql-qb.queries
  (:require [clojure.string :as string]
            [graphql-qb.types :as types]
            [grafter.rdf.sparql :as sp]
            [graphql-qb.vocabulary :refer :all]
            [graphql-qb.query-model :as qm]))

(defn get-observation-filter-model [dim-filter]
  (reduce (fn [m [dim value]]
            (types/apply-filter dim m value))
          qm/empty-model
          dim-filter))

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

(defn get-unmapped-dimension-values [repo {:keys [uri] :as dataset}]
  (let [results (vec (sp/query "get-unmapped-dimension-values.sparql" {:ds uri} repo))]
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