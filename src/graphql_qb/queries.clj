(ns graphql-qb.queries
  (:require [clojure.string :as string]
            [graphql-qb.types :as types]
            [grafter.rdf.sparql :as sp]
            [graphql-qb.vocabulary :refer :all]
            [graphql-qb.query-model :as qm]))

(defn get-observation-filter-model [ds-dimensions query-dimensions]
  (reduce (fn [m {:keys [field-name] :as dim}]
            (let [graphql-value (get query-dimensions field-name)]
              (types/apply-filter dim m graphql-value))) qm/empty-model ds-dimensions))

(defn get-observation-count-query [ds-uri ds-dimensions query-dimensions]
  (let [model (get-observation-filter-model ds-dimensions query-dimensions)]
    (qm/get-observation-count-query model "obs" ds-uri)))

(defn get-observation-query-model [{:keys [dimensions measures] :as dataset} query-dimensions order-by-dims-measures observation-selections]
  (let [filter-model (get-observation-filter-model dimensions query-dimensions)
        with-projection (reduce (fn [m dm]
                                  (types/apply-projection dm m observation-selections))
                                filter-model
                                (concat dimensions measures))]
    ;;apply order by
    (reduce (fn [m [dim-measure direction]]
              (types/apply-order-by dim-measure m direction))
            with-projection
            order-by-dims-measures)))

(defn get-observation-query [ds-uri dataset query-dimensions order-by-dim-measures observation-selections]
  (let [model (get-observation-query-model dataset query-dimensions order-by-dim-measures observation-selections)]
    (qm/get-query model "obs" ds-uri)))

(defn get-observation-page-query [ds-uri dataset query-dimensions limit offset order-by-dim-measures observation-selections]
  (str
    (get-observation-query ds-uri dataset query-dimensions order-by-dim-measures observation-selections)
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