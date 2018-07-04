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
    ""
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
      (str (string/join "\n" and-clauses)))))



;;Added by Dimitris
(defn get-measures-filter [{meas-and :and}]
  (if (empty? meas-and)
    ""
    (let [and-clauses (map-indexed (fn [idx uri]
                                     (let [comp-var (str "?compMeas" (inc idx))]
                                       (str
                                        "?struct qb:component " comp-var ". \n"
                                        comp-var " a qb:ComponentSpecification .\n"
                                        comp-var " qb:measure <" (str uri) "> .\n")))
                            meas-and)]
      (str (string/join and-clauses)))))


;;Added by Dimitris
(defn get-measures-or [{meas-or :or}]
  (if (empty? meas-or)
    ""
    (let [union-clauses (map (fn [meas]
                               (str "{ ?struct qb:component ?comp .\n"
                                    "  ?comp qb:measure <" meas "> . }\n"))
                             meas-or)]
      (str
       "{ SELECT DISTINCT ?ds WHERE {\n"
       "  ?ds a qb:DataSet .\n"
       "  ?ds qb:structure ?struct .\n"
       "  ?struct a qb:DataStructureDefinition .\n"
       (string/join " UNION " union-clauses)
       "} }"))))

;;Added by Dimitris
(defn get-attributes-filter [{attr-and :and}]
  (if (empty? attr-and)
    ""
    (let [and-clauses (map-indexed (fn [idx uri]
                                     (let [comp-var (str "?compAttr" (inc idx))]
                                       (str
                                        "?struct qb:component " comp-var ". \n"
                                        comp-var " a qb:ComponentSpecification .\n"
                                        comp-var " qb:attribute <" (str uri) "> .\n")))
                            attr-and)]
      (str (string/join and-clauses)))))


;;Added by Dimitris
(defn get-attributes-or [{attr-or :or}]
  (if (empty? attr-or)
    ""
    (let [union-clauses (map (fn [attr]
                               (str "{ ?struct qb:component ?comp .\n"
                                    "  ?comp qb:attribute <" attr "> . }\n"))
                             attr-or)]
      (str
       "{ SELECT DISTINCT ?ds WHERE {\n"
       "  ?ds a qb:DataSet .\n"
       "  ?ds qb:structure ?struct .\n"
       "  ?struct a qb:DataStructureDefinition .\n"
       (string/join " UNION " union-clauses)
       "} }"))))

;;Added by Dimitris
(defn get-data-filter [{data-and :and}]
  (if (empty? data-and)
    ""
    (let [and-clauses (map-indexed (fn [idx {comp :component vals :values levs :levels}]
                                     (let [incidx (str (inc idx))]
                                       (str " ?struct qb:component ?compdata" incidx " .\n"
                                            " ?compdata" incidx " qb:dimension|qb:attribute <" comp "> .\n" ;;the component can be either a dimension or attribute
                                            " ?compdata" incidx " qb:codeList ?cl" incidx ".\n" ;the codelist should contain ONLY the values used at the dataset
                                             (if (some? vals)
                                                (let [cl-vals
                                                  (map (fn[uri] (str "  ?cl" incidx " skos:member <" uri ">.\n")) vals)]
                                                  (str (string/join cl-vals))))
                                             (if (some? levs)
                                               (let [cl-levs
                                                  (map (fn[uri] (str "  ?cl" incidx " skos:member/<http://publishmydata.com/def/ontology/foi/memberOf> <" uri ">.\n")) levs)]
                                                  (str (string/join cl-levs)))))))
                        data-and)]
      (str (string/join and-clauses)))))

;;Added by Dimitris
(defn get-data-or [{data-or :or}]
  (if (empty? data-or)
    ""
    (let [union-clauses (map (fn [{comp :component vals :values levs :levels}]
                               (str "{ ?struct qb:component ?comp .\n"
                                    "  ?comp qb:dimension|qb:attribute <" comp "> .\n" ;;the component can be either a dimension or attribute
                                    "  ?comp qb:codeList ?cl.\n" ;;the codelist should contain ONLY the values used at the dataset
                                    "  ?cl skos:member ?mem.\n"
                                   (if (some? vals)
                                       (let [members
                                            (map (fn[uri] (str "  ?mem=<" uri ">")) vals)]
                                            (str "FILTER("                                      
                                              (string/join "||" members)
                                              ")")))
                                   (if (some? levs)
                                     (str "  ?mem <http://publishmydata.com/def/ontology/foi/memberOf> ?lev.\n"
                                       (let [levelsOr
                                            (map (fn[uri] (str "  ?lev=<" uri ">")) levs)]
                                            (str "FILTER("                                      
                                              (string/join "||" levelsOr)
                                              ")"))))                                                                                               
                                    "}\n"))
                             data-or)]
    (str
       "{ SELECT DISTINCT ?ds WHERE {\n"
       "  ?ds a qb:DataSet .\n"
       "  ?ds qb:structure ?struct .\n"
       "  ?struct a qb:DataStructureDefinition .\n"
       (string/join " UNION " union-clauses)
       "} }"))))

;;Modified by Dimitris
(defn get-datasets-query [dimensions measures attributes componentValue uri]
  (str
    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
    "PREFIX qb: <http://purl.org/linked-data/cube#>"
    "PREFIX dcterms: <http://purl.org/dc/terms/>"
    "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
    "SELECT DISTINCT ?ds ?title ?description ?licence ?issued ?modified ?publisher WHERE {"
     "  ?ds a qb:DataSet ."
     "  ?ds qb:structure ?struct ."
     "  ?struct a qb:DataStructureDefinition ."
    (get-dimensions-or dimensions)
    (get-measures-or measures)
    (get-attributes-or attributes)
    (get-data-or componentValue)
    "  ?ds rdfs:label ?title ."
    "  OPTIONAL { ?ds rdfs:comment ?description . }"
    "  OPTIONAL { ?ds dcterms:license ?licence }"
    "  OPTIONAL { ?ds dcterms:issued ?issued }"
    "  OPTIONAL { ?ds dcterms:modified ?modified }"
    "  OPTIONAL { ?ds dcterms:publisher ?publisher }"
    (get-dimensions-filter dimensions)
    (get-measures-filter measures)
    (get-attributes-filter attributes)
    (get-data-filter componentValue)
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