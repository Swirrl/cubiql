(ns graphql-qb.queries
  (:require [clojure.string :as string]
            [graphql-qb.types :as types]
            [graphql-qb.vocabulary :refer :all]
            [graphql-qb.query-model :as qm]
            [graphql-qb.config :as config]
            [graphql-qb.util :as util]
            [graphql-qb.types.scalars :as scalars]
            [graphql-qb.schema.mapping.dataset :as dsm]))

(defn- add-observation-filter-measure-bindings [dataset-mapping model]
  (if (dsm/has-measure-type-dimension? dataset-mapping)
    (-> model
        (qm/add-binding [[:mp qb:measureType]] ::qm/var)
        (qm/add-binding [[:mv (qm/->QueryVar "mp")]] ::qm/var))
    (reduce (fn [m {{:keys [uri order] :as measure} :measure :as measure-mapping}]
              (let [measure-key (keyword (str "mv" order))]
                (qm/add-binding m [[measure-key uri]] ::qm/var)))
            model
            (dsm/measures dataset-mapping))))

(defn get-observation-filter-model [dataset-mapping dim-filter]
  (let [m (add-observation-filter-measure-bindings dataset-mapping qm/empty-model)]
    (reduce (fn [m [dim value]]
              (types/apply-filter dim m value))
            m
            dim-filter)))

(defn apply-model-projections [filter-model dataset observation-selections config]
  (reduce (fn [m dm]
            (types/apply-projection dm m observation-selections config))
          filter-model
          (types/dataset-dimension-measures dataset)))

(defn apply-model-order-by [model order-by-dims-measures config]
  (reduce (fn [m [dim-measure direction]]
            (types/apply-order-by dim-measure m direction config))
          model
          order-by-dims-measures))

(defn filter-model->observations-query [filter-model dataset order-by observation-selections config]
  (-> filter-model
      (apply-model-projections dataset observation-selections config)
      (apply-model-order-by order-by config)))

(defn get-observation-query [{ds-uri :uri :as dataset} filter-model order-by observation-selections config]
  (let [model (filter-model->observations-query filter-model dataset order-by observation-selections config)]
    (qm/get-query model "obs" ds-uri)))

(defn get-observation-page-query [dataset filter-model limit offset order-by-dim-measures observation-selections config]
  (str
    (get-observation-query dataset filter-model order-by-dim-measures observation-selections config)
    " LIMIT " limit " OFFSET " offset))

(defn get-dimensions-or [{dims-or :or} configuration]
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

(defn get-dimensions-filter [{dims-and :and} configuration]
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

(defn get-measures-filter [{meas-and :and} configuration]
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

(defn get-measures-or [{meas-or :or} configuration]
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

(defn get-attributes-filter [{attr-and :and} configuration]
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

(defn get-attributes-or [{attr-or :or} configuration]
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

(defn get-data-filter [{data-and :and} configuration]
  (if (empty? data-and)
    ""
    (let [codelist-predicate (config/codelist-predicate configuration)
          and-clauses (map-indexed (fn [idx {comp :component vals :values levs :levels}]
                                     (let [incidx (str (inc idx))]
                                       (str " ?struct qb:component ?compdata" incidx " .\n"
                                            " ?compdata" incidx " qb:dimension|qb:attribute <" comp "> .\n" ;;the component can be either a dimension or attribute
                                            " ?compdata" incidx " <" codelist-predicate "> ?cl" incidx ".\n" ;the codelist should contain ONLY the values used at the dataset
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

(defn get-data-or [{data-or :or} configuration]
  (if (empty? data-or)
    ""
    (let [codelist-predicate (config/codelist-predicate configuration)
          union-clauses (map (fn [{comp :component vals :values levs :levels}]
                               (str "{ ?struct qb:component ?comp .\n"
                                    "  ?comp qb:dimension|qb:attribute <" comp "> .\n" ;;the component can be either a dimension or attribute
                                    "  ?comp <" codelist-predicate "> ?cl.\n" ;;the codelist should contain ONLY the values used at the dataset
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

(defn get-datasets-query [dimensions measures attributes componentValue uri configuration]
  (let [dataset-label (config/dataset-label configuration)
        schema-lang (config/schema-label-language configuration)]
    (str
      "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
      "PREFIX qb: <http://purl.org/linked-data/cube#>"
      "PREFIX dcterms: <http://purl.org/dc/terms/>"
      "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
      "SELECT DISTINCT ?ds ?name WHERE {"
      "  ?ds a qb:DataSet ."
      "  ?ds qb:structure ?struct ."
      "  ?struct a qb:DataStructureDefinition ."
      (get-dimensions-or dimensions configuration)
      (get-measures-or measures configuration)
      (get-attributes-or attributes configuration)
      (get-data-or componentValue configuration)
      "  ?ds <" (str dataset-label) "> ?name ."
      "  FILTER(LANG(?name) = \"" schema-lang "\")"
      (get-dimensions-filter dimensions configuration)
      (get-measures-filter measures configuration)
      (get-attributes-filter attributes configuration)
      (get-data-filter componentValue configuration)
      (if (some? uri)
        (str "FILTER(?ds = <" uri ">) ."))
      "}")))

(defn get-datasets [repo dimensions measures attributes componentValue uri configuration]
  (let [q (get-datasets-query dimensions measures attributes componentValue uri configuration)
        results (util/eager-query repo q)]
    (map (util/convert-binding-labels [:name]) results)))

(defn- get-datasets-metadata-query [dataset-uris configuration lang]
  (let [label-predicate (str (config/dataset-label configuration))]
    (str
      "PREFIX qb: <http://purl.org/linked-data/cube#>"
      "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
      "PREFIX dcterms: <http://purl.org/dc/terms/>"
      "SELECT DISTINCT * WHERE {"
      "  VALUES ?ds { " (string/join " " (map #(str "<" % ">") dataset-uris)) " }"
      "  ?ds a qb:DataSet ."
      "{"
      "  ?ds <" label-predicate "> ?title ."
      (when lang
        (str "FILTER(LANG(?title) = \"" lang "\")"))
      "}"
      "UNION {"
      "  ?ds rdfs:comment ?description ."
      (when lang
        (str "FILTER(LANG(?description) = \"" lang "\")"))
      "}"
      "UNION { ?ds dcterms:issued ?issued . }"
      "UNION { ?ds dcterms:publisher ?publisher . }"
      "UNION { ?ds dcterms:license ?licence . }"
      "UNION {"
      "  SELECT ?modified WHERE {"
      "    ?ds dcterms:modified ?modified ."
      "  } ORDER BY DESC(?modified) LIMIT 1"
      "}"
      "}")))

(def metadata-keys #{:title :description :issued :publisher :licence :modified})

(defn process-dataset-metadata-bindings [bindings]
  (let [{:keys [title description issued publisher licence modified]} (util/to-multimap bindings)]
    {:title       (util/label->string (first title))              ;;TODO: allow multiple titles?
     :description (mapv util/label->string description)
     :issued      (mapv scalars/grafter-date->datetime issued)
     :publisher   (or publisher [])
     :licence     (or licence [])
     :modified    (some-> (first modified) (scalars/grafter-date->datetime))}))

(defn get-dataset-metadata [repo dataset-uri configuration lang]
  (let [q (get-datasets-metadata-query [dataset-uri] configuration lang)
        bindings (util/eager-query repo q)]
    (process-dataset-metadata-bindings bindings)))

(defn get-datasets-metadata [repo dataset-uris configuration lang]
  (let [q (get-datasets-metadata-query dataset-uris configuration lang)
        results (util/eager-query repo q)]
    (group-by :ds results)))

(defn get-dimension-codelist-values-query [ds-uri configuration lang]
  (let [codelist-label (config/codelist-label configuration)
        codelist-predicate (config/codelist-predicate configuration)]
    (str
      "PREFIX qb: <http://purl.org/linked-data/cube#>"
      "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
      "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
      "PREFIX ui: <http://www.w3.org/ns/ui#>"
      "SELECT ?dim ?member ?label WHERE {"
      "<" (str ds-uri) "> qb:structure ?struct ."
      "?struct a qb:DataStructureDefinition ."
      "?struct qb:component ?comp ."
      "?comp qb:dimension ?dim ."
      (config/codelist-source configuration) " <" codelist-predicate "> ?list ."
      "{"
      "  ?list skos:member ?member ."
      "  OPTIONAL {"
      "    ?member <" (str codelist-label) "> ?label ."
      (when lang
        (str "FILTER(LANG(?label) = \"" lang "\") ."))
      "  }"
      "} UNION {"
      "  ?member skos:inScheme ?list ."
      "  OPTIONAL {"
      "    ?member <" (str codelist-label) "> ?label ."
      (when lang
        (str "FILTER(LANG(?label) = \"" lang "\") ."))
      "  }"
      "}"
      "}")))

(defn get-dimension-codelist-values [repo {:keys [uri] :as dataset} config lang]
  (let [dimvalues-query (get-dimension-codelist-values-query uri config lang)
        results (util/eager-query repo dimvalues-query)]
    (map (util/convert-binding-labels [:label]) results)))

(defn get-all-enum-dimension-values
  "Gets all codelist members for all dimensions across all datasets. Each dimension is expected to have a
  single label without a language code. Each codelist item should have at most one label without a language
  code used to generate the enum name."
  [configuration]
  (let [codelist-label (config/codelist-label configuration)
        codelist-predicate (config/codelist-predicate configuration)
        ignored-dimensions (config/ignored-codelist-dimensions configuration)
        dimension-filters (map (fn [dim-uri] (format "FILTER(?dim != <%s>)" dim-uri)) ignored-dimensions)]
    (str
      "PREFIX qb: <http://purl.org/linked-data/cube#>"
      "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
      "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
      "SELECT * WHERE {"
      "  ?ds qb:structure ?struct ."
      "  ?struct a qb:DataStructureDefinition ."
      "  ?struct qb:component ?comp ."
      "  ?comp qb:dimension ?dim ."
      (string/join "\n" dimension-filters)
      "  OPTIONAL { ?dim rdfs:comment ?doc }"
      (config/codelist-source configuration) " <" codelist-predicate "> ?codelist ."
      "{"
      "  ?codelist skos:member ?member ."
      "  ?member <" (str codelist-label) "> ?vallabel ."
      "}"
      "UNION {"
      "  ?member skos:inScheme ?codelist ."
      "  ?member rdfs:label ?vallabel ."
      "}"

      "}")))

(defn get-measures-by-lang-query [ds-uri lang configuration]
  (str
    "PREFIX qb: <http://purl.org/linked-data/cube#>"
    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
    "SELECT ?mt ?label WHERE {"
    "  <" ds-uri "> qb:structure ?struct ."
    "  ?struct qb:component ?comp ."
    "  ?comp qb:measure ?mt ."
    "  ?mt a qb:MeasureProperty ."
    "  OPTIONAL {"
    "    ?mt <" (config/dataset-label configuration) "> ?label ."
    "    FILTER(LANG(?label) = \"" lang "\")"
    "  }"
    "}"))

(defn get-dimensions-by-lang-query [ds-uri lang configuration]
  (str
    "PREFIX qb: <http://purl.org/linked-data/cube#>"
    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
    "SELECT ?dim ?label WHERE {"
    "  <" ds-uri "> qb:structure ?struct ."
    "  ?struct qb:component ?comp ."
    "  ?comp qb:dimension ?dim ."
    "  ?dim a qb:DimensionProperty ."
    "  OPTIONAL {"
    "    ?dim <" (config/dataset-label configuration) "> ?label ."
    "    FILTER(LANG(?label) = \"" lang "\")"
    "  }"
    "}"))

(defn get-dimension-labels-query [configuration]
  (str
    "PREFIX qb: <http://purl.org/linked-data/cube#>"
    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
    "SELECT ?dim ?label ?doc WHERE {"
    "  ?dim a qb:DimensionProperty ."
    "  { ?dim <" (config/dataset-label configuration) "> ?label . }"
    "  UNION { ?dim rdfs:comment ?doc . }"
    "}"))

(defn get-measure-labels-query [configuration]
  (let [dataset-label (config/dataset-label configuration)]
    (str
      "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
      "PREFIX qb: <http://purl.org/linked-data/cube#>"
      "SELECT ?measure ?label WHERE {"
      "  ?measure a qb:MeasureProperty ."
      "  ?measure <" (str dataset-label) "> ?label ."
      "}")))