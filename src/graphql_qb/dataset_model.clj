(ns graphql-qb.dataset-model
  (:require [graphql-qb.config :as config]
            [graphql-qb.util :as util]
            [grafter.rdf.sparql :as sp]
            [clojure.string :as string]
            [graphql-qb.types :as types]))

(defn find-all-datasets-query [configuration]
  (str
    (let [dataset-label (config/dataset-label configuration)]
      (str
        "PREFIX qb: <http://purl.org/linked-data/cube#>"
        "SELECT ?ds ?name WHERE {"
        "  ?ds a qb:DataSet ."
        "  ?ds <" dataset-label "> ?name ."
        "}"))))

(defn find-all-datasets [repo configuration]
  (let [q (find-all-datasets-query configuration)
        results (util/eager-query repo q)
        by-uri (group-by :ds results)]
    (map (fn [[ds-uri bindings]]
           (let [names (map :name bindings)]
             {:ds ds-uri
              :name (util/find-best-language names (config/schema-label-language configuration))}))
         by-uri)))

(defn find-all-dimensions-query [configuration]
  (let [dataset-label (config/dataset-label configuration)]
    (str
      "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
      "PREFIX qb: <http://purl.org/linked-data/cube#>"
      "SELECT ?dim ?label WHERE {"
      "  ?dim a qb:DimensionProperty ."
      "  ?dim <" (str dataset-label) "> ?label ."
      "}")))

(defn dimension-bindings->dimensions [dimension-bindings configuration]
  (map (fn [[dim-uri bindings]]
         (let [labels (map :label bindings)]
           {:uri dim-uri
            :label (util/find-best-language labels (config/schema-label-language configuration))}))
       (group-by :dim dimension-bindings)))

(defn find-all-dimensions [repo configuration]
  (let [q (find-all-dimensions-query configuration)
        results (util/eager-query repo q)]
    (dimension-bindings->dimensions results configuration)))

(defn measure-bindings->measures [measure-bindings configuration]
  (map (fn [[measure-uri bindings]]
         (let [labels (map :label bindings)]
           {:uri measure-uri
            :label (util/find-best-language labels (config/schema-label-language configuration))}))
       (group-by :measure measure-bindings)))

(defn find-all-measures-query [configuration]
  (let [dataset-label (config/dataset-label configuration)]
    (str
      "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
      "PREFIX qb: <http://purl.org/linked-data/cube#>"
      "SELECT ?measure ?label WHERE {"
      "  ?measure a qb:MeasureProperty ."
      "  ?measure <" (str dataset-label) "> ?label ."
      "}")))

(defn- is-measure-numeric? [repo measure-uri]
  (sp/query "is-measure-numeric.sparql" {:measure measure-uri} repo))

(defn find-numeric-measures [repo all-measures]
  (into #{} (keep (fn [{:keys [uri] :as measure}]
                    (if (is-measure-numeric? repo uri)
                      uri))
                  all-measures)))

(defn find-all-measures [repo configuration]
  (let [q (find-all-measures-query configuration)
        results (util/eager-query repo q)
        measures (measure-bindings->measures results configuration)
        numeric-measures (find-numeric-measures repo measures)]
    (map (fn [{:keys [uri] :as measure}]
           (assoc measure :is-numeric? (contains? numeric-measures uri)))
         measures)))

(defn- get-all-components-query [configuration]
  (str
    "PREFIX qb: <http://purl.org/linked-data/cube#>"
    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
    "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
    "PREFIX dcterms: <http://purl.org/dc/terms/>"
    "SELECT * WHERE {"
    "  ?ds qb:structure ?dsd ."
    "  ?dsd a qb:DataStructureDefinition ."
    "  ?dsd qb:component ?comp ."
    "  OPTIONAL { ?comp qb:order ?order }"
    "  OPTIONAL { ?comp qb:dimension ?dim }"
    "  OPTIONAL { " (config/codelist-source configuration) " qb:codeList ?codelist }"
    "  OPTIONAL { ?comp qb:measure ?measure }"
    "}"))

(defn- get-all-components [repo configuration]
  (let [q (get-all-components-query configuration)
        results (util/eager-query repo q)]
    (->> results
         (map (fn [bindings] (util/rename-key bindings :dim :dimension)))
         (util/distinct-by :comp))))

(defn get-codelist-members-query [configuration]
  (let [dimension-filters (map (fn [dim] (str "FILTER(?dim != <" dim ">)")) (config/ignored-codelist-dimensions configuration))]
    (str
      "PREFIX qb: <http://purl.org/linked-data/cube#>"
      "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
      "SELECT * WHERE {"
      "  ?ds a qb:DataSet ."
      "  ?ds qb:structure ?dsd ."
      "  ?dsd a qb:DataStructureDefinition ."
      "  ?dsd qb:component ?comp ."
      "  ?comp qb:dimension ?dim ."
      (string/join "\n" dimension-filters)
      (config/codelist-source configuration) " qb:codeList ?codelist ."
      "  ?codelist skos:member ?member ."
      "}")))

(defn get-codelist-members [repo configuration]
  (let [q (get-codelist-members-query configuration)]
    (util/eager-query repo q)))

(defn members->codelist [members]
  (util/map-values (fn [bindings]
                     (into #{} (map :member bindings)))
                   (group-by :codelist members)))

(defn get-all-codelists [repo configuration]
  (let [members (get-codelist-members repo configuration)]
    (members->codelist members)))

(defn set-component-orders [components]
  (let [has-order? (comp some? :order)
        with-order (filter has-order? components)
        without-order (remove has-order? components)
        max-order (if (seq with-order)
                    (apply max (map :order with-order))
                    0)]
    (concat (sort-by :order with-order)
            (map-indexed (fn [idx comp]
                           (assoc comp :order (+ max-order idx 1)))
                         without-order))))

(defn get-dimension-type [{:keys [uri label] :as dim} codelist-uri codelists configuration]
  (cond
    (= (config/geo-dimension configuration) uri)
    (types/->RefAreaType)

    (= (config/time-dimension configuration) uri)
    (types/->RefPeriodType)

    :else
    (let [codelist (util/strict-get codelists codelist-uri)
          enum-name (types/label->field-name label)]
      (types/->EnumType enum-name codelist))))

(defn construct-dataset [{ds-uri :ds ds-name :name :as dataset} components uri->dimension uri->measure codelists configuration]
  (let [dimension-components (filter (fn [comp] (some? (:dimension comp))) components)
        ordered-dim-components (set-component-orders dimension-components)
        dimensions (map (fn [{dim-uri :dimension order :order codelist-uri :codelist :as comp}]
                          (let [dimension (util/strict-get uri->dimension dim-uri)
                                type (get-dimension-type dimension codelist-uri codelists configuration)]
                            (types/->Dimension dim-uri (:label dimension) order type)))
                        ordered-dim-components)
        measure-components (filter (fn [comp] (some? (:measure comp))) components)
        ordered-measure-components (set-component-orders measure-components)
        measures (map (fn [{measure-uri :measure order :order :as comp}]
                        (let [{:keys [label is-numeric?]} (util/strict-get uri->measure measure-uri)
                              field-name (types/label->field-name label)
                              measure (types/->MeasureType measure-uri label order is-numeric?)]
                          (assoc measure :field-name field-name)))
                      ordered-measure-components)]
    (types/->Dataset ds-uri ds-name dimensions measures)))

(defn construct-datasets [datasets components dimensions measures codelists configuration]
  (let [dataset-components (group-by :ds components)
        uri->dimension (util/strict-map-by :uri dimensions)
        uri->measure (util/strict-map-by :uri measures)]
    (map (fn [{uri :ds :as ds}]
           (let [components (get dataset-components uri)]
             (construct-dataset ds components uri->dimension uri->measure codelists configuration)))
         datasets)))

(defn get-all-datasets
  "1. Find all datasets
   2. Find all components
   3. Find all codelists
   4. Find all dimensions
   5. Find all measures
   6. Find which measures are numeric"
  [repo configuration]
  (let [datasets (find-all-datasets repo configuration)
        components (get-all-components repo configuration)
        dimensions (find-all-dimensions repo configuration)
        measures (find-all-measures repo configuration)
        codelists (get-all-codelists repo configuration)]
    (construct-datasets datasets components dimensions measures codelists configuration)))
