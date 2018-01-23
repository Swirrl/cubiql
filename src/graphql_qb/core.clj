(ns graphql-qb.core
  (:require [grafter.rdf.sparql :as sp]
            [com.walmartlabs.lacinia.schema :as lschema]
            [com.walmartlabs.lacinia.util :refer [attach-resolvers]]
            [com.walmartlabs.lacinia :refer [execute]]
            [graphql-qb.util :refer [read-edn-resource rename-key] :as util]
            [graphql-qb.types :refer :all :as types]
            [clojure.pprint :as pprint]
            [graphql-qb.schema :as schema]
            [graphql-qb.resolvers :as resolvers]
            [graphql-qb.queries :as queries]
            [graphql-qb.vocabulary :as vocab]))

(defn code-list->enum-items [code-list]
  (let [by-enum-name (group-by #(types/enum-label->value-name (:label %)) code-list)
        items (mapcat (fn [[enum-name item-results]]
                        (if (= 1 (count item-results))
                          (map (fn [{:keys [member label]}]
                                 (types/->EnumItem member label enum-name))
                               item-results)
                          (map-indexed (fn [n {:keys [member label]}]
                                         (types/->EnumItem member label (types/enum-label->value-name label (inc n))))
                                       item-results)))
                      by-enum-name)]
    (vec items)))

(defn get-dataset-enum-dimensions [enum-dim-values]
  (map (fn [[dim values]]
         (let [{:keys [label doc]} (first values)
               code-list (map #(util/rename-key % :vallabel :label) values)
               items (code-list->enum-items code-list)
               enum-name (types/label->field-name label)]
           {:uri   dim
            :label (str label)
            :doc (str doc)
            :type {:enum-name enum-name :values items}}))
       (group-by :dim enum-dim-values)))

(defn get-dataset-dimensions [ds-uri schema known-dimensions known-dimension-members enum-dims]
  (let [known-dims (filter (fn [{:keys [uri]}]
                             (contains? (get known-dimension-members uri) ds-uri))
                           known-dimensions)
        enum-dims (map (fn [dim]
                         (update dim :type #(types/map->EnumType (assoc % :schema schema))))
                       enum-dims)
        dims (concat known-dims enum-dims)]
    (map-indexed (fn [index dim]
                   (-> dim
                       (assoc :ds-uri ds-uri)
                       (assoc :schema schema)
                       (assoc :order (inc index))
                       (assoc :field-name (types/->field-name dim))
                       (types/map->Dimension)))
                 dims)))

(defn get-dataset-measures-mapping [measure-results]
  (util/map-values (fn [ds-measures]
                     (map-indexed (fn [idx {:keys [mt label is-numeric? field-name]}]
                                    (assoc (types/->MeasureType mt label (inc idx) is-numeric?) :field-name field-name))
                                  ds-measures))
                   (group-by :ds measure-results)))

(defn can-generate-schema?
  "Indicates whether a GraphQL schema can be generated for the given dataset"
  [dataset]
  (not (empty? (types/dataset-dimension-measures dataset))))

(defn construct-datasets [datasets dataset-enum-values dataset-measures known-dimensions known-dimension-members]
  (let [dataset-enum-values (group-by :ds dataset-enum-values)
        datasets (map (fn [{uri :ds :as dataset}]
                        (let [{:keys [title description issued modified publisher licence]} dataset
                              schema (types/dataset-schema dataset)
                              enum-dim-values (get dataset-enum-values uri)
                              measures-mapping (get-dataset-measures-mapping dataset-measures)
                              enum-dims (get-dataset-enum-dimensions enum-dim-values)
                              dimensions (get-dataset-dimensions uri schema known-dimensions known-dimension-members enum-dims)
                              measures (or (get measures-mapping uri) [])
                              d (types/->Dataset uri title description dimensions measures)]
                          (assoc d
                            :issued (some-> issued (types/grafter-date->datetime))
                            :modified (some-> modified (types/grafter-date->datetime))
                            :publisher publisher
                            :licence licence)))
                      datasets)]
    (filter can-generate-schema? datasets)))

(defn get-numeric-measures [repo measures]
  (into #{} (filter (fn [measure-uri]
                      (let [results (vec (sp/query "sample-observation-measure.sparql" {:mt measure-uri} repo))]
                        (number? (:measure (first results)))))
                    measures)))

(defn get-dataset-measures [repo]
  (let [results (vec (sp/query "get-measure-types.sparql" repo))
        measure-type-uris (distinct (map :mt results))
        numeric-measures (get-numeric-measures repo measure-type-uris)]
    (map (fn [{:keys [mt] :as measure}]
           (-> measure
               (assoc :is-numeric? (contains? numeric-measures mt))
               (assoc :field-name (types/->field-name measure))))
         results)))

(defn collect-dimensions-results [known-dimension-types results]
  (let [dim-results (group-by :dim results)
        m (map (fn [[dim-uri type]]
                 (if-let [{:keys [label comment]} (first (get dim-results dim-uri))]
                   {:uri dim-uri :type type :label (str label) :doc (str comment)}))
               known-dimension-types)]
    (remove #(nil? (second %)) m)))

(defn get-known-dimensions [repo known-dimension-types]
  (let [q (queries/get-dimensions-query (map first known-dimension-types))
        results (util/eager-query repo q)]
    (collect-dimensions-results known-dimension-types results)))

(defn get-all-datasets [repo]
  "1. Find all datasets
   2. Lookup details of ref area and ref period dimensions
   3. Find URIs for all datasets containing refArea and refPeriod dimensions
   4. Get all dimension values for all enum dimensions
   5. Get all dataset measures
   6. Construct datasets"
  (let [datasets-query (queries/get-datasets-query nil nil nil)
        datasets (util/eager-query repo datasets-query)
        known-dimension-types [[vocab/sdmx:refArea (types/->RefAreaType)]
                               [vocab/sdmx:refPeriod (types/->RefPeriodType)]]
        known-dimensions (get-known-dimensions repo known-dimension-types)
        known-dimension-members (into {} (map (fn [[dim-uri _type]]
                                                [dim-uri (queries/get-datasets-containing-dimension repo dim-uri)])
                                              known-dimension-types))
        dataset-enum-values (vec (sp/query "get-all-enum-dimension-values.sparql" repo))
        dataset-measures (get-dataset-measures repo)]
    (construct-datasets datasets dataset-enum-values dataset-measures known-dimensions known-dimension-members)))

(defn get-schema [datasets]
  (let [base-schema (read-edn-resource "base-schema.edn")
        base-schema (assoc base-schema :scalars types/custom-scalars)
        ds-schemas (map schema/get-dataset-schema datasets)
        {:keys [resolvers] :as combined-schema} (reduce (fn [acc schema] (merge-with merge acc schema)) base-schema ds-schemas)
        query-resolvers (merge {:resolve-observation-sparql-query resolvers/resolve-observations-sparql-query
                                :resolve-datasets                 resolvers/resolve-datasets
                                :resolve-dataset-dimensions       resolvers/resolve-dataset-dimensions
                                :resolve-dataset-measures         resolvers/resolve-dataset-measures}
                               resolvers)]
    (attach-resolvers (dissoc combined-schema :resolvers) query-resolvers)))

(defn dump-schema [repo]
  (let [datasets (get-all-datasets repo)
        schema (get-schema datasets)]
    (pprint/pprint schema)))

(defn build-schema-context [repo]
  (let [datasets (get-all-datasets repo)
        schema (get-schema datasets)]
    {:schema (lschema/compile schema)
     :datasets datasets}))
