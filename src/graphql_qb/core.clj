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
                          (map (fn [{:keys [member label priority]}]
                                 (types/->EnumItem member label enum-name priority))
                               item-results)
                          (map-indexed (fn [n {:keys [member label priority]}]
                                         (types/->EnumItem member label (types/enum-label->value-name label (inc n)) priority))
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

;;TODO: get known dimensions label/docs from database
(def ref-area-dim
  {:uri vocab/sdmx:refArea
   :label "Reference Area"
   :doc "Reference area"
   :type (types/->RefAreaType nil)})

(def ref-period-dim
  {:uri vocab/sdmx:refPeriod
   :label "Reference Period"
   :doc "Reference period"
   :type (types/->RefPeriodType nil)})

(defn get-dataset-dimensions [ds-uri schema has-ref-area-dim? has-ref-period-dim? enum-dims]
  (let [known-dims (remove nil? [(if has-ref-area-dim? ref-area-dim) (if has-ref-period-dim? ref-period-dim)])
        enum-dims (map (fn [dim]
                         (update dim :type #(types/map->EnumType (assoc % :schema schema))))
                       enum-dims)
        dims (concat known-dims enum-dims)]
    (map-indexed (fn [index dim]
                   (-> dim
                       (assoc :ds-uri ds-uri)
                       (assoc :schema schema)
                       (assoc :order (inc index))
                       (types/map->Dimension)))
                 dims)))

(defn get-dataset-measures-mapping [measure-results]
  (util/map-values (fn [ds-measures]
                     (map-indexed (fn [idx {:keys [mt label is-numeric?]}]
                                    (types/->MeasureType mt label (inc idx) is-numeric?))
                                  ds-measures))
                   (group-by :ds measure-results)))

(defn construct-datasets [datasets dataset-enum-values dataset-measures ref-area-datasets ref-period-datasets]
  (let [dataset-enum-values (group-by :ds dataset-enum-values)]
    (map (fn [{uri :ds :as dataset}]
           (let [{:keys [title description issued modified publisher licence]} dataset
                 schema (types/dataset-schema dataset)
                 enum-dim-values (get dataset-enum-values uri)
                 measures-mapping (get-dataset-measures-mapping dataset-measures)
                 has-ref-area-dim? (contains? ref-area-datasets uri)
                 has-ref-period-dim? (contains? ref-period-datasets uri)
                 enum-dims (get-dataset-enum-dimensions enum-dim-values)
                 dimensions (get-dataset-dimensions uri schema has-ref-area-dim? has-ref-period-dim? enum-dims)
                 measures (or (get measures-mapping uri) [])
                 d (types/->Dataset uri title description dimensions measures)]
             (assoc d
               :issued (some-> issued (types/grafter-date->datetime))
               :modified (some-> modified (types/grafter-date->datetime))
               :publisher publisher
               :licence licence)))
         datasets)))

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
           (assoc measure :is-numeric? (contains? numeric-measures mt)))
         results)))

(defn get-all-datasets [repo]
  "1. Find all datasets
   2. Find URIs for all datasets containing refArea and refPeriod dimensions
   3. Get all dimension values for all enum dimensions
   4. Get all dataset measures
   5. Construct datasets"
  (let [datasets-query (queries/get-datasets-query nil nil nil)
        datasets (util/eager-query repo datasets-query)
        dataset-enum-values (vec (sp/query "get-all-enum-dimension-values.sparql" repo))
        dataset-measures (get-dataset-measures repo)
        ref-area-datasets (queries/get-datasets-containing-ref-area-dimension repo)
        ref-period-datasets (queries/get-datasets-containing-ref-period-dimension repo)]
    (construct-datasets datasets dataset-enum-values dataset-measures ref-area-datasets ref-period-datasets)))

(defn get-schema [datasets]
  (let [base-schema (read-edn-resource "base-schema.edn")
        base-schema (assoc base-schema :scalars types/custom-scalars)
        ds-schemas (map schema/get-dataset-schema datasets)
        {:keys [resolvers] :as combined-schema} (reduce (fn [acc schema] (merge-with merge acc schema)) base-schema ds-schemas)
        query-resolvers (merge {:resolve-observations resolvers/resolve-observations
                                :resolve-observations-page resolvers/resolve-observations-page
                                :resolve-datasets resolvers/resolve-datasets
                                :resolve-dataset-dimensions resolvers/resolve-dataset-dimensions
                                :resolve-dataset-measures resolvers/resolve-dataset-measures}
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
