(ns graphql-qb.core
  (:require [grafter.rdf.sparql :as sp]
            [com.walmartlabs.lacinia.schema :as lschema]
            [com.walmartlabs.lacinia.util :refer [attach-resolvers]]
            [com.walmartlabs.lacinia :refer [execute]]
            [graphql-qb.util :refer [read-edn-resource rename-key] :as util]
            [graphql-qb.types :refer :all :as types]
            [graphql-qb.types.scalars :as scalars]
            [clojure.pprint :as pprint]
            [graphql-qb.schema :as schema]
            [graphql-qb.resolvers :as resolvers]
            [graphql-qb.queries :as queries]
            [graphql-qb.schema-model :as sm]
            [graphql-qb.schema.mapping.labels :as mapping]
            [graphql-qb.config :as config]))

(defn get-dataset-enum-dimensions [enum-dim-values]
  (map (fn [[dim values]]
         (let [{:keys [label]} (first values)
               items (into #{} (map :member values))
               enum-name (types/label->field-name label)]
           {:uri   dim
            :label (str label)
            :type (types/->EnumType enum-name items)}))
       (group-by :dim enum-dim-values)))

(defn get-dataset-dimensions [ds-uri known-dimensions known-dimension-members enum-dims]
  (let [known-dims (filter (fn [{:keys [uri]}]
                             (contains? (get known-dimension-members uri) ds-uri))
                           known-dimensions)
        dims (concat known-dims enum-dims)]
    (map-indexed (fn [index dim]
                   (-> dim
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
                        (let [{:keys [issued modified publisher licence]} dataset
                              enum-dim-values (get dataset-enum-values uri)
                              measures-mapping (get-dataset-measures-mapping dataset-measures)
                              enum-dims (get-dataset-enum-dimensions enum-dim-values)
                              dimensions (get-dataset-dimensions uri known-dimensions known-dimension-members enum-dims)
                              measures (or (get measures-mapping uri) [])
                              d (types/->Dataset uri (:name dataset) dimensions measures)]
                          (assoc d
                            :issued (some-> issued (scalars/grafter-date->datetime))
                            :modified (some-> modified (scalars/grafter-date->datetime))
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
  (let [dim-results (group-by :dim results)]
    (map (fn [[dim-uri type]]
           (if-let [{:keys [label]} (first (get dim-results dim-uri))]
             {:uri dim-uri :type type :label (str label)}))
         known-dimension-types)))

(defn get-known-dimensions [repo known-dimension-types config]
  (let [q (queries/get-dimensions-query (map first known-dimension-types) config)
        results (util/eager-query repo q)]
    (collect-dimensions-results known-dimension-types results)))

(defn get-all-datasets
  [repo configuration]
  "1. Find all datasets
   2. Lookup details of ref area and ref period dimensions
   3. Find URIs for all datasets containing refArea and refPeriod dimensions
   4. Get all dimension values for all enum dimensions
   5. Get all dataset measures
   6. Construct datasets"
  (let [datasets (queries/get-datasets repo nil nil nil configuration nil)
        area-dim (config/geo-dimension configuration)
        time-dim (config/time-dimension configuration)
        known-dimension-types [[area-dim (types/->RefAreaType)]
                               [time-dim (types/->RefPeriodType)]]
        known-dimensions (get-known-dimensions repo known-dimension-types configuration)
        known-dimension-members (into {} (map (fn [[dim-uri _type]]
                                                [dim-uri (queries/get-datasets-containing-dimension repo dim-uri)])
                                              known-dimension-types))
        enum-dimension-values-query (queries/get-all-enum-dimension-values configuration)
        dataset-enum-values (util/eager-query repo enum-dimension-values-query)
        dataset-measures (get-dataset-measures repo)]
    (construct-datasets datasets dataset-enum-values dataset-measures known-dimensions known-dimension-members)))

(defn get-datasets-enum-mappings
  [repo config]
  (let [enum-dimension-values-query (queries/get-all-enum-dimension-values config)
        dataset-enum-values (util/eager-query repo enum-dimension-values-query)]
    (mapping/get-datasets-enum-mappings dataset-enum-values)))

(defn get-datasets-measures-mapping [repo]
  (let [measure-values (vec (sp/query "get-measure-types.sparql" {} repo))]
    (mapping/get-measure-mappings measure-values)))

(defn get-schema [datasets enum-mappings measure-mappings]
  (let [base-schema (read-edn-resource "base-schema.edn")
        base-schema (assoc base-schema :scalars scalars/custom-scalars)
        {:keys [qb-fields schema]} (schema/get-qb-fields-schema datasets enum-mappings measure-mappings)
        base-schema (update-in base-schema [:objects :qb :fields] merge qb-fields)
        {:keys [resolvers] :as combined-schema} (sm/merge-schemas base-schema schema)
        query-resolvers (merge {:resolve-observation-sparql-query resolvers/resolve-observations-sparql-query
                                :resolve-datasets                 (resolvers/wrap-options resolvers/resolve-datasets)
                                :resolve-dataset-dimensions       (resolvers/dataset-dimensions-resolver enum-mappings)
                                :resolve-dataset-measures         (resolvers/dataset-measures-resolver measure-mappings)
                                :resolve-cuibiql                  resolvers/resolve-cubiql}
                               resolvers)]
    (attach-resolvers (dissoc combined-schema :resolvers) query-resolvers)))

(defn generate-schema [repo]
  (let [config (config/read-config)
        datasets (get-all-datasets repo config)
        enum-mappings (get-datasets-enum-mappings repo config)
        measure-mappings (get-datasets-measures-mapping repo)]
    (get-schema datasets enum-mappings measure-mappings)))

(defn dump-schema [repo]
  (pprint/pprint (generate-schema repo)))

(defn build-schema-context [repo config]
  (let [datasets (get-all-datasets repo config)
        enum-mappings (get-datasets-enum-mappings repo config)
        measure-mappings (get-datasets-measures-mapping repo)
        schema (get-schema datasets enum-mappings measure-mappings)]
    {:schema (lschema/compile schema)
     :datasets datasets}))
