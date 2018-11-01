(ns graphql-qb.core
  (:require [com.walmartlabs.lacinia.schema :as lschema]
            [com.walmartlabs.lacinia.util :refer [attach-resolvers]]
            [com.walmartlabs.lacinia :refer [execute]]
            [graphql-qb.util :refer [read-edn-resource]]
            [graphql-qb.types :refer :all :as types]
            [graphql-qb.types.scalars :as scalars]
            [graphql-qb.schema :as schema]
            [graphql-qb.resolvers :as resolvers]
            [graphql-qb.schema-model :as sm]
            [graphql-qb.schema.mapping.labels :as mapping]
            [graphql-qb.config :as config]
            [graphql-qb.dataset-model :as ds-model]))

(defn can-generate-schema?
  "Indicates whether a GraphQL schema can be generated for the given dataset"
  [dataset]
  (not (empty? (types/dataset-dimension-measures dataset))))

(defn build-schema [dataset-mappings]
  (let [base-schema (read-edn-resource "base-schema.edn")
        base-schema (assoc base-schema :scalars scalars/custom-scalars)
        {:keys [qb-fields schema]} (schema/get-qb-fields-schema dataset-mappings)
        base-schema (update-in base-schema [:objects :qb :fields] merge qb-fields)
        {:keys [resolvers] :as combined-schema} (sm/merge-schemas base-schema schema)
        query-resolvers (merge {:resolve-observation-sparql-query resolvers/resolve-observations-sparql-query
                                :resolve-datasets                 (resolvers/wrap-options resolvers/resolve-datasets)
                                :resolve-dataset-dimensions       (schema/create-global-dataset-dimensions-resolver dataset-mappings)
                                :resolve-dataset-measures         schema/global-dataset-measures-resolver
                                :resolve-cuibiql                  resolvers/resolve-cubiql}
                               resolvers)]
    (attach-resolvers (dissoc combined-schema :resolvers) query-resolvers)))

(defn- get-schema-components [repo config]
  (let [datasets (ds-model/get-all-datasets repo config)
        datasets (filter can-generate-schema? datasets)
        dataset-mappings (mapping/get-dataset-mapping-models repo datasets config)]
    {:schema (build-schema dataset-mappings)
     :datasets datasets
     :dataset-mappings dataset-mappings}))

(defn get-schema [repo]
  (:schema (get-schema-components repo (config/read-config))))

(defn build-schema-context [repo config]
  (let [result (get-schema-components repo config)]
    (update result :schema lschema/compile)))
