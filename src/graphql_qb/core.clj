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
            [grafter.rdf.repository :as repo])
  (:import [java.net URI]))

(defn get-enum-items [repo {:keys [ds-uri uri] :as dim}]
  (let [results (util/distinct-by :member (sp/query "get-enum-values.sparql" {:ds ds-uri :dim uri} repo))
        by-enum-name (group-by #(types/enum-label->value-name (:label %)) results)
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

(defn get-dimension-type [repo {:keys [uri label] :as dim} {:keys [schema] :as ds}]
  (cond
    (= (URI. "http://purl.org/linked-data/sdmx/2009/dimension#refArea") uri)
    (types/->RefAreaType)
    
    (= (URI. "http://purl.org/linked-data/sdmx/2009/dimension#refPeriod") uri)
    (types/->RefPeriodType)
    
    :else
    (let [items (get-enum-items repo dim)
          enum-name (types/label->enum-name label)]
      (types/->EnumType schema enum-name items))))

(defn get-dimensions
  [repo {:keys [uri schema] :as ds}]
  (let [results (util/distinct-by :dim (sp/query "get-dimensions.sparql" {:ds uri} repo))
        dims (map-indexed (fn [idx bindings]
                            (let [dim (-> bindings
                                          (assoc :ds-uri uri)
                                          (assoc :schema schema)
                                          (assoc :order (inc idx))
                                          (rename-key :dim :uri))
                                  type (get-dimension-type repo dim ds)
                                  dim-rec (types/map->Dimension (assoc dim :type type))]
                              (assoc dim-rec :field-name (->field-name dim))))
                          results)]
    (vec dims)))

(defn is-measure-numeric? [repo ds-uri measure-uri]
  (let [results (vec (sp/query "sample-observation-measure.sparql" {:ds ds-uri :mt measure-uri} repo))]
    (number? (:measure (first results)))))

(defn get-measure-types [repo {:keys [uri] :as ds}]
  (let [results (vec (sp/query "get-measure-types.sparql" {:ds uri} repo))]
    (map-indexed (fn [idx {:keys [mt label] :as bindings}]
                   (let [measure-type (types/->MeasureType mt label (inc idx) (is-measure-numeric? repo uri mt))]
                     (assoc measure-type :field-name (->field-name bindings)))) results)))

(defn- transform-dataset-result [repo {:keys [ds title description issued modified licence publisher] :as dataset} get-dims-and-measures]
  (let [uri ds
        schema (dataset-label->schema-name title)
        measures (if get-dims-and-measures (get-measure-types repo {:uri uri}))
        dims (if get-dims-and-measures (get-dimensions repo {:uri uri :schema schema}))]
    (assoc (types/->Dataset uri title description dims measures)
      :issued (some-> issued (types/grafter-date->datetime))
      :modified (some-> modified (types/grafter-date->datetime))
      :publisher publisher
      :licence licence)))

(defn find-datasets [repo]
  (let [q (queries/get-datasets-query nil nil nil)
        results (repo/query repo q)]
    (mapv #(transform-dataset-result repo % true) results)))

(defn get-schema [datasets]
  (let [base-schema (read-edn-resource "base-schema.edn")
        base-schema (assoc base-schema :scalars types/custom-scalars)
        ds-schemas (map schema/get-dataset-schema datasets)
        combined-schema (reduce (fn [acc schema] (merge-with merge acc schema)) base-schema ds-schemas)
        schema-resolvers (into {} (map (fn [dataset]
                                         [(schema/dataset-resolver dataset) (fn [context args field]
                                                                              (resolvers/resolve-dataset context dataset))])
                                       datasets))
        query-resolvers (merge {:resolve-observations resolvers/resolve-observations
                                :resolve-observations-page resolvers/resolve-observations-page
                                :resolve-datasets resolvers/resolve-datasets
                                :resolve-dataset-dimensions resolvers/resolve-dataset-dimensions
                                :resolve-dataset-measures resolvers/resolve-dataset-measures
                                :resolve-observations-min (partial resolvers/resolve-observations-aggregation :min)
                                :resolve-observations-max (partial resolvers/resolve-observations-aggregation :max)
                                :resolve-observations-sum (partial resolvers/resolve-observations-aggregation :sum)
                                :resolve-observations-average (partial resolvers/resolve-observations-aggregation :avg)}
                               schema-resolvers)]
    (attach-resolvers combined-schema query-resolvers)))

(defn dump-schema [repo]
  (let [datasets (find-datasets repo)
        schema (get-schema datasets)]
    (pprint/pprint schema)))

(defn build-schema-context [repo]
  (let [datasets (find-datasets repo)
        schema (get-schema datasets)]
    {:schema (lschema/compile schema)
     :datasets datasets}))
