(ns graphql-qb.core
  (:require [grafter.rdf.repository :as repo]
            [grafter.rdf.sparql :as sp]
            [clojure.string :as string]
            [com.walmartlabs.lacinia.schema :as lschema]
            [com.walmartlabs.lacinia.util :refer [attach-resolvers]]
            [com.walmartlabs.lacinia :refer [execute]]
            [graphql-qb.util :refer [read-edn-resource rename-key] :as util]
            [graphql-qb.types :refer :all :as types]
            [clojure.pprint :as pprint]
            [graphql-qb.schema :as schema]
            [graphql-qb.queries :as queries])
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

(defn dimension-enum-value->graphql [{:keys [value label name] :as item}]
  {:uri (str value) :label (str label) :enum_name (clojure.core/name name)})

(defn dimension-measure->graphql [{:keys [uri label] :as measure}]
  {:uri   uri
   :label (str label)
   :enum_name  (name (:name (types/to-enum-value measure)))})

(def measure->graphql dimension-measure->graphql)

(defn dimension->graphql [{:keys [type] :as dim}]
  (let [base-dim (dimension-measure->graphql dim)]
    (if (types/is-enum-type? type)
      (assoc base-dim :values (map dimension-enum-value->graphql (:values type)))
      base-dim)))

(defn resolve-dataset-dimensions [{:keys [repo] :as context} _args ds-field]
  (let [dims (get-dimensions repo ds-field)]
    (map dimension->graphql dims)))

(defn resolve-dataset-measures [{:keys [repo] :as context} _args ds-field]
  (let [measures (get-measure-types repo ds-field)]
    (map measure->graphql measures)))

(defn get-observation-count [repo ds-uri ds-dimensions query-dimensions]
  (let [query (queries/get-observation-count-query ds-uri ds-dimensions query-dimensions)
        results (repo/query repo query)]
    (:c (first results))))

(def default-limit 10)
(def max-limit 1000)

(defn get-limit [args]
  (min (max 0 (or (:first args) default-limit)) max-limit))

(defn get-offset [args]
  (max 0 (or (:after args) 0)))

(defn calculate-next-page-offset [offset limit total-matches]
  (let [next-offset (+ offset limit)]
    (if (> total-matches next-offset)
      next-offset)))

(defn get-dimension-measure-ordering [dataset sorts sort-spec]
  (map (fn [dm-enum]
         (let [dm (types/graphql-enum->dimension-measure dataset dm-enum)
               field (types/->field-name dm)
               sort-dir (get sort-spec field :ASC)]
           [dm sort-dir]))
       sorts))

(defn resolve-observations [{:keys [repo uri->dataset] :as context}
                            {query-dimensions :dimensions order-by :order order-spec :order_spec :as args}
                            {:keys [uri] :as ds-field}]
  (let [{:keys [dimensions] :as dataset} (get uri->dataset uri)
          total-matches (get-observation-count repo uri dimensions query-dimensions)
          ordered-dim-measures (get-dimension-measure-ordering dataset order-by order-spec)
          query (queries/get-observation-query uri dimensions query-dimensions ordered-dim-measures)]
      {::query-dimensions query-dimensions
       ::order-by-dimension-measures ordered-dim-measures
       ::dataset ds-field
       :sparql (string/join (string/split query #"\n"))
       :total_matches total-matches
       :aggregations {:query-dimensions query-dimensions :ds-uri uri}}))

(defn resolve-observations-page [{:keys [repo uri->dataset] :as context} args observations-field]
  (let [query-dimensions (::query-dimensions observations-field)
        order-by-dim-measures (::order-by-dimension-measures observations-field)
        ds-uri (get-in observations-field [::dataset :uri])
        {:keys [dimensions measures]} (get uri->dataset ds-uri)
        limit (get-limit args)
        offset (get-offset args)
        total-matches (:total_matches observations-field)
        query (queries/get-observation-page-query ds-uri dimensions query-dimensions limit offset order-by-dim-measures)
        results (repo/query repo query)
        matches (mapv (fn [{:keys [obs mp mv] :as bindings}]
                        (let [dimension-values (map (fn [{:keys [field-name] :as ft}]
                                                      (let [result-key (keyword (types/->query-var-name ft))
                                                            value (get bindings result-key)]
                                                        [field-name (types/to-graphql ft value)]))
                                                    dimensions)
                              {measure-field :field-name :as obs-measure} (first (filter #(= mp (:uri %)) measures))
                              measure-value (types/to-graphql obs-measure mv)]
                          (into {:uri obs measure-field measure-value} dimension-values)))
                      results)
        next-page (calculate-next-page-offset offset limit total-matches)]
    {:next_page next-page
     :count (count matches)
     :result matches}))

(defn exec-observation-aggregation [repo uri->dataset measure query-dimensions ds-uri aggregation-fn]
  (let [dataset (get uri->dataset ds-uri)
        q (queries/get-observation-aggregation-query aggregation-fn measure dataset query-dimensions)
        results (repo/query repo q)]
    (get (first results) aggregation-fn)))

(defn resolve-observations-aggregation [aggregation-fn
                                        {:keys [repo uri->dataset] :as context}
                                        {:keys [measure] :as args}
                                        {:keys [query-dimensions ds-uri] :as aggregation-field}]
  (exec-observation-aggregation repo uri->dataset measure query-dimensions ds-uri aggregation-fn))

(defn resolve-datasets [{:keys [repo]} {:keys [dimensions measures uri] :as args} _parent]
  (let [q (queries/get-datasets-query dimensions measures uri)
        results (repo/query repo q)]
    (map (fn [{:keys [title] :as bindings}]
           (-> bindings
               (rename-key :ds :uri)
               (update :issued #(some-> % types/grafter-date->datetime))
               (update :modified #(some-> % types/grafter-date->datetime))
               (assoc :schema (name (dataset-label->schema-name title)))))
         results)))

(defn- transform-dataset-result [repo {:keys [uri title description] :as ds}]
  (let [schema (dataset-label->schema-name title)
        measures (get-measure-types repo {:uri uri})
        dims (get-dimensions repo {:uri uri :schema schema})]
    (types/->Dataset uri title description dims measures)))

(defn find-datasets [repo]
  (let [results (sp/query "get-datasets.sparql" repo)]
    (mapv #(transform-dataset-result repo %) results)))

(defn dataset->graphql [context {:keys [uri dimensions measures] :as dataset}]
  (let [ds (first (resolve-datasets context {:uri uri} nil))]
    (merge ds {:dimensions (map dimension->graphql dimensions)
               :measures (map measure->graphql measures)})))

(defn get-schema [datasets]
  (let [base-schema (read-edn-resource "base-schema.edn")
        base-schema (assoc base-schema :scalars types/custom-scalars)
        ds-schemas (map schema/get-dataset-schema datasets)
        combined-schema (reduce (fn [acc schema] (merge-with merge acc schema)) base-schema ds-schemas)
        schema-resolvers (into {} (map (fn [dataset]
                                         [(schema/dataset-resolver dataset) (fn [context args field]
                                                                              (dataset->graphql context dataset))])
                                       datasets))
        query-resolvers (merge {:resolve-observations resolve-observations
                                :resolve-observations-page resolve-observations-page
                                :resolve-datasets resolve-datasets
                                :resolve-dataset-dimensions resolve-dataset-dimensions
                                :resolve-dataset-measures resolve-dataset-measures
                                :resolve-observations-min (partial resolve-observations-aggregation :min)
                                :resolve-observations-max (partial resolve-observations-aggregation :max)
                                :resolve-observations-sum (partial resolve-observations-aggregation :sum)
                                :resolve-observations-average (partial resolve-observations-aggregation :avg)}
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
