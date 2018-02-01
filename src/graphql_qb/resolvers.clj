(ns graphql-qb.resolvers
  (:require [graphql-qb.queries :as queries]
            [graphql-qb.types :as types]
            [graphql-qb.types.scalars :as scalars]
            [graphql-qb.util :as util]
            [grafter.rdf.sparql :as sp]
            [graphql-qb.context :as context]
            [com.walmartlabs.lacinia.schema :as ls]
            [graphql-qb.query-model :as qm]
            [clojure.walk :as walk]
            [com.walmartlabs.lacinia.executor :as executor]
            [clojure.spec.alpha :as s]
            [clojure.pprint :as pp])
  (:import (graphql_qb.types Dimension MeasureType)))

(s/def ::order-direction #{:ASC :DESC})
(s/def ::dimension #(instance? Dimension %))
(s/def ::measure #(instance? MeasureType %))
(s/def ::dimension-measure (s/or :dimension ::dimension :measure ::measure))
(s/def ::order-item (s/cat :dimmeasure ::dimension-measure :direction ::order-direction))
(s/def ::order-by (s/coll-of ::order-item))
(s/def ::dimension-filter (constantly true))                ;TODO: specify properly
(s/def ::dimensions-filter (s/map-of ::dimension ::dimension-filter))

(defn un-namespace-keys [m]
  (walk/postwalk (fn [x]
                   (if (map? x)
                     (util/map-keys (fn [k] (keyword (name k))) x)
                     x)) m))

(defn flatten-selections [m]
  (walk/postwalk (fn [x]
                   (if (and (map? x) (contains? x :selections))
                     (:selections x)
                     x)) m))

(defn get-selections [context]
  (-> context (executor/selections-tree) (un-namespace-keys) (flatten-selections)))

(defn get-observation-selections [context]
  (get-in (get-selections context) [:page :observations]))

(defn get-observation-count [repo ds-uri filter-model]
  (let [query (qm/get-observation-count-query filter-model "obs" ds-uri)
        results (util/eager-query repo query)]
    (:c (first results))))

(defn resolve-observations [context args {:keys [uri] :as ds-field}]
  (let [repo (context/get-repository context)
        dimension-filter (::dimensions-filter args)
        filter-model (queries/get-observation-filter-model dimension-filter)
        total-matches (get-observation-count repo uri filter-model)]
    (merge
      (select-keys args [::dimensions-filter ::order-by])
      {::dataset                     ds-field
       ::filter-model                filter-model
       ::observation-selections      (get-observation-selections context)
       :total_matches                total-matches
       :aggregations                 {::dimensions-filter dimension-filter ::filter-model filter-model :ds-uri uri}})))

(defn resolve-observations-sparql-query [_context _args obs-field]
  (let [#::{:keys [dataset observation-selections order-by filter-model]} obs-field
        model (queries/filter-model->observations-query filter-model dataset order-by observation-selections)]
    (qm/get-query model "obs" (:uri dataset))))

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

(defn wrap-pagination-resolver [inner-resolver]
  (fn [context args observations-field]
    (let [limit (get-limit args)
          offset (get-offset args)
          total-matches (:total_matches observations-field)
          page {::page-offset offset ::page-size limit}
          result (inner-resolver context (assoc args ::page page) observations-field)
          page-count (count (::observation-results result))
          next-page (calculate-next-page-offset offset limit total-matches)]
      (assoc result :next_page next-page
                    :count page-count))))

(defn inner-resolve-observations-page [context args observations-field]
  (let [order-by (::order-by observations-field)
        dataset (::dataset observations-field)
        observation-selections (::observation-selections observations-field)
        filter-model (::filter-model observations-field)
        #::{:keys [page-offset page-size]} (::page args)
        query (queries/get-observation-page-query dataset filter-model page-size page-offset order-by observation-selections)
        repo (context/get-repository context)
        results (util/eager-query repo query)]
    {::observation-results results}))

(def resolve-observations-page (wrap-pagination-resolver inner-resolve-observations-page))

(defn resolve-datasets [context {:keys [dimensions measures uri] :as args} _parent]
  (let [repo (context/get-repository context)
        q (queries/get-datasets-query dimensions measures uri)
        results (util/eager-query repo q)]
    (map (fn [{:keys [title] :as bindings}]
           (-> bindings
               (util/rename-key :ds :uri)
               (update :issued #(some-> % scalars/grafter-date->datetime))
               (update :modified #(some-> % scalars/grafter-date->datetime))
               (assoc :schema (name (types/dataset-label->schema-name title)))))
         results)))

(defn exec-observation-aggregation [repo dataset measure filter-model aggregation-fn]
  (let [q (qm/get-observation-aggregation-query filter-model aggregation-fn (:uri dataset) (:uri measure))
        results (util/eager-query repo q)]
    (get (first results) aggregation-fn)))

(defn resolve-observations-aggregation [aggregation-fn
                                        context
                                        {:keys [measure] :as args}
                                        {:keys [ds-uri] :as aggregation-field}]
  (let [repo (context/get-repository context)
        dataset (context/get-dataset context ds-uri)
        filter-model (::filter-model aggregation-field)]
    (exec-observation-aggregation repo dataset measure filter-model aggregation-fn)))

(defn resolve-dataset-measures [context _args {:keys [uri] :as ds-field}]
  (let [repo (context/get-repository context)
        results (vec (sp/query "get-measure-types.sparql" {:ds uri} repo))]
    (mapv (fn [{:keys [mt label]}]
           {:uri       mt
            :label     (str label)
            :enum_name (name (types/enum-label->value-name (str label)))})
         results)))

(defn dimension-enum-value->graphql [{:keys [value label name] :as item}]
  (ls/tag-with-type
    {:uri (str value) :label (str label) :enum_name (clojure.core/name name)}
    :enum_dim_value))

(defn dimension-measure->graphql [{:keys [uri label] :as measure}]
  {:uri   uri
   :label (str label)
   :enum_name  (name (types/enum-label->value-name label))})

(defn dimension->graphql [unmapped-dimensions {:keys [uri type] :as dim}]
  (let [base-dim (dimension-measure->graphql dim)]
    (if (types/is-enum-type? type)
      (assoc base-dim :values (map dimension-enum-value->graphql (:values type)))
      (let [code-list (get unmapped-dimensions uri)]
        (assoc base-dim :values (map (fn [member] (ls/tag-with-type (util/rename-key member :member :uri) :unmapped_dim_value)) code-list))))))

(def measure->graphql dimension-measure->graphql)

(defn resolve-dataset [context {:keys [uri] :as dataset}]
  (context/get-dataset context uri))

(defn resolve-dataset-dimensions [context _args {:keys [uri] :as ds-field}]
  (let [{:keys [dimensions]} (context/get-dataset context uri)
        repo (context/get-repository context)
        unmapped-dimensions (queries/get-unmapped-dimension-values repo ds-field)]
    (map #(dimension->graphql unmapped-dimensions %) dimensions)))
