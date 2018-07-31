(ns graphql-qb.resolvers
  (:require [graphql-qb.queries :as queries]
            [graphql-qb.types :as types]
            [graphql-qb.types.scalars :as scalars]
            [graphql-qb.util :as util]
            [graphql-qb.context :as context]
            [graphql-qb.query-model :as qm]
            [clojure.walk :as walk]
            [com.walmartlabs.lacinia.executor :as executor]
            [clojure.spec.alpha :as s]
            [clojure.pprint :as pp]
            [graphql-qb.schema.mapping.labels :as mapping])
  (:import [graphql_qb.types Dimension MeasureType]))

(defn wrap-post-resolver [inner-resolver f]
  (fn [context args field]
    (f (inner-resolver context args field))))

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

(defn resolve-observations [context args field]
  (let [{:keys [uri] :as dataset} (::dataset field)
        repo (context/get-repository context)
        dimension-filter (::dimensions-filter args)
        filter-model (queries/get-observation-filter-model dimension-filter)
        total-matches (get-observation-count repo uri filter-model)]
    (merge
      (select-keys args [::dimensions-filter ::order-by])
      {::dataset                     dataset
       ::filter-model                filter-model
       ::observation-selections      (get-observation-selections context)
       :total_matches                total-matches
       :aggregations                 {::dimensions-filter dimension-filter ::filter-model filter-model :ds-uri uri}})))

(defn resolve-observations-sparql-query [context _args obs-field]
  (let [config (context/get-configuration context)
        #::{:keys [dataset observation-selections order-by filter-model]} obs-field
        model (queries/filter-model->observations-query filter-model dataset order-by observation-selections config)]
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
        config (context/get-configuration context)
        query (queries/get-observation-page-query dataset filter-model page-size page-offset order-by observation-selections config)
        repo (context/get-repository context)
        results (util/eager-query repo query)]
    {::observation-results results}))

(def resolve-observations-page (wrap-pagination-resolver inner-resolve-observations-page))

(defn resolve-datasets [context {:keys [dimensions measures uri] :as args} _parent]
  (let [repo (context/get-repository context)
        config (context/get-configuration context)
        q (queries/get-datasets-query dimensions measures uri config)
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

(defn dataset-measures-resolver [all-measure-mappings]
  (fn [_context _args {:keys [uri] :as dataset}]
    (get all-measure-mappings uri)))

(defn dataset-resolver [dataset]
  (fn [context args field]
    {::dataset dataset}))

(defn wrap-options [inner-resolver]
  (fn [context args field]
    (let [opts (::options field)
          result (inner-resolver context args field)]
      (assoc result ::options opts))))

(defn get-lang [field]
  (get-in field [::options ::lang]))

(defn dataset-dimensions-resolver [all-enum-mappings]
  (fn [context _args {:keys [uri] :as ds-field}]
    (let [lang (get-lang ds-field)
          repo (context/get-repository context)
          dataset (context/get-dataset context uri)
          config (context/get-configuration context)
          ds-enum-mappings (get all-enum-mappings uri)
          unmapped-dimensions (queries/get-unmapped-dimension-values repo dataset config lang)]
      (mapping/format-dataset-dimension-values dataset ds-enum-mappings unmapped-dimensions))))

(defn resolve-cubiql [_context {lang :lang_preference :as args} _field]
  {::options {::lang lang}})

(defn create-dataset-dimensions-resolver [dataset dataset-enum-mappings]
  (fn [context _args field]
    (let [lang (get-lang field)
          repo (context/get-repository context)
          config (context/get-configuration context)
          unmapped-dims (queries/get-unmapped-dimension-values repo dataset config lang)]
      (mapping/format-dataset-dimension-values dataset dataset-enum-mappings unmapped-dims))))