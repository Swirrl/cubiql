(ns graphql-qb.resolvers
  (:require [graphql-qb.queries :as queries]
            [graphql-qb.util :as util]
            [graphql-qb.context :as context]
            [graphql-qb.query-model :as qm]
            [clojure.spec.alpha :as s]
            [graphql-qb.config :as config])
  (:import [graphql_qb.types Dimension MeasureType]))

(s/def ::order-direction #{:ASC :DESC})
(s/def ::dimension #(instance? Dimension %))
(s/def ::measure #(instance? MeasureType %))
(s/def ::dimension-measure (s/or :dimension ::dimension :measure ::measure))
(s/def ::order-item (s/cat :dimmeasure ::dimension-measure :direction ::order-direction))
(s/def ::order-by (s/coll-of ::order-item))
(s/def ::dimension-filter (constantly true))                ;TODO: specify properly
(s/def ::dimensions-filter (s/map-of ::dimension ::dimension-filter))

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
       :total_matches                total-matches
       :aggregations                 {::dimensions-filter dimension-filter ::filter-model filter-model :ds-uri uri}})))

(defn resolve-observations-sparql-query [context _args obs-field]
  (let [config (context/get-configuration context)
        #::{:keys [dataset observation-selections order-by filter-model]} obs-field
        model (queries/filter-model->observations-query filter-model dataset order-by observation-selections config)]
    (qm/get-query model "obs" (:uri dataset))))

(def default-limit 10)
(def default-max-observations-page-size 1000)

(defn get-limit [args configuration]
  (let [max-limit (or (config/max-observations-page-size configuration) default-max-observations-page-size)]
    (min (max 0 (or (:first args) default-limit)) max-limit)))

(defn get-offset [args]
  (max 0 (or (:after args) 0)))

(defn calculate-next-page-offset [offset limit total-matches]
  (let [next-offset (+ offset limit)]
    (if (> total-matches next-offset)
      next-offset)))

(defn wrap-pagination-resolver [inner-resolver]
  (fn [context args observations-field]
    (let [configuration (context/get-configuration context)
          limit (get-limit args configuration)
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

(defn get-lang [field]
  (get-in field [::options ::lang]))

(defn wrap-options [inner-resolver]
  (fn [context args field]
    (let [opts (::options field)
          result (inner-resolver context args field)]
      (cond
        (map? result)
        (assoc result ::options opts)

        (seqable? result)
        (map (fn [r] (assoc r ::options opts)) result)

        :else
        (throw (ex-info "Unexpected result type when associating options" {:result result}))))))

(defn resolve-datasets [context {:keys [dimensions measures uri] :as args} parent-field]
  (let [repo (context/get-repository context)
        config (context/get-configuration context)
        lang (get-lang parent-field)
        results (queries/get-datasets repo dimensions measures uri config)]
    (mapv (fn [ds]
            (let [{:keys [uri] :as dataset} (util/rename-key ds :ds :uri :strict? true)
                  metadata (queries/get-dataset-metadata repo uri config lang)
                  with-metadata (merge dataset metadata)
                  {:keys [schema] :as dataset-mapping} (context/get-dataset-mapping context uri)]
              (assoc with-metadata :schema (name schema))))
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

(defn- resolve-dataset-measures [repo dataset-uri lang configuration]
  (let [q (queries/get-measures-by-lang-query dataset-uri lang configuration)
        results (util/eager-query repo q)]
    (mapv (fn [{:keys [mt label]}]
            {:uri   mt
             :label (util/label->string label)})
          results)))

(defn dataset-resolver [{:keys [uri] :as dataset-mapping}]
  (fn [context _args field]
    (let [repo (context/get-repository context)
          config (context/get-configuration context)
          lang (get-lang field)
          dataset (context/get-dataset context uri)
          metadata (queries/get-dataset-metadata repo uri config lang)
          dataset-result (merge dataset-mapping metadata)]
      (assoc dataset-result ::dataset dataset))))

(defn combine-dimension-results [dimension-results dimension-codelist-results]
  (let [dimension-uri->codelist (group-by :dim dimension-codelist-results)]
    (mapv (fn [{:keys [uri] :as dim}]
            (if-let [codelist-members (get dimension-uri->codelist uri)]
              (let [values (map (fn [{:keys [member label]}] {:uri member :label label}) codelist-members)]
                (assoc dim :values values))
              dim))
          dimension-results)))

(defn get-dataset-dimensions [repo dataset-uri lang configuration]
  (let [q (queries/get-dimensions-by-lang-query dataset-uri lang configuration)
        results (util/eager-query repo q)]
    (map (fn [{:keys [dim label] :as bindings}]
           {:uri dim
            :label (util/label->string label)})
         results)))

(defn dataset-dimensions-resolver [context _args {dataset-uri :uri :as ds-field}]
  (let [lang (get-lang ds-field)
        repo (context/get-repository context)
        dataset (context/get-dataset context dataset-uri)
        config (context/get-configuration context)
        dimension-results (get-dataset-dimensions repo dataset-uri lang config)
        dimension-codelists (queries/get-dimension-codelist-values repo dataset config lang)]
    (combine-dimension-results dimension-results dimension-codelists)))

(defn resolve-cubiql [_context {lang :lang_preference :as args} _field]
  {::options {::lang lang}})

(defn create-dataset-dimensions-resolver [dataset-mapping]
  (fn [context _args {dataset-uri :uri :as field}]
    (let [lang (get-lang field)
          repo (context/get-repository context)
          config (context/get-configuration context)
          dimension-results (get-dataset-dimensions repo dataset-uri lang config)
          dimension-codelists (queries/get-dimension-codelist-values repo dataset-mapping config lang)]
      (combine-dimension-results dimension-results dimension-codelists))))

(defn create-dataset-measures-resolver [dataset-mapping]
  (fn [context _args field]
    (let [lang (get-lang field)
          repo (context/get-repository context)
          configuration (context/get-configuration context)]
      (resolve-dataset-measures repo (:uri dataset-mapping) lang configuration))))