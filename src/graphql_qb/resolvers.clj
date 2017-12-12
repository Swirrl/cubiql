(ns graphql-qb.resolvers
  (:require [graphql-qb.queries :as queries]
            [graphql-qb.types :as types]
            [grafter.rdf.repository :as repo]
            [clojure.string :as string]
            [graphql-qb.util :as util]
            [grafter.rdf.sparql :as sp]))

(defn get-observation-count [repo ds-uri ds-dimensions query-dimensions]
  (let [query (queries/get-observation-count-query ds-uri ds-dimensions query-dimensions)
        results (repo/query repo query)]
    (:c (first results))))

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
    {::query-dimensions            query-dimensions
     ::order-by-dimension-measures ordered-dim-measures
     ::dataset                     ds-field
     :sparql                       (string/join (string/split query #"\n"))
     :total_matches                total-matches
     :aggregations                 {:query-dimensions query-dimensions :ds-uri uri}}))

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

(defn resolve-datasets [{:keys [repo]} {:keys [dimensions measures uri] :as args} _parent]
  (let [q (queries/get-datasets-query dimensions measures uri)
        results (repo/query repo q)]
    (map (fn [{:keys [title] :as bindings}]
           (-> bindings
               (util/rename-key :ds :uri)
               (update :issued #(some-> % types/grafter-date->datetime))
               (update :modified #(some-> % types/grafter-date->datetime))
               (assoc :schema (name (types/dataset-label->schema-name title)))))
         results)))

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

(defn resolve-dataset-measures [{:keys [repo] :as context} _args {:keys [uri] :as ds-field}]
  (let [results (sp/query "get-measure-types.sparql" {:ds uri} repo)]
    (mapv (fn [{:keys [mt label]}]
            {:uri       mt
             :label     (str label)
             :enum_name (name (types/enum-label->value-name (str label)))})
          results)))

(defn dimension-enum-value->graphql [{:keys [value label name] :as item}]
  {:uri (str value) :label (str label) :enum_name (clojure.core/name name)})

(defn dimension-measure->graphql [{:keys [uri label] :as measure}]
  {:uri   uri
   :label (str label)
   :enum_name  (name (:name (types/to-enum-value measure)))})


(defn dimension->graphql [{:keys [type] :as dim}]
  (let [base-dim (dimension-measure->graphql dim)]
    (if (types/is-enum-type? type)
      (assoc base-dim :values (map dimension-enum-value->graphql (:values type)))
      base-dim)))

(def measure->graphql dimension-measure->graphql)

(defn resolve-dataset [{:keys [uri->dataset] :as context} {:keys [uri dimensions measures] :as dataset}]
  (let [ds (get uri->dataset uri)]
    (merge ds {:dimensions (map dimension->graphql dimensions)
               :measures (map measure->graphql measures)})))

(defn resolve-dataset-dimensions [{:keys [uri->dataset] :as context} _args {:keys [uri] :as ds-field}]
  (let [{:keys [dimensions] :as ds} (get uri->dataset uri)]
    (map dimension->graphql dimensions)))
