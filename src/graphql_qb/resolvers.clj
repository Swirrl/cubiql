(ns graphql-qb.resolvers
  (:require [graphql-qb.queries :as queries]
            [graphql-qb.types :as types]
            [clojure.string :as string]
            [graphql-qb.util :as util]
            [grafter.rdf.sparql :as sp]
            [graphql-qb.context :as context]))

(defn get-observation-count [repo ds-uri ds-dimensions query-dimensions]
  (let [query (queries/get-observation-count-query ds-uri ds-dimensions query-dimensions)
        results (util/eager-query repo query)]
    (:c (first results))))

(defn get-dimension-measure-ordering [dataset sorts sort-spec]
  (map (fn [dm-enum]
         (let [dm (types/graphql-enum->dimension-measure dataset dm-enum)
               field (types/->field-name dm)
               sort-dir (get sort-spec field :ASC)]
           [dm sort-dir]))
       sorts))

(defn resolve-observations [context
                            {query-dimensions :dimensions order-by :order order-spec :order_spec :as args}
                            {:keys [uri] :as ds-field}]
  (let [repo (context/get-repository context)
        {:keys [dimensions] :as dataset} (context/get-dataset context uri)
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

(defn resolve-observations-page [context args observations-field]
  (let [repo (context/get-repository context)
        query-dimensions (::query-dimensions observations-field)
        order-by-dim-measures (::order-by-dimension-measures observations-field)
        ds-uri (get-in observations-field [::dataset :uri])
        {:keys [dimensions measures]} (context/get-dataset context ds-uri)
        limit (get-limit args)
        offset (get-offset args)
        total-matches (:total_matches observations-field)
        query (queries/get-observation-page-query ds-uri dimensions query-dimensions limit offset order-by-dim-measures)
        results (util/eager-query repo query)
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

(defn resolve-datasets [context {:keys [dimensions measures uri] :as args} _parent]
  (let [repo (context/get-repository context)
        q (queries/get-datasets-query dimensions measures uri)
        results (util/eager-query repo q)]
    (map (fn [{:keys [title] :as bindings}]
           (-> bindings
               (util/rename-key :ds :uri)
               (update :issued #(some-> % types/grafter-date->datetime))
               (update :modified #(some-> % types/grafter-date->datetime))
               (assoc :schema (name (types/dataset-label->schema-name title)))))
         results)))

(defn exec-observation-aggregation [repo dataset measure query-dimensions aggregation-fn]
  (let [q (queries/get-observation-aggregation-query aggregation-fn measure dataset query-dimensions)
        results (util/eager-query repo q)]
    (get (first results) aggregation-fn)))

(defn resolve-arguments [args type-mapping]
  ;;TODO: deal with nested type mappings
  (into {} (map (fn [[k graphql-value]]
                  (if-let [type (get type-mapping k)]
                    [k (types/from-graphql type graphql-value)]
                    [k graphql-value]))
                args)))

(defn wrap-resolver
  "Returns a resolver function which resolves the incoming GraphQL arguments according to the given
   type mapping (e.g. resolves enum values to their underlying values). Updates the argument maps with the
   resolved values and invokes the inner resolver function"
  [resolver-fn type-mapping]
  (fn [context args field]
    (let [resolved-args (resolve-arguments args type-mapping)
          updated-args (merge args resolved-args)]
      (resolver-fn context updated-args field))))

(defn resolve-observations-aggregation [aggregation-fn
                                        context
                                        {:keys [measure] :as args}
                                        {:keys [query-dimensions ds-uri] :as aggregation-field}]
  (let [repo (context/get-repository context)
        dataset (context/get-dataset context ds-uri)]
    (exec-observation-aggregation repo dataset measure query-dimensions aggregation-fn)))

(defn resolve-dataset-measures [context _args {:keys [uri] :as ds-field}]
  (let [repo (context/get-repository context)
        results (sp/query "get-measure-types.sparql" {:ds uri} repo)]
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

(defn resolve-dataset [context {:keys [uri dimensions measures] :as dataset}]
  (let [ds (context/get-dataset context uri)]
    (merge ds {:dimensions (map dimension->graphql dimensions)
               :measures (map measure->graphql measures)})))

(defn resolve-dataset-dimensions [context _args {:keys [uri] :as ds-field}]
  (let [{:keys [dimensions]} (context/get-dataset context uri)]
    (map dimension->graphql dimensions)))
