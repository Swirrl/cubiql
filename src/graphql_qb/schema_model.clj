(ns graphql-qb.schema-model
  (:require [graphql-qb.types :as types]
            [graphql-qb.resolvers :as resolvers]))

(defn get-dimension-measure-enum [dataset]
  (types/build-enum :ignored (types/dataset-dimension-measures dataset)))

(defn get-order-by [{:keys [order order_spec] :as args} dataset]
  (let [dim-measure-enum (get-dimension-measure-enum dataset)]
    (map (fn [enum-value]
           (let [{:keys [field-name] :as dim-measure} (types/from-graphql dim-measure-enum enum-value)
                 dir (get order_spec field-name)]
             [dim-measure (or dir :ASC)]))
         order)))

(defn map-dimension-filter [{filter :dimensions :as args} {:keys [dimensions] :as dataset}]
  (into {} (map (fn [{:keys [field-name] :as f}]
                  (let [graphql-value (get filter field-name)]
                    [f (types/from-graphql f graphql-value)]))
                dimensions)))

(defn observation-args-mapper [dataset]
  (fn [args]
    (let [dim-filter (map-dimension-filter args dataset)
          order-by (get-order-by args dataset)]
      (-> args
          (assoc ::resolvers/dimensions-filter dim-filter)
          (assoc ::resolvers/order-by order-by)))))

