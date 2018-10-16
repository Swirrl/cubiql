(ns graphql-qb.schema.mapping.dataset
  (:require [graphql-qb.util :as util]
            [graphql-qb.vocabulary :refer [qb:measureType]]))

(def uri :uri)
(def schema :schema)
(def dimensions :dimensions)
(def measures :measures)

(defn description [dataset-mapping]
  ;;TODO: add dataset description to mapping!
  "")

(defn components [dataset-mapping]
  (concat (dimensions dataset-mapping) (measures dataset-mapping)))

(defn numeric-measure-mappings [dataset-mapping]
  (seq (filter :is-numeric? (measures dataset-mapping))))

(defn- find-by-uri [components uri]
  (util/find-first (fn [comp] (= uri (:uri comp))) components))

(defn get-component-by-uri [dataset-mapping uri]
  (find-by-uri (components dataset-mapping) uri))

(defn get-dimension-by-uri [dataset-mapping uri]
  (find-by-uri (dimensions dataset-mapping) uri))

(defn get-measure-by-uri [dataset-mapping uri]
  (find-by-uri (measures dataset-mapping) uri))

(defn- find-by-enum-name [components enum-name]
  (util/find-first (fn [comp] (= enum-name (:enum-name comp))) components))

(defn get-component-by-enum-name [dataset-mapping enum-name]
  (find-by-enum-name (components dataset-mapping) enum-name))

(defn- find-by-field-name [components field-name]
  (util/find-first (fn [comp] (= field-name (:field-name comp))) components))

(defn get-component-by-field-name [dataset-mapping field-name]
  (find-by-field-name (components dataset-mapping) field-name))

(defn get-dimension-by-field-name [dataset-mapping field-name]
  (find-by-field-name (dimensions dataset-mapping) field-name))

(defn component-mapping->component [comp]
  (or (:dimension comp) (:measure comp)))

(defn components-enum-group [dataset-mapping]
  (:components-enum dataset-mapping))

(defn aggregation-measures-enum-group [dataset-mapping]
  (:aggregation-measures-enum dataset-mapping))

(defn has-measure-type-dimension?
  "Whether the given dataset has an explicit qb:measureType dimension"
  [dataset-mapping]
  (let [dims (dimensions dataset-mapping)
        measure-dim (some (fn [dim] (= qb:measureType (:uri dim))) dims)]
    (some? measure-dim)))