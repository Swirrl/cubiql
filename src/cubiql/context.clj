(ns cubiql.context
  "Functions for managing the graphql execution context passed into resolver functions"
  (:require [cubiql.util :as util]
            [clojure.walk :as walk]
            [com.walmartlabs.lacinia.executor :as executor]))

(defn create
  "Creates a context map from a repository and collection of datasets"
  [repo datasets dataset-mappings config]
  (let [uri->dataset (util/keyed-by :uri datasets)
        uri->dataset-mapping (util/keyed-by :uri dataset-mappings)]
    {:repo repo :uri->dataset uri->dataset :uri->dataset-mapping uri->dataset-mapping :config config}))

(defn get-dataset
  "Fetches a dataset from the context by its URI. Returns nil if the dataset was not found."
  [context dataset-uri]
  (get-in context [:uri->dataset dataset-uri]))

(defn get-dataset-mapping [context dataset-uri]
  (get-in context [:uri->dataset-mapping dataset-uri]))

(defn get-repository
  "Gets the SPARQL repository from the context"
  [context]
  (:repo context))

(defn get-configuration [context]
  (:config context))

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
