(ns graphql-qb.context
  "Functions for managing the graphql execution context passed into resolver functions"
  (:require [graphql-qb.util :as util]))

(defn create
  "Creates a context map from a repository and collection of datasets"
  [repo datasets]
  (let [uri->dataset (util/keyed-by :uri datasets)]
    {:repo repo :uri->dataset uri->dataset}))

(defn get-dataset
  "Fetches a dataset from the context by its URI. Returns nil if the dataset was not found."
  [context dataset-uri]
  (get-in context [:uri->dataset dataset-uri]))

(defn get-repository
  "Gets the SPARQL repository from the context"
  [context]
  (:repo context))
