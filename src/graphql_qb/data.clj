(ns graphql-qb.data
  (:require
   [clojure.java.io :as io]
   [grafter.rdf.repository :as repo]))

(defn get-test-repo []
  (repo/resource-repo
   "earnings.nt"
   "earnings_metadata.nt"
   "dimension_pos.nt"
   "member_labels.nt"
   "measure_properties.nt"
   "healthy_life_expectancy.nt"
   "healthy_life_expectancy_metadata.nt"))
