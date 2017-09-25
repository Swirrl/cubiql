(ns graphql-qb.data
  (:require
   [clojure.string :as string]
   [grafter.rdf.repository :as repo]
   [grafter.rdf.formats :as formats]
   [grafter.rdf.io :as gio]
   [grafter.rdf :refer [add]]))

(defn get-test-repo []
  (repo/resource-repo
   "earnings.nt"
   "earnings_metadata.nt"
   "dimension_pos.nt"
   "member_labels.nt"
   "measure_properties.nt"
   "healthy_life_expectancy.nt"
   "healthy_life_expectancy_metadata.nt"
   "resources/geo-labels.nt"))

(defn get-scotland-repo []
  (repo/sparql-repo "https://production-drafter-sg.publishmydata.com/v1/sparql/live"))

(defn fetch-geo-labels
  "Fetches the labels for all areas found across all observations and
  saves them into a resource file."
  []
  (let [live-repo (get-scotland-repo)
        test-repo (get-test-repo)
        q (str
           "SELECT DISTINCT ?area WHERE {"
           "  ?obs <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/linked-data/cube#Observation> ."
           "  ?obs <http://purl.org/linked-data/sdmx/2009/dimension#refArea> ?area ."
           "}")
        geo-uris (map :area (repo/query test-repo q))
        values (string/join " " (map (fn [uri] (str "<" uri ">")) geo-uris))
        label-q (str
                 "CONSTRUCT { ?geo <http://www.w3.org/2000/01/rdf-schema#label> ?label } WHERE {"
                 "  VALUES ?geo { " values " }"
                 "  ?geo <http://www.w3.org/2000/01/rdf-schema#label> ?label ."
                 "}")
        quads (repo/query live-repo label-q)]
    (add (gio/rdf-serializer "resources/geo-labels.nt" :format formats/rdf-ntriples) quads)))
