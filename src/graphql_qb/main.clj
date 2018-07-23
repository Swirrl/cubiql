(ns graphql-qb.main
  (:require [graphql-qb.server :as server]
            [clojure.tools.cli :as cli]
            [graphql-qb.data :as data]
            [grafter.rdf.repository :as repo]
            [graphql-qb.config :as config]
            [clojure.java.io :as io])
  (:gen-class)
  (:import [java.net URI]
           (java.io File)))

(def cli-options
  [["-p" "--port PORT" "Port number to start the server on"
    :default 8080
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 % 65536) "Port number must be in the range (0, 65536)"]]

   ["-e" "--endpoint ENDPOINT" "Uri of the SPARQL query endpoint to search for datasets. Uses the test repository if not specified"
    :parse-fn #(URI. %)]

   ["-c" "--configuration CONFIGURATION" "File containing data cube configuration"
    :parse-fn io/file
    :validate [(fn [^File f] (.exists f)) "Configuration file not found"]]])

(defn print-usage [arg-summary]
  (println "Usage: graphql-qb OPTIONS")
  (println "The following options are available:")
  (println)
  (println arg-summary))

(defn print-errors-and-usage [errors arg-summary]
  (binding [*out* *err*]
    (doseq [err errors]
      (println err))
    (println)
    (print-usage arg-summary)))

(defn get-repo [uri-or-nil]
  (if (nil? uri-or-nil)
    (data/get-test-repo)
    (repo/sparql-repo (str uri-or-nil))))

(defn -main
  [& args]
  (let [{:keys [options summary errors]} (cli/parse-opts args cli-options)]
    (if (some? errors)
      (do
        (print-errors-and-usage errors summary)
        (System/exit 1))
      (let [{:keys [port endpoint configuration]} options
            repo (get-repo endpoint)
            config (if (some? configuration)
                     (config/read-config configuration)
                     (config/read-config))]
        (server/start-server port repo config)
        (println "Started server on port " port)))))
