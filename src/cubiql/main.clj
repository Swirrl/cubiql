(ns cubiql.main
  (:require [cubiql.server :as server]
            [clojure.tools.cli :as cli]
            [cubiql.data :as data]
            [cubiql.config :as config]
            [clojure.java.io :as io])
  (:gen-class)
  (:import [java.io File]))

(def cli-options
  [["-p" "--port PORT" "Port number to start the server on"
    :default 8080
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 % 65536) "Port number must be in the range (0, 65536)"]]

   ["-e" "--endpoint ENDPOINT" "Uri of the SPARQL query endpoint to search for datasets. Uses the test repository if not specified"
    :parse-fn data/parse-endpoint]

   ["-c" "--configuration CONFIGURATION" "File containing data cube configuration"
    :parse-fn io/file
    :validate [(fn [^File f] (.exists f)) "Configuration file not found"]]])

(defn print-usage [arg-summary]
  (println "Usage: cubiql OPTIONS")
  (println "The following options are available:")
  (println)
  (println arg-summary))

(defn print-errors-and-usage [errors arg-summary]
  (binding [*out* *err*]
    (doseq [err errors]
      (println err))
    (println)
    (print-usage arg-summary)))

(defn parse-arguments [args]
  (let [{:keys [options] :as result} (cli/parse-opts args cli-options)
        endpoint (:endpoint options)]
    (if (nil? endpoint)
      (update result :errors conj "Endpoint required")
      result)))

(defn -main
  [& args]
  (let [{:keys [options summary errors]} (parse-arguments args)]
    (if (some? errors)
      (do
        (print-errors-and-usage errors summary)
        (System/exit 1))
      (let [{:keys [port endpoint configuration]} options
            config (if (some? configuration)
                     (config/read-config configuration)
                     (config/read-config))]
        (server/start-server port endpoint config)
        (println "Started server on port " port)))))
