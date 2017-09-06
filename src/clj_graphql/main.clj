(ns clj-graphql.main
  (:require [clj-graphql.server :as server])
  (:gen-class))

(defn get-port
  "Gets the port to host the server on. If args contains a first
  element it should be the port to use. If no port is provided use the
  default port 8080."
  [args]
  (if-let [port-str (first args)]
    (try
      (Integer/parseInt port-str)
      (catch NumberFormatException ex
        (binding [*out* *err*]
          (println "Invalid port number:" port-str)
          (println "Usage: clj-graphql PORT"))
        (System/exit 1)))
    8080))

(defn -main
  [& args]
  (let [port (get-port args)]
    (server/start-server port)
    (println "Started server on port" port)))
