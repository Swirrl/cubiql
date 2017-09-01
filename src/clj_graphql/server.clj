(ns clj-graphql.server
  (:require [clj-graphql.core :as core]
            [com.walmartlabs.lacinia.pedestal :as lp]
            [io.pedestal.http :as server]))

(defn create-server [port]
  (let [repo (core/get-test-repo)
        schema (core/get-compiled-schema repo)
        uri-mapping (core/get-value->uri-mapping repo)
        opts {:app-context {:repo repo :value->uri-mapping uri-mapping}
              :port port
              :graphiql true}]
    (lp/pedestal-service schema opts)))

(defn start-server [port]
  (server/start (create-server port)))

