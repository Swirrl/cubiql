(ns clj-graphql.server
  (:require [clj-graphql.core :as core]
            [com.walmartlabs.lacinia.pedestal :as lp]
            [io.pedestal.http :as server]))

(defn create-server [port]
  (let [repo (core/get-test-repo)
        schema (core/get-compiled-schema repo)
        ds-dimensions (core/get-dimensions repo)
        opts {:app-context {:repo repo :dimensions ds-dimensions}
              :port port
              :graphiql true}]
    (lp/pedestal-service schema opts)))

(defn start-server [port]
  (server/start (create-server port)))

