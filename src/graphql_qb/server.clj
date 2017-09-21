(ns graphql-qb.server
  (:require [graphql-qb.core :as core]
            [graphql-qb.data :as data]
            [graphql-qb.util :as util]
            [com.walmartlabs.lacinia.pedestal :as lp]
            [io.pedestal.http :as server]))

(defn create-server [port]
  (let [repo (data/get-test-repo)
        {:keys [schema datasets]} (core/build-schema-context repo)
        uri->dataset (util/keyed-by :uri datasets)
        opts {:app-context {:repo repo :uri->dataset uri->dataset}
              :port port
              :graphiql true}]
    (lp/pedestal-service schema opts)))

(defn start-server [port]
  (server/start (create-server port)))
