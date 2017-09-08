(ns graphql-qb.server
  (:require [graphql-qb.core :as core]
            [com.walmartlabs.lacinia.pedestal :as lp]
            [io.pedestal.http :as server])
  (:import [java.net URI]))

(defn create-server [port]
  (let [repo (core/get-test-repo)
        {:keys [schema ds-uri->dims-measures]} (core/build-schema-context repo)
        opts {:app-context {:repo repo :ds-uri->dims-measures ds-uri->dims-measures}
              :port port
              :graphiql true}]
    (lp/pedestal-service schema opts)))

(defn start-server [port]
  (server/start (create-server port)))

