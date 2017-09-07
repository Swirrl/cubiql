(ns clj-graphql.server
  (:require [clj-graphql.core :as core]
            [com.walmartlabs.lacinia.pedestal :as lp]
            [io.pedestal.http :as server])
  (:import [java.net URI]))

(defn create-server [port]
  (let [repo (core/get-test-repo)
        schema (core/get-compiled-schema repo)
        ds-dimensions (core/get-dimensions repo (URI. "http://statistics.gov.scot/data/earnings"))
        measure-types (core/get-measure-types repo)
        opts {:app-context {:repo repo :dimensions ds-dimensions :measure-types measure-types}
              :port port
              :graphiql true}]
    (lp/pedestal-service schema opts)))

(defn start-server [port]
  (server/start (create-server port)))

