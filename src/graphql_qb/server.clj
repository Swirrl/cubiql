(ns graphql-qb.server
  (:require [graphql-qb.core :as core]
            [graphql-qb.context :as context]
            [com.walmartlabs.lacinia.pedestal :as lp]
            [io.pedestal.http :as http]))

(def cors-config {:allowed-origins (constantly true)
                  :creds           false
                  :max-age         (* 60 60 2)                          ;2 hours
                  :methods         "GET, POST, OPTIONS"})

(defn create-server
  ([port repo config]
   (let [{:keys [schema datasets dataset-mappings]} (core/build-schema-context repo config)
         context (context/create repo datasets dataset-mappings config)
         opts {:app-context context
               :port        port
               :graphiql    true}
         service-map (lp/service-map schema opts)]
     (-> service-map
         (assoc ::http/allowed-origins cors-config)
         (http/create-server)))))

(defn start-server [port repo config]
  (http/start (create-server port repo config)))
