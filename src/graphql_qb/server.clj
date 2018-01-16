(ns graphql-qb.server
  (:require [graphql-qb.core :as core]
            [graphql-qb.data :as data]
            [graphql-qb.context :as context]
            [com.walmartlabs.lacinia.pedestal :as lp]
            [io.pedestal.http :as http]))

(def cors-config {:allowed-origins (constantly true)
                  :creds           false
                  :max-age         (* 60 60 2)                          ;2 hours
                  :methods         "GET, POST, OPTIONS"})

(defn create-server
  ([port] (create-server port (data/get-test-repo)))
  ([port repo]
   (let [{:keys [schema datasets]} (core/build-schema-context repo)
         context (context/create repo datasets)
         opts {:app-context context
               :port        port
               :graphiql    true}
         service-map (lp/service-map schema opts)]
     (-> service-map
         (assoc ::http/allowed-origins cors-config)
         (http/create-server)))))

(defn start-server [port repo]
  (http/start (create-server port repo)))
