(ns graphql-qb.config
  (:require [graphql-qb.vocabulary :refer :all]
            [aero.core :as aero]
            [clojure.java.io :as io])
  (:import [java.net URI]))

(defn read-config
  ([] (read-config (io/resource "config.edn")))
  ([source]
   (aero/read-config source)))

(defn geo-dimension [{config-geo :geo-dimension-uri :as config}]
  ;;Return the default sdmx:refArea if :geo-dimension is not defined at configuration
  (if (nil? config-geo)
    sdmx:refArea
    (URI. config-geo)))

(defn time-dimension [{config-time :time-dimension-uri :as config}]
  ;;Return the default sdmx:refPeriod if :time-dimension is not defined at configuration
  (if (nil? config-time)
    sdmx:refPeriod
    (URI. config-time)))

;;accepted values: [dimension component]
(defn codelist-source [{config-codelist :codelist-source :as config}]
  ;;Return the default "?dim" if :codelist-source is not defined at configuration
  (case config-codelist
    "dimension" "?dim"
    "component" "?comp"
    "?dim"))

(defn codelist-label [{config-cl-label :codelist-label-uri :as config}]
  ;;Return the default skos:prefLabel if :codelist-label-uri is not defined at configuration
  (if (nil? config-cl-label)
    skos:prefLabel
    (URI. config-cl-label)))

(defn dataset-label [{config-dataset-label :dataset-label-uri :as config}]
  ;;Return the default rdfs:label if :dataset-label-uri is not defined at configuration
  (if (nil? config-dataset-label)
    rdfs:label
    (URI. config-dataset-label)))

(defn schema-label-language [config]
  (:schema-label-language config))

(defn ignored-codelist-dimensions
  "Returns a collection of URIs for dimensions which should not have an associated codelist."
  [config]
  (remove nil? [(geo-dimension config)
                (time-dimension config)]))