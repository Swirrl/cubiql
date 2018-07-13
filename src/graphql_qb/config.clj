(ns graphql-qb.config
  (:require [graphql-qb.vocabulary :refer :all] 
            [aero.core :as aero])
  (:import [java.net URI]))

(defn read-config []
  (aero/read-config "resources/config.edn"))

(defn geo-dimension [config]
  (let [config-geo (get-in config [:geo-dimension-uri])]    
    ;;Return the default sdmx:refArea if :geo-dimension is not defined at configuration
    (if (nil? config-geo)
      sdmx:refArea
      (URI. config-geo))))

(defn time-dimension [config]
  (let [config-time (get-in config [:time-dimension-uri])]    
    ;;Return the default sdmx:refPeriod if :time-dimension is not defined at configuration
    (if (nil? config-time)
      sdmx:refPeriod
      (URI. config-time))))

;;accepted values: [dimension component]
(defn codelist-source [config]
  (let [config-codelist (get-in config [:codelist-source])]    
    ;;Return the default "?dim" if :codelist-source is not defined at configuration
    (case config-codelist
      "dimension" "?dim"
      "component" "?comp"
      "?dim")))

(defn codelist-label [config]
  (let [config-cl-label (get-in config [:codelist-label-uri])]    
    ;;Return the default skos:prefLabel if :codelist-label-uri is not defined at configuration
    (if (nil? config-cl-label)
      skos:prefLabel
      (URI. config-cl-label))))

(defn dataset-label [config]
  (let [config-dataset-label (get-in config [:dataset-label-uri])]    
    ;;Return the default rdfs:label if :dataset-label-uri is not defined at configuration
    (if (nil? config-dataset-label)
      rdfs:label
      (URI. config-dataset-label))))