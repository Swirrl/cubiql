(ns graphql-qb.config
  (:require [graphql-qb.vocabulary :refer :all] 
            [aero.core :as aero])
  (:import [java.net URI]))

(defn read-config []
  (aero/read-config "resources/config.edn"))

(defn geo-dimension [config]
  (let [config-geo (get-in config [:geo-dimension])]    
    ;;Return the default sdmx:refArea if :geo-dimension is not defined at configuration
    (if (nil? config-geo)
      sdmx:refArea
      (URI. config-geo))))

(defn geo-dimension [config]
  (let [config-time (get-in config [:time-dimension])]    
    ;;Return the default sdmx:refPeriod if :time-dimension is not defined at configuration
    (if (nil? config-time)
      sdmx:refPeriod
      (URI. config-time))))

;;accepted values: [dimension component]
(defn codelist-source [config]
  (let [config-codelist (get-in config [:codelist-source])]    
    ;;Return the default "dimension" if :codelist-source is not defined at configuration
    (case config-codelist
      "dimension" "dimension"
      "component" "component"
      "dimension")))