(ns clj-graphql.util
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn])
  (:import [java.io PushbackReader]))

(defn read-edn
  "Reads EDN from the given source."
  [source]
  (with-open [pbr (PushbackReader. (io/reader source))]
    (edn/read pbr)))

(defn read-edn-resource
  "Loads EDN from the named resource."
  [resource-name]
  (if-let [r (io/resource resource-name)]
    (read-edn r)
    (throw (IllegalArgumentException. "Resource not found"))))

(defn rename-key
  "Renames the key k in the map m to new-k."
  [m k new-k]
  {:pre [(contains? m k)]
   :post [(contains? % new-k)
          (not (contains? % k))]}
  (let [v (get m k)]
    (-> m
        (assoc new-k v)
        (dissoc k))))
