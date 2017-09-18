(ns graphql-qb.util
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn])
  (:import [java.io PushbackReader]
           [java.nio ByteBuffer]))

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

(defn keyed-by [f s]
  (into {} (map (fn [v] [(f v) v]) s)))

(defn long->bytes [i]
  {:post [(= 8 (alength %))]}
  (.. (ByteBuffer/allocate 8) (putLong i) (array)))

(defn bytes->long [bytes]
  {:pre [(= 8 (alength bytes))]}
  (.. (ByteBuffer/wrap bytes) (getLong)))
