(ns graphql-qb.util
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [grafter.rdf.repository :as repo])
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

(defn map-values [f m]
  (into {} (map (fn [[k v]] [k (f v)]) m)))

(defn map-keys [f m]
  (into {} (map (fn [[k v]] [(f k) v]) m)))

(defn distinct-by
  "Returns a sequence containing distinct elements by the given key function."
  [f s]
  (let [keys (atom #{})]
    (filter (fn [v]
              (let [k (f v)]
                (if (contains? @keys k)
                  false
                  (do
                    (swap! keys conj k)
                    true))))
            s)))

(defn long->bytes [i]
  {:post [(= 8 (alength %))]}
  (.. (ByteBuffer/allocate 8) (putLong i) (array)))

(defn bytes->long [bytes]
  {:pre [(= 8 (alength bytes))]}
  (.. (ByteBuffer/wrap bytes) (getLong)))

(defn eager-query
  "Executes a SPARQL query against the given repository and eagerly evaluates the results. This prevents
   connections being left open by lazy sequence operators."
  [repo sparql-string]
  (with-open [conn (repo/->connection repo)]
    (doall (repo/query conn sparql-string))))

(defn find-first
  "Returns the first item in s which satisfies the predicate p. Returns nil if no items satisfy p."
  [p s]
  (first (filter p s)))

(defn label->string [l]
  (some-> l str))

(defn convert-binding-labels
  "Returns a function which converts each label associated with the specified keys to a string."
  [keys]
  (fn [bindings]
    (reduce (fn [acc k]
              (update acc k label->string))
            bindings
            keys)))