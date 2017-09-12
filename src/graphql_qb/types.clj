(ns graphql-qb.types
  "Functions for mapping DSD elements to/from GraphQL types"
  (:require [clojure.string :as string])
  (:import [java.net URI]))

(defn parse-year [year-str]
  (let [year (Integer/parseInt year-str)]
    (URI. (str "http://reference.data.gov.uk/id/year/" year))))

(defn parse-geography [geo-code]
  (URI. (str "http://statistics.gov.scot/id/statistical-geography/" geo-code)))

(defn uri->last-path-segment [uri]
  (last (string/split (.getPath uri) #"/")))

(def serialise-year uri->last-path-segment)
(def serialise-geography uri->last-path-segment)

(defn dataset-label->schema-name [label]
  (keyword (string/join "_" (cons "dataset" (map string/lower-case (string/split label #"\s+"))))))

(defn label->field-name [label]
  (keyword (string/join "_" (map string/lower-case (string/split (str label) #"\s+")))))

(defn ->field-name [{:keys [label]}]
  (label->field-name label))

(defn has-valid-name-first-char? [name]
  (boolean (re-find #"^[_a-zA-Z]" name)))

(defn enum-label->value-name [label]
  (let [name (string/join "_" (map string/upper-case (string/split (str label) #"\s+")))
        valid-name (if (has-valid-name-first-char? name) name (str "a_" name))]
    (keyword valid-name)))

(defn field-name->type-name [field-name ds-schema]
  (keyword (str (name ds-schema) "_" (name field-name) "_type")))

(defn ->type-name [f ds-schema]
  (field-name->type-name (->field-name f) ds-schema))


