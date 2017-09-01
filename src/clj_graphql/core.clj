(ns clj-graphql.core
  (:require [grafter.rdf.repository :as repo]
            [clojure.string :as string]
            [com.walmartlabs.lacinia.schema :as schema]
            [com.walmartlabs.lacinia.util :refer [attach-resolvers]]
            [com.walmartlabs.lacinia :refer [execute]]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.data.json :as json])
  (:import [java.net URI]
           [java.io PushbackReader]))

(defn get-dimensions-query []
  (str
   "PREFIX qb: <http://purl.org/linked-data/cube#>"
   "SELECT ?dim ?order ?dimlabel ?doc WHERE {"
       "  ?ds qb:structure ?struct ."
       "  ?struct a qb:DataStructureDefinition ."
       "  ?struct qb:component ?comp ."
       "  ?comp a qb:ComponentSpecification ."
       "  ?comp qb:order ?order ."
       "  ?comp qb:dimension ?dim ."
       "  ?dim rdfs:label ?dimlabel ."
       "  OPTIONAL { ?dim rdfs:comment ?doc }"
       "}"))

(defn get-enum-values-query [dim-uri]
  (str
   "PREFIX qb: <http://purl.org/linked-data/cube#>"
   "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"   
   "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
   "SELECT ?member ?label WHERE {"
   "  ?comp qb:dimension <" dim-uri "> ."
   "  ?comp qb:codeList ?list ."
   #_"  ?list a skos:Collection ."
   "  ?list skos:member ?member ."
   "  ?member rdfs:label ?label ."
   "}"))

(defn parse-year [year-str]
  (let [year (Integer/parseInt year-str)]
    (URI. (str "http://reference.data.gov.uk/id/year/" year))))

(defn uri->last-path-segment [uri])

(defn dim-label->field-name [label]
  (keyword (string/join "_" (map string/lower-case (string/split (str label) #"\s+")))))

(defn has-valid-name-first-char? [name]
  (boolean (re-find #"^[_a-zA-Z]" name)))

(defn enum-label->value-name [label]
  (let [name (string/join "_" (map string/upper-case (string/split (str label) #"\s+")))
        valid-name (if (has-valid-name-first-char? name) name (str "a_" name))]
    (keyword valid-name)))

(defn enum-label->type-name [label]
  (keyword (str (name (dim-label->field-name label)) "_type")))

(defn get-enum-values [repo dim-uri]
  (let [results (repo/query repo (get-enum-values-query dim-uri))]
    (map (fn [{:keys [label] :as m}]
           (assoc m :value (enum-label->value-name label)))
         results)))

(defn get-dimension-type [repo {:keys [dim dimlabel]}]
  (cond
    (= (URI. "http://purl.org/linked-data/sdmx/2009/dimension#refArea") dim)
    {:type :ref_area
     :kind :scalar
     :parse (schema/as-conformer #(URI. (str "http://statistics.gov.scot/id/statistical-geography/" %)))
     :serialize (schema/as-conformer uri->last-path-segment)}
    
    (= (URI. "http://purl.org/linked-data/sdmx/2009/dimension#refPeriod") dim)
    {:type :year
     :kind :scalar
     :parse (schema/as-conformer parse-year)
     :serialize (schema/as-conformer uri->last-path-segment)}
    
    :else
    {:kind :enum
     :type (enum-label->type-name dimlabel)
     :values (get-enum-values repo dim)}))

(defn get-test-repo []
  (repo/fixture-repo "earnings.nt" "earnings_metadata.nt" "dimension_pos.nt" "member_labels.nt"))

(defn get-dimensions [repo]
  (let [base-dims (repo/query repo (get-dimensions-query))]
    (map (fn [{:keys [dim] :as m}]
           (merge m (get-dimension-type repo m)))
         base-dims)))

(defn dimensions->obs-dims-schema [dims]
  (let [fields (map (fn [{:keys [dimlabel type]}]
                      [(dim-label->field-name dimlabel) {:type type}])
                    dims)]
    {:fields (into {} fields)}))

(defn dim-has-kind? [kind dim]
  (= kind (:kind dim)))

(defn is-enum? [dim]
  (dim-has-kind? :enum dim))

(defn is-scalar? [dim]
  (dim-has-kind? :scalar dim))

(defn dimensions->enums-schema [dims]
  (let [enum-dims (filter is-enum? dims)
        enum-defs (map (fn [{:keys [kind values type dimlabel] :as d}]
                         [type {:values (mapv :value values)
                                :description (str dimlabel)}])
                       enum-dims)]
    (into {} enum-defs)))

(defn dimensions->scalars-schema [dims]
  (let [scalar-dims (filter is-scalar? dims)]
    (into {} (map (fn [{:keys [type parse serialize]}]
                    [type {:parse parse :serialize serialize}])
                  scalar-dims))))

(defn read-schema-resource [resource-name]
  (if-let [r (io/resource resource-name)]
    (let [pbr (PushbackReader. (io/reader r))]
      (edn/read pbr))
    (throw (IllegalArgumentException. "Resource not found"))))

(defn resolve-dataset-dims [{:keys [repo] :as context} args field]
  (let [dims (get-dimensions repo)]
    (map (fn [{:keys [dim values]}]
           {:uri (str dim)
            :values (map (fn [{:keys [member label]}]
                           {:uri (str member)
                            :label label})
                         values)})
         dims)))

(def dataset-query
  (str
   "SELECT ?uri ?title ?description WHERE {"
   "  ?ds a <http://publishmydata.com/def/dataset#Dataset> ."
   "  ?ds <http://purl.org/dc/terms/title> ?title ."
   "  ?ds <http://www.w3.org/2000/01/rdf-schema#comment> ?description ."
   "  BIND(?ds as ?uri)"
   "}"))

(defn get-dataset [repo]
  (first (repo/query repo dataset-query)))

(defn resolve-dataset [{:keys [repo] :as context} args field]
  (let [ds (get-dataset repo)
        dims (resolve-dataset-dims context args field)]
    (assoc ds :dimensions dims)))

(defn resolve-observations [{:keys [repo] :as context} args {:keys [uri] :as ds-field}]
  (println "args: " args)
  (println "ds-field: " ds-field)
  {:observations
   {:matches []
    :free_dimensions []}})

(defn get-schema [repo]
  (let [dims (get-dimensions repo)
        obs-dims-schema (dimensions->obs-dims-schema dims)
        enums-schema (dimensions->enums-schema dims)
        dim-scalars-schema (dimensions->scalars-schema dims)
        scalars-schema (assoc dim-scalars-schema :uri {:parse (schema/as-conformer #(URI. %))
                                                       :serialize (schema/as-conformer str)})
        base-schema (read-schema-resource "base-schema.edn")]
    (-> base-schema
        (assoc :enums enums-schema)
        (assoc-in [:input-objects :obs_dims] obs-dims-schema)
        (attach-resolvers {:resolve-dataset resolve-dataset
                           :resolve-observations resolve-observations})
        (assoc :scalars scalars-schema))))

(defn get-compiled-schema [repo]
  (schema/compile (get-schema repo)))

(defn load-dim-schema []
  (-> (read-schema-resource "dim-schema.edn")
      (attach-resolvers {:resolve-dataset resolve-dataset
                         :resolve-observations resolve-observations})
      (assoc :scalars {:uri
                       {:parse (schema/as-conformer #(URI. %))
                        :serialize (schema/as-conformer str)}

                       :ref_area
                       {:parse (schema/as-conformer #(URI. (str "http://statistics.gov.scot/id/statistical-geography/" %)))
                        :serialize (schema/as-conformer uri->last-path-segment)}

                       :year
                       {:parse (schema/as-conformer parse-year)
                        :serialize (schema/as-conformer uri->last-path-segment)}})))

(defn compile-dim-schema []
  (schema/compile (load-dim-schema)))

