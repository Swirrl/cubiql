(ns clj-graphql.core
  (:require [grafter.rdf.repository :as repo]
            [clojure.string :as string]
            [com.walmartlabs.lacinia.schema :as schema]
            [com.walmartlabs.lacinia.util :refer [attach-resolvers]]
            [com.walmartlabs.lacinia :refer [execute]]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.data.json :as json]
            [clojure.set :as set])
  (:import [java.net URI]
           [java.io PushbackReader]))

(defn get-dimensions-query []
  (str
   "PREFIX qb: <http://purl.org/linked-data/cube#>"
   "SELECT ?dim ?order ?label ?doc WHERE {"
       "  ?ds qb:structure ?struct ."
       "  ?struct a qb:DataStructureDefinition ."
       "  ?struct qb:component ?comp ."
       "  ?comp a qb:ComponentSpecification ."
       "  ?comp qb:order ?order ."
       "  ?comp qb:dimension ?dim ."
       "  ?dim rdfs:label ?label ."
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

(defn uri->last-path-segment [uri]
  (last (string/split (.getPath uri) #"/")))

(defn dim-label->field-name [label]
  (keyword (string/join "_" (map string/lower-case (string/split (str label) #"\s+")))))

(defn dimension->field-name [{:keys [label]}]
  (dim-label->field-name label))

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

(defn get-dimension-type [repo {:keys [dim label]}]
  (cond
    (= (URI. "http://purl.org/linked-data/sdmx/2009/dimension#refArea") dim)
    {:type :ref_area
     :kind :scalar
     :parse (schema/as-conformer #(URI. (str "http://statistics.gov.scot/id/statistical-geography/" %)))
     :serialize (schema/as-conformer uri->last-path-segment)
     :value->dimension-uri identity
     :dimension-uri->value identity}
    
    (= (URI. "http://purl.org/linked-data/sdmx/2009/dimension#refPeriod") dim)
    {:type :year
     :kind :scalar
     :parse (schema/as-conformer parse-year)
     :serialize (schema/as-conformer uri->last-path-segment)
     :value->dimension-uri identity
     :dimension-uri->value identity}
    
    :else
    (let [values (get-enum-values repo dim)
          value->uri (into {} (map (juxt :value :member) values))]
      {:kind :enum
       :type (enum-label->type-name label)
       :values (get-enum-values repo dim)
       :value->dimension-uri value->uri
       :dimension-uri->value (set/map-invert value->uri)})))

(defn get-test-repo []
  (repo/fixture-repo "earnings.nt" "earnings_metadata.nt" "dimension_pos.nt" "member_labels.nt" "measure_properties.nt"))

(defn get-dimensions [repo]
  (let [base-dims (repo/query repo (get-dimensions-query))]
    (map (fn [{:keys [dim] :as m}]
           (merge m (get-dimension-type repo m)))
         base-dims)))

(defn get-measure-types-query []
  (str
   "PREFIX qb: <http://purl.org/linked-data/cube#>"
   "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
   "SELECT ?mt ?label WHERE {"
   "  ?ds qb:structure ?struct ."
   "  ?struct qb:component ?comp ."
   "  ?comp qb:measure ?mt ."
   "  ?mt a qb:MeasureProperty ."
   "  ?mt rdfs:label ?label ."
   "}"))

(defn rename-key [m k new-k]
  (let [v (get m k)]
    (-> m
        (assoc new-k v)
        (dissoc k))))

(defn get-measure-types [repo]
  (let [q (get-measure-types-query)
        results (repo/query repo q)]
    (map-indexed (fn [idx bindings]
                   (-> bindings
                       (rename-key :mt :uri)
                       (assoc :order (inc idx))))  results)))

(defn get-measure-type-schemas [repo]
  (let [measure-types (get-measure-types repo)]
    (map (fn [{:keys [label] :as mt}]
           (let [field-name (dim-label->field-name label)]
             {field-name {:type 'String}})) measure-types)))

(defn dimensions->obs-dims-schema [dims]
  (let [fields (map (fn [{:keys [label type]}]
                      [(dim-label->field-name label) {:type type}])
                    dims)]
    {:fields (into {} fields)}))

(defn dimensions->obs-dim-schemas [dims]
  (map (fn [{:keys [label type]}]
                      [(dim-label->field-name label) {:type type}])
                    dims))

(defn dim-has-kind? [kind dim]
  (= kind (:kind dim)))

(defn is-enum? [dim]
  (dim-has-kind? :enum dim))

(defn is-scalar? [dim]
  (dim-has-kind? :scalar dim))

(defn dimensions->enums-schema [dims]
  (let [enum-dims (filter is-enum? dims)
        enum-defs (map (fn [{:keys [kind values type label] :as d}]
                         [type {:values (mapv :value values)
                                :description (str label)}])
                       enum-dims)]
    (into {} enum-defs)))

(defn dimensions->scalars-schema [dims]
  (let [scalar-dims (filter is-scalar? dims)]
    (into {} (map (fn [{:keys [type parse serialize]}]
                    [type {:parse parse :serialize serialize}])
                  scalar-dims))))

(defn dimension-args->uris [args mapping]
  {:gender {:->uri str :uri (URI. "http://foo.com")}}
  (into {} (map (fn [[dim-key value]]
                  (let [{:keys [->uri uri]} (get mapping dim-key)]
                    [uri (->uri value)]))
                args)))

(defn dimension->query-var-name [{:keys [order]}]
  (str "dim" order))

(defn get-dimension-query-binding [dim bindings-map]
  (get bindings-map (keyword (dimension->query-var-name dim))))

(defn get-dimension-query-value [{:keys [dimension-uri->value] :as dim} bindings-map]
  (let [binding (get-dimension-query-binding dim bindings-map)]
    (dimension-uri->value binding)))

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

(defn measure-type->query-var-name [{:keys [order]}]
  (str "mt" order))

(defn get-observation-query [ds-dimensions query-dimensions measure-types]
  (let [field->ds-dims (into {} (map (fn [dim] [(dimension->field-name dim) dim]) ds-dimensions))
        field->measure-types (into {} (map (fn [{:keys [label] :as dim}] [(dim-label->field-name label) dim]) measure-types))
        constrained-dims (select-keys field->ds-dims (keys query-dimensions))
        free-dims (apply dissoc field->ds-dims (keys query-dimensions))
        constrained-patterns (map (fn [[field-name field-value]]
                                    (let [dim (get field->ds-dims field-name)
                                          val-uri ((:value->dimension-uri dim) field-value)]
                                      (str "?obs <" (str (:dim dim)) "> <" (str val-uri) "> .")))
                                  query-dimensions)
        measure-type-patterns (map (fn [[field-name {:keys [uri] :as mt}]]
                                     (str
                                      "  ?obs qb:measureType <" (str uri) "> . \n" 
                                      "  ?obs <" (str uri) "> ?" (measure-type->query-var-name mt) " ."))                                   
                                   field->measure-types)
        binds (map (fn [[field-name field-value]]
                     (let [dim (get field->ds-dims field-name)
                           val-uri ((:value->dimension-uri dim) field-value)
                           var-name (dimension->query-var-name dim)]
                       (str "BIND(<" val-uri "> as ?" var-name ") .")))
                   query-dimensions)
        query-patterns (map (fn [[field-name dim]]
                              (let [var-name (dimension->query-var-name dim)]
                                (str "?obs <" (:dim dim) "> ?" var-name " .")))
                            free-dims)]
    (str
     "PREFIX qb: <http://purl.org/linked-data/cube#>"
     "SELECT * WHERE {"
     "  ?obs a qb:Observation ."
     "  ?obs qb:dataSet <http://statistics.gov.scot/data/earnings> ."
     "  ?obs qb:measureType ?measureType ."
     "  ?obs ?measureType ?value ."
     (string/join "\n" measure-type-patterns)
     (string/join "\n" constrained-patterns)
     (string/join "\n" query-patterns)
     (string/join "\n" binds)
     "}")))

(defn resolve-observations [{:keys [repo dimensions measure-types] :as context} {query-dimensions :dimensions :as args} {:keys [uri] :as ds-field}]
  (let [query (get-observation-query dimensions query-dimensions measure-types)
        results (repo/query repo query)
        matches (mapv (fn [{:keys [obs] :as bindings}]
                        (let [mapped-field-values (map (fn [dim]
                                                         (let [field-name (dimension->field-name dim)
                                                               value (get-dimension-query-value dim bindings)]
                                                           [field-name value]))
                                                       dimensions)
                              mapped-measure-types (map (fn [{:keys [label] :as mt}]
                                                          (let [field-name (dim-label->field-name label)
                                                                var-name (keyword (measure-type->query-var-name mt))]
                                                            [field-name (some-> (get bindings var-name) str)]))
                                                        measure-types)]
                          (into {:uri obs} (concat mapped-field-values mapped-measure-types))))
                      results)]
    {:matches matches
     :free_dimensions []}))

(defn merge-type-schemas [s1 s2]
  (merge-with merge s1 s2))

(defn get-schema [repo]
  (let [dims (get-dimensions repo)
        obs-dim-schemas (dimensions->obs-dim-schemas dims)
        measure-type-schemas (get-measure-type-schemas repo)
        enums-schema (dimensions->enums-schema dims)
        dim-scalars-schema (dimensions->scalars-schema dims)
        scalars-schema (assoc dim-scalars-schema :uri {:parse (schema/as-conformer #(URI. %))
                                                       :serialize (schema/as-conformer str)})
        base-schema (read-schema-resource "base-schema.edn")]
    (-> base-schema
        (assoc :enums enums-schema)
        (assoc-in [:input-objects :obs_dims] {:fields (into {} obs-dim-schemas)})
        (attach-resolvers {:resolve-dataset resolve-dataset
                           :resolve-observations resolve-observations})
        (assoc :scalars scalars-schema)
        (update-in [:objects :observation :fields] #(into % (concat obs-dim-schemas measure-type-schemas))))))

(defn get-compiled-schema [repo]
  (schema/compile (get-schema repo)))


