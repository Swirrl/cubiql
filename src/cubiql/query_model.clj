(ns cubiql.query-model
  "Represents a simplified model of a SPARQL query. Within the model, items at specified 'paths' are defined which
  can be constrained to a specific value or a collection of associated predicates. The path is a sequence of [key predicate]
  pairs where the key identifies the binding in the resulting SPARQL query and the predicate defines the predicate URI
  for the RDF relation."
  (:require [clojure.string :as string]
            [cubiql.vocabulary :refer :all]
            [cubiql.vocabulary :refer [time:DateTime]])
  (:import [java.net URI]
           [java.util Date]
           [java.text SimpleDateFormat]
           [java.time.temporal TemporalAccessor]
           [java.time.format DateTimeFormatter]))

(defprotocol QueryItem
  "Protocol representing items that have a specific representation within a SPARQL query."
  (query-format [this]
    "Returns the string representation of this value within a SPARQL query"))

(defrecord QueryVar [name]
  QueryItem
  (query-format [_this] (str "?" name)))

(extend-protocol QueryItem
  String
  (query-format [s] (str "\"" s "\""))

  URI
  (query-format [uri] (str "<" uri ">"))

  TemporalAccessor
  (query-format [dt]
    (str "\"" (.format DateTimeFormatter/ISO_OFFSET_DATE_TIME dt) "\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"))

  Date
  (query-format [date]
    ;;TODO: fix date format to correctly include time zone
    ;;Stardog treats a time zone offset of 0000 different from Z so just hardcode to Z for now
    (let [fmt (SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss")]
      (str "\"" (.format fmt date) "Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"))))

(def empty-model {:bindings {}
                  :filters {}
                  :order-by []})

(defn- update-binding-value [old-value new-value]
  (cond
    (= ::var old-value) new-value
    (= ::var new-value) old-value
    :else (throw (Exception. "Cannot bind multiple values to the same path"))))

(defn- update-binding-spec [old-spec new-spec]
  (-> old-spec
      (update ::match update-binding-value (::match new-spec))
      (update ::optional? #(and %1 %2) (::optional? new-spec))))

(defn- format-path [path]
  (string/join " -> " (map (fn [[key uri]] (str "[" key ", " uri "]")) path)))

(defn- format-key-path [key-path]
  (string/join " -> " (map name key-path)))

(defn- update-binding [binding path-prefix path binding-spec]
  (let [[[key uri :as path-item] & ps] path]
    (if-let [[existing-uri spec] (get binding key)]
      (if (= uri existing-uri)
        (if (seq? ps)
          (let [updated (update-binding spec (conj path-prefix path-item) ps binding-spec)]
            (assoc binding key [uri updated]))
          ;;update existing match spec
          (assoc binding key [uri (update-binding-spec spec binding-spec)]))
        (throw (IllegalArgumentException. (str "Mismatched URIs for key path " (format-path (conj path-prefix path-item)) " - existing = " existing-uri " attempted = " uri))))
      (let [inner (if (seq? ps)
                    (update-binding {} (conj path-prefix path-item) ps binding-spec)
                    binding-spec)]
        (assoc binding key [uri inner])))))

(defn add-binding
  "Adds a binding for the specified path to the given model. Path should be a sequence of [key predicate] pairs
   where predicate is the predicate URI for the corresponding triple pattern. Value can be either an object
   implementing the QueryItem protocol or the special value ::var indicating a variable. "
  [model path value & {:keys [optional?]
                       :or {optional? false}}]
  (if (= 0 (count path))
    model
    (update model :bindings (fn [b] (update-binding b [] path {::match value ::optional? optional?})))))

(defn- key-path->spec-path [key-path]
  (mapcat (fn [k] [k 1]) key-path))

(defn- get-spec-by-key-path [{:keys [bindings] :as qm} key-path]
  (get-in bindings (key-path->spec-path key-path)))

(defn- key-path-valid? [{:keys [bindings] :as model} key-path]
  (and (seq key-path)
       (let [spec (get-spec-by-key-path model key-path)]
         (and (map? spec) (contains? spec ::match)))))

(defn add-filter
  "Adds a filter to a path in the given query. A binding for the key path should already exist."
  [model key-path filter]
  (if (key-path-valid? model key-path)
    (update model :filters (fn [fm] (update-in fm key-path (fnil conj []) filter)))
    (throw (IllegalArgumentException. (str "No existing binding for key path " (format-key-path key-path))))))

(defn add-order-by
  "Adds an ORDER BY clause to this query."
  [model order-by]
  (update model :order-by conj order-by))

(defn key-path->var-name
  "Converts a binding key path into the corresponding SPARQL variable."
  [key-path]
  (string/join "" (map name key-path)))

(defn key-path->var-key
  "Converts a binding key path into the corresponding SPARQL variable key."
  [key-path]
  (keyword (key-path->var-name key-path)))

(defn- key-path->query-var [key-path]
  (->QueryVar (key-path->var-name key-path)))

(defn- is-literal? [l]
  (and (some? l) (not= ::var l)))

(defn- get-query-var-bindings [{:keys [bindings] :as model}]
  (->> bindings
       (filter (fn [[k [pred m]]] (is-literal? (::match m))))
       (map (fn [[k [pred m]]]
              [(::match m) k]))))

(defn- format-query-var-binding [[uri var-kw]]
  (let [query-var (->QueryVar (name var-kw))]
    (str "BIND(" (query-format uri) " AS " (query-format query-var) ")")))

(defn- get-query-order-by [{:keys [order-by]}]
  (map (fn [ordering]
         (let [dir (if (contains? ordering :ASC) :ASC :DESC)
               key-path (or (:ASC ordering) (:DESC ordering))]
           [dir (key-path->query-var key-path)]))
       order-by))

(defn- format-ordering [[dir query-var]]
  (if (= :ASC dir)
    (query-format query-var)
    (str "DESC(" (query-format query-var) ")")))

(defn- format-query-order-by [orderings]
  (if (empty? orderings)
    ""
    (str "ORDER BY " (string/join " " (map format-ordering orderings)))))

(defn- get-path-bgps [key-path parent [predicate binding-spec]]
  (let [match (::match binding-spec)
        optional? (::optional? binding-spec)
        nested (dissoc binding-spec ::match ::optional?)
        path-var-name (key-path->var-name key-path)
        object (if (is-literal? match) match (->QueryVar path-var-name))
        child-bgps (mapcat (fn [[key binding-spec]]
                             (get-path-bgps (conj key-path key) object binding-spec))
                           nested)]
    (conj child-bgps {::s parent ::p predicate ::o object ::optional? optional?})))

(defn- get-query-triple-patterns [{:keys [bindings] :as model} obs-var-name]
  (mapcat (fn [[key spec-pair]]
            (get-path-bgps [key] (->QueryVar obs-var-name) spec-pair))
          bindings))

(defn- format-query-triple-pattern [#::{:keys [s p o optional?]}]
  (let [tp (str (query-format s) " " (query-format p) " " (query-format o) " .")]
    (if optional?
      (str "OPTIONAL { " tp " }")
      tp)))

(defn- get-filters [path-prefix k v]
  (if (map? v)
    (mapcat (fn [[ck cv]]
              (get-filters (conj path-prefix k) ck cv))
            v)
    (let [query-var (key-path->query-var (conj path-prefix k))]
      (map (fn [f] [query-var f]) v))))

(defn- get-query-filters [{:keys [filters] :as model}]
  (mapcat (fn [[k v]] (get-filters [] k v)) filters))

(defn- format-filter [[query-var [fun value]]]
  (format "FILTER(%s %s %s)" (query-format query-var) fun (query-format value)))

(defn get-query
  "Returns the observation SPARQL query for the given query model."
  [model obs-var-name dataset-uri]
  (let [var-bindings (get-query-var-bindings model)
        binding-clauses (string/join " " (map format-query-var-binding var-bindings))
        obs-var (->QueryVar obs-var-name)]
    (str
      "PREFIX qb: <http://purl.org/linked-data/cube#>"
      "SELECT * WHERE {"
      binding-clauses
      "  " (query-format obs-var) "  a qb:Observation ."
      "  " (query-format obs-var) "  qb:dataSet " (query-format dataset-uri) " ."
      (string/join " " (map format-query-triple-pattern (get-query-triple-patterns model obs-var-name)))
      (string/join " " (map format-filter (get-query-filters model)))
      "}" (format-query-order-by (get-query-order-by model)))))

(defn get-observation-count-query
  "Returns the SPARQL query for finding the number of matching observations for the given query model."
  [model obs-var-name dataset-uri]
  (let [obs-var (->QueryVar obs-var-name)]
    (str
      "PREFIX qb: <http://purl.org/linked-data/cube#>"
      "SELECT (COUNT(*) AS ?c) WHERE {"
      "  " (query-format obs-var) " a qb:Observation ."
      "  " (query-format obs-var) " qb:dataSet " (query-format dataset-uri) " ."
      (string/join " " (map format-query-triple-pattern (get-query-triple-patterns model obs-var-name)))
      (string/join " " (map format-filter (get-query-filters model)))
      "}")))

(defn get-observation-aggregation-query
  "Returns the SPARQL query for aggregating the specified measure for an observation query model."
  [model aggregation-fn dataset-uri measure-uri]
  (let [measure-var-name "mv"
        obs-var-name "obs"
        sparql-fn (string/upper-case (name aggregation-fn))]
    (str
      "PREFIX qb: <http://purl.org/linked-data/cube#>"
      "SELECT (" sparql-fn "(?mv) AS ?" (name aggregation-fn) ") WHERE {"
      "  ?obs a qb:Observation ."
      "  ?obs qb:dataSet " (query-format dataset-uri) " ."
      (string/join " " (map format-query-triple-pattern (get-query-triple-patterns model obs-var-name)))
      (string/join " " (map format-filter (get-query-filters model)))
      "  ?obs " (query-format measure-uri) " ?" measure-var-name " ."
      "}")))

(defn get-path-binding-value
  "Returns the value bound at the given key path."
  [model key-path]
  (::match (get-spec-by-key-path model key-path)))

(defn is-path-binding-optional?
  "Returns whether the binding at the given key paths is OPTIONAL."
  [model key-path]
  (::optional? (get-spec-by-key-path model key-path)))

(defn get-path-filters
  "Returns all the filters associated with the binding at the given key path."
  [{:keys [filters] :as qm} key-path]
  (get-in filters key-path))

(defn get-order-by
  "Returns the ORDER BY clauses for the given query model."
  [qm]
  (:order-by qm))
