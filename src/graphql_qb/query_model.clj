(ns graphql-qb.query-model
  (:require [clojure.string :as string]
            [clojure.pprint :as pp]
            [graphql-qb.vocabulary :refer :all])
  (:import [java.net URI]
           (java.util Date)
           (java.text SimpleDateFormat)
           (java.time.temporal TemporalAccessor)
           (java.time.format DateTimeFormatter)))

(defprotocol QueryItem
  (query-format [this]))

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

(def time:DateTime (URI. "http://www.w3.org/2006/time#inXSDDateTime"))
{:bindings {:dim1 [(URI. "http://purl.org/linked-data/sdmx/2009/dimension#refArea") {::match ::var
                                                                                     ::optional? true
                                                                                     :label  {::match ::var}}]
            :dim2 [(URI. "http://refPeriod") {:end   [(URI. "http://www.w3.org/2006/time#hasEnd") {:time [time:DateTime {::match ::var}]}]
                                              :begin [(URI. "http://www.w3.org/2006/time#hasStart") {:time [time:DateTime {::match ::var}]}]}]
            :dim3 [(URI. "http://statistics.gov.scot/def/dimension/gender") (URI. "http://statistics.gov.scot/def/concept/gender/male")]
            :dim4 [(URI. "http://purl.org/linked-data/cube#measureType") {::match ::var}]
            :dim5 [(URI. "http://statistics.gov.scot/def/dimension/populationGroup") {::match ::var}]}
 :filters  {:dim2 {:end {:time ['(>= "2008-01-01T00:00:00Z")]}}}
 :order-by [{:ASC [:dim1 :label]}]}

(def empty-model {:bindings {}
                  :filters {}
                  :order-by []})

(defn update-binding-value [old-value new-value]
  (cond
    (= ::var old-value) new-value
    (= ::var new-value) old-value
    :else (throw (Exception. "TODO: handle multiple values for same path"))))

(defn update-binding-spec [old-spec new-spec]
  (-> old-spec
      (update ::match update-binding-value (::match new-spec))
      (update ::optional? #(and %1 %2) (::optional? new-spec))))

(defn format-path [path]
  (string/join " -> " (map (fn [[key uri]] (str "[" key ", " uri "]")) path)))

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

(defn add-binding [model path value & {:keys [optional?]
                                       :or {optional? false}}]
  (if (= 0 (count path))
    model
    (update model :bindings (fn [b] (update-binding b [] path {::match value ::optional? optional?})))))

(defn add-filter [model key-path filter]
  ;;TODO: check key-path is valid
  (update model :filters (fn [fm] (update-in fm key-path (fn [v]
                                                           (conj (or v []) filter))))))

(defn add-order-by [model order-by]
  (update model :order-by conj order-by))

(defn key-path->var-name [key-path]
  (string/join "" (map name key-path)))

(defn key-path->query-var [key-path]
  (->QueryVar (key-path->var-name key-path)))

(defn is-literal? [l]
  (and (some? l) (not= ::var l)))

(defn get-query-var-bindings [{:keys [bindings] :as model}]
  (->> bindings
       (filter (fn [[k [pred m]]] (is-literal? (::match m))))
       (map (fn [[k [pred m]]]
              [(::match m) k]))))

(defn format-query-var-binding [[uri var-kw]]
  (let [query-var (->QueryVar (name var-kw))]
    (str "BIND(" (query-format uri) " AS " (query-format query-var) ")")))

(defn get-query-order-by [{:keys [order-by]}]
  (map (fn [ordering]
         (let [dir (if (contains? ordering :ASC) :ASC :DESC)
               key-path (or (:ASC ordering) (:DESC ordering))]
           [dir (key-path->query-var key-path)]))
       order-by))

(defn format-ordering [[dir query-var]]
  (if (= :ASC dir)
    (query-format query-var)
    (str "DESC(" (query-format query-var) ")")))

(defn format-query-order-by [orderings]
  (if (empty? orderings)
    ""
    (str "ORDER BY " (string/join " " (map format-ordering orderings)))))

(defn get-path-bgps [key-path parent [predicate binding-spec]]
  (let [match (::match binding-spec)
        optional? (::optional? binding-spec)
        nested (dissoc binding-spec ::match ::optional?)
        path-var-name (key-path->var-name key-path)
        object (if (is-literal? match) match (->QueryVar path-var-name))
        child-bgps (mapcat (fn [[key binding-spec]]
                             (get-path-bgps (conj key-path key) object binding-spec))
                           nested)]
    (conj child-bgps {::s parent ::p predicate ::o object ::optional? optional?})))

(defn get-query-triple-patterns [{:keys [bindings] :as model} obs-var-name]
  (mapcat (fn [[key spec-pair]]
            (get-path-bgps [key] (->QueryVar obs-var-name) spec-pair))
          bindings))

(defn format-query-triple-pattern [#::{:keys [s p o optional?]}]
  (let [tp (str (query-format s) " " (query-format p) " " (query-format o) " .")]
    (if optional?
      (str "OPTIONAL { " tp " }")
      tp)))

(defn get-filters [path-prefix k v]
  (if (map? v)
    (mapcat (fn [[ck cv]]
              (get-filters (conj path-prefix k) ck cv))
            v)
    (let [query-var (key-path->query-var (conj path-prefix k))]
      (map (fn [f] [query-var f]) v))))

(defn get-query-filters [{:keys [filters] :as model}]
  (mapcat (fn [[k v]] (get-filters [] k v)) filters))

(defn format-filter [[query-var [fun value]]]
  (format "FILTER(%s %s %s)" (query-format query-var) fun (query-format value)))

(defn get-query [model obs-var-name dataset-uri]
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

(defn get-observation-count-query [model obs-var-name dataset-uri]
  (let [obs-var (->QueryVar obs-var-name)]
    (str
      "PREFIX qb: <http://purl.org/linked-data/cube#>"
      "SELECT (COUNT(*) AS ?c) WHERE {"
      "  " (query-format obs-var) " a qb:Observation ."
      "  " (query-format obs-var) " qb:dataSet " (query-format dataset-uri) " ."
      (string/join " " (map format-query-triple-pattern (get-query-triple-patterns model obs-var-name)))
      (string/join " " (map format-filter (get-query-filters model)))
      "}")))

(defn get-observation-aggregation-query [model aggregation-fn dataset-uri measure-uri]
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

(defn example []
  (-> empty-model
      (add-binding [[:dim1 sdmx:refArea]] ::var)
      (add-binding [[:dim1 sdmx:refArea] [:label rdfs:label]] ::var :optional? true)
      (add-binding [[:dim2 sdmx:refPeriod] [:end time:hasEnd] [:time time:DateTime]] ::var)
      (add-binding [[:dim2 sdmx:refPeriod] [:begin time:hasBeginning] [:time time:DateTime]] ::var)
      (add-binding [[:dim3 (URI. "http://statistics.gov.scot/def/dimension/gender")]] (URI. "http://statistics.gov.scot/def/concept/gender/male"))
      (add-binding [[:dim4 (URI. "http://purl.org/linked-data/cube#measureType")]] ::var)
      (add-binding [[:dim5 (URI. "http://statistics.gov.scot/def/dimension/populationGroup")]] ::var)
      (add-filter [:dim2 :end :time] ['>= (Date. 108 0 1)])
      (add-filter [:dim2 :begin :time] ['<= (Date. 118 0 1)])
      (add-order-by {:ASC [:dim1 :label]})))