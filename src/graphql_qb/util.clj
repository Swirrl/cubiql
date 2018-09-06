(ns graphql-qb.util
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [grafter.rdf.repository :as repo]
            [grafter.rdf.protocols :as pr])
  (:import [java.io PushbackReader]
           [java.nio ByteBuffer]
           [grafter.rdf.protocols LangString]
           [org.openrdf.repository RepositoryConnection]))

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
  "Renames the key k in the map m to new-k. If the optional keyword argument strict? is true
   then an exception will be thrown if k does not exist in m."
  [m k new-k & {:keys [strict?] :or {strict? false}}]
  (cond
    (contains? m k)
    (let [v (get m k)]
      (-> m
          (assoc new-k v)
          (dissoc k)))

    strict?
    (throw (IllegalArgumentException. (format "Source key %s not found in input map" (str k))))

    :else
    m))

(defn keyed-by
  "Returns a map {key item} for each element in the source sequence items according to the key function.
   If multiple items map to the same key, the last matching item will be the one included in the result map."
  [key-fn items]
  (into {} (map (fn [i] [(key-fn i) i]) items)))

(defn strict-map-by
  "Returns a map {key item} for the given sequence items and key function f. Throws an exception
   if any of the items in the input sequence map to the same key."
  [f items]
  (reduce (fn [acc i]
            (let [k (f i)]
              (if (contains? acc k)
                (throw (ex-info (str "Duplicate entries for k " k)
                                {:existing (get acc k)
                                 :duplicate i}))
                (assoc acc k i))))
          {}
          items))

(defn map-values
  "Maps each value in the map m according to the mapping function f."
  [f m]
  (into {} (map (fn [[k v]] [k (f v)]) m)))

(defn map-keys
  "Maps each key in the map m according to the mapping function f. If multiple keys in m are
  mapped to the same value by f, no guarantees are made about which key will be chosen."
  [f m]
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
  (let [buf (ByteBuffer/allocate 8)]
    (.putLong buf i)
    (.array buf)))

(defn bytes->long [^bytes bytes]
  {:pre [(= 8 (alength bytes))]}
  (let [buf (ByteBuffer/wrap bytes)]
    (.getLong buf)))

(defn eager-query
  "Executes a SPARQL query against the given repository and eagerly evaluates the results. This prevents
   connections being left open by lazy sequence operators."
  [repo sparql-string]
  (with-open [^RepositoryConnection conn (repo/->connection repo)]
    (doall (repo/query conn sparql-string))))

(defn find-first
  "Returns the first item in s which satisfies the predicate p. Returns nil if no items satisfy p."
  [p s]
  (first (filter p s)))

(defn label->string
  "Converts a grafter string type into a java string."
  [l]
  (some-> l str))

(defn convert-binding-labels
  "Returns a function which converts each label associated with the specified keys to a string."
  [keys]
  (fn [bindings]
    (reduce (fn [acc k]
              (update acc k label->string))
            bindings
            keys)))

(defprotocol HasLang
  (get-language-tag [this]
    "Returns the language tag associated with this item, or nil if there is no language."))

(extend-protocol HasLang
  String
  (get-language-tag [_s] nil)

  LangString
  (get-language-tag [ls] (name (pr/lang ls))))

(defn- score-language-match
  "Scores how closely the given string matches the specified language. A higher score indicates a better match,
   a score of 0 indicates no match."
  [s lang]
  (let [lang-tag (get-language-tag s)]
    (cond
      (= lang lang-tag) 3
      (and (some? lang) (nil? lang-tag)) 2
      (= lang-tag "en") 1
      :else 0)))

(defn find-best-language
  "Searches for the Grafter string with the closest matching language label. Strings which match the requested
   language tag exactly are preferred, followed by string literals without a language, then english labels. If neither
   a matching language, string literal or english label can be found then nil is returned."
  [labels lang]
  (when (seq labels)
    (let [with-scores (map (fn [l] [l (score-language-match l lang)]) labels)
          [label score] (apply max-key second with-scores)]
      (if (pos? score)
        (label->string label)))))

(defn strict-get
  "Retrieves the value associated with the key k from the map m. Throws an exception if the key
   does not exist. The optional :key-desc keyword argument can be used to describe the keys in the
   map and will be included in the error message for the exception thrown if the key is not found."
  [m k & {:keys [key-desc] :or {key-desc "Key"}}]
  (let [v (get m k ::missing)]
    (if (= ::missing v)
      (throw (ex-info (format "%s %s not found" key-desc (str k)) {}))
      v)))

(defn to-multimap
  "Takes a sequence of maps and returns a map of the form {:key [values]} for each key encountered in all
   of the input maps. Any entries where a key is mapped to a nil value is ignored."
  [maps]
  (let [non-nil-pairs (mapcat (fn [m] (filter (comp some? val) m)) maps)]
    (reduce (fn [acc [k v]]
              (update acc k (fnil conj []) v))
            {}
            non-nil-pairs)))

(defn xmls-boolean->boolean
  "XML Schema allows boolean values to be represented as the literals true, false 0 or 1.
   This function converts the clojure representation of those values into the corresponding
   boolean value."
  [xb]
  (if (number? xb)
    (= 1 xb)
    (boolean xb)))