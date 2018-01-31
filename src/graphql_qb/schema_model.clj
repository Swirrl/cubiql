(ns graphql-qb.schema-model
  (:require [graphql-qb.types :as types]
            [clojure.string :as string]
            [clojure.pprint :as pp]
            [graphql-qb.schema.mapping.labels])
  (:import [graphql_qb.schema.mapping.labels GroupMapping]))

(defn get-order-by [{:keys [order order_spec] :as args} dataset]
  (map (fn [dm-uri]
         (let [{:keys [field-name] :as dim-measure} (types/get-dataset-dimension-measure-by-uri dataset dm-uri)
               dir (get order_spec field-name :ASC)]
           [dim-measure dir]))
       order))

(defn map-dimension-filter [{filter :dimensions :as args} {:keys [dimensions] :as dataset}]
  (into {} (map (fn [{:keys [field-name] :as f}]
                  [f (get filter field-name)])
                dimensions)))

(defn is-graphql-type? [x]
  (or (symbol? x)
      (and (keyword? x)
           (nil? (namespace x)))))

(defn is-type-ref? [x]
  (and (keyword? x)
       (some? (namespace x))))

(defrecord NonNull [type-def])
(def non-null ->NonNull)

(defn is-enum-mapping? [type]
  (instance? GroupMapping type))

(defn merge-schemas [s1 s2]
  (merge-with (fn [v1 v2]
                (if (and (map? v1) (map? v2))
                  (merge-schemas v1 v2)
                  v2))
              s1 s2))

(defn path->object-name [path]
  (keyword (string/join "_" (map name path))))

(declare visit-object)

(defn visit-type [path type type-schema-key]
  (cond
    (instance? NonNull type)
    (let [type-def (:type-def type)
          element-type (visit-type path type-def type-schema-key)]
      {::name (list 'non-null (::name element-type))
       ::schema (::schema element-type)})

    (is-enum-mapping? type)
    (let [{:keys [name items]} type]
      {::name   name
       ::schema {:enums {name {:values (mapv :name items)}}}})

    (map? type)
    (let [obj-result (visit-object path type type-schema-key)
          type-name (path->object-name path)
          type-schema {type-schema-key {type-name (::object obj-result)}}]
      {::schema (merge-schemas (::schema obj-result) type-schema)
       ::name   type-name})

    (is-graphql-type? type)
    {::name type ::schema {}}

    (is-type-ref? type)
    {::name type ::schema {}}

    (vector? type)
    (let [type-def (first type)
          element-type (visit-type path type-def type-schema-key)]
      {::name (list 'list (::name element-type))
       ::schema (::schema element-type)})))

(defn visit-arg [path {:keys [type] :as arg-def}]
  (let [type-result (visit-type path type :input-objects)
        out-arg (assoc arg-def :type (::name type-result))]
    {::schema (::schema type-result) ::arg out-arg}))

(defn visit-args [path args]
  (reduce (fn [acc [arg-name arg-def]]
            (let [arg-result (visit-arg (conj path arg-name) arg-def)]
              (-> acc
                  (update ::schema merge-schemas (::schema arg-result))
                  (assoc-in [::args arg-name] (::arg arg-result)))))
          {::schema {} ::args {}}
          args))

(defn path->resolver-name [path]
  (keyword (str "resolve_" (string/join "_" (map name path)))))

(defn visit-resolver [path resolver]
  (cond
    (fn? resolver)
    (let [resolver-name (path->resolver-name path)]
      {::resolver resolver-name ::schema {:resolvers {resolver-name resolver}}})

    (keyword? resolver)
    {::resolver resolver ::schema {}}

    :else
    (throw (IllegalArgumentException. "Expected fn or keyword for resolver"))))

(defn visit-field [path field-name {:keys [type args resolve] :as field} type-schema-key]
  (let [type-result (visit-type path type type-schema-key)
        result {::field (assoc field :type (::name type-result)) ::schema (::schema type-result)}
        result (if (some? args)
                 (let [args-result (visit-args path args)]
                   (-> result
                       (assoc-in [::field :args] (::args args-result))
                       (update ::schema merge-schemas (::schema args-result))))
                 result)
        result (if (some? resolve)
                 (let [resolver-result (visit-resolver path resolve)]
                   (-> result
                       (assoc-in [::field :resolve] (::resolver resolver-result))
                       (update ::schema merge-schemas (::schema resolver-result))))
                 result)]
    ;;TODO: map resolver arguments?
    result))

(defn visit-object
  ([path object-def] (visit-object path object-def :objects))
  ([path {:keys [fields] :as object-def} type-schema-key]
   (let [fields-result (reduce (fn [acc [field-name field-def]]
                              (let [result (visit-field (conj path field-name) field-name field-def type-schema-key)]
                                (-> acc
                                    (update ::schema merge-schemas (::schema result))
                                    (update-in [::fields] assoc field-name (::field result)))))
                            {::schema {} ::fields {}}
                            fields)
         schema (::schema fields-result)
         out-fields (::fields fields-result)
         out-obj (assoc object-def :fields out-fields)
         obj-type-name (path->object-name path)
         obj-schema {type-schema-key {obj-type-name out-obj}}]
     {::schema (merge-schemas schema obj-schema)
      ::object out-obj})))

(defn visit-query [query-name {:keys [type resolve] :as query-def}]
  (let [path [query-name]
        object-def (visit-object path type :objects)
        resolver-def (visit-resolver path resolve)]
    (merge-schemas
      (merge-schemas (::schema object-def) (::schema resolver-def))
      {:queries
       {query-name
        {:type query-name
         :resolve (::resolver resolver-def)}}})))

(defn visit-queries [queries-def]
  (reduce (fn [acc [query-name query-def]]
            (let [query-schema (visit-query query-name query-def)]
              (merge-schemas acc query-schema)))
          {}
          queries-def))
