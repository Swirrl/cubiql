(ns graphql-qb.schema-model
  (:require [graphql-qb.types :as types]
            [graphql-qb.resolvers :as resolvers]
            [clojure.string :as string]
            [clojure.pprint :as pp]))

(defn get-dimension-measure-enum [dataset]
  (types/build-enum :ignored (types/dataset-dimension-measures dataset)))

(defn get-order-by [{:keys [order order_spec] :as args} dataset]
  (let [dim-measure-enum (get-dimension-measure-enum dataset)]
    (map (fn [enum-value]
           (let [{:keys [field-name] :as dim-measure} (types/from-graphql dim-measure-enum enum-value)
                 dir (get order_spec field-name)]
             [dim-measure (or dir :ASC)]))
         order)))

(defn map-dimension-filter [{filter :dimensions :as args} {:keys [dimensions] :as dataset}]
  (into {} (map (fn [{:keys [field-name] :as f}]
                  (let [graphql-value (get filter field-name)]
                    [f (types/from-graphql f graphql-value)]))
                dimensions)))

(defn observation-args-mapper [dataset]
  (fn [args]
    (let [dim-filter (map-dimension-filter args dataset)
          order-by (get-order-by args dataset)]
      (-> args
          (assoc ::resolvers/dimensions-filter dim-filter)
          (assoc ::resolvers/order-by order-by)))))

(comment {:objects
          {:dataset-earnings
           {:fields
            {:observations
             {:type {:fields
                     {:sparql        {:type        'String
                                      :description "SPARQL query used to retrieve matching observations."
                                      :resolve     :resolve-observation-sparql-query}
                      :page          {:type        {:fields
                                                    {:next_page    {:type :SparqlCursor :description "Cursor to the next page of results"}
                                                     :count        {:type 'Int}
                                                     :observations {:type (list 'list observation-type-name) :description "List of observations on this page"}}}
                                      :args        {:after {:type :SparqlCursor}
                                                    :first {:type 'Int}}
                                      :description "Page of results to retrieve."
                                      :resolve     observations-page-resolver-name}
                      :total_matches {:type 'Int}}}
              :args {:dimensions :wat}}}}}})

(defn is-graphql-type? [x]
  (or (symbol? x)
      (and (keyword? x)
           (nil? (namespace x)))))

(defn is-type-ref? [x]
  (and (keyword? x)
       (some? (namespace x))))

;;TODO: this is a duplicate of schema/merge-schemas
(defn merge-schemas [s1 s2]
  (merge-with (fn [v1 v2]
                (if (and (map? v1) (map? v2))
                  (merge-schemas v1 v2)
                  v2))
              s1 s2))

(defn path->object-name [path]
  (keyword (string/join "_" (map name path))))

(declare visit-object)

(defn visit-type [path type direction]
  (cond
    (map? type)
    (let [obj-result (visit-object path type direction)
          type-name (path->object-name path)
          type-schema (if (= :output direction)
                        {:objects {type-name (::object obj-result)}}
                        {:input-objects {type-name (::object obj-result)}})]
      {::schema (merge-schemas (::schema obj-result) type-schema)
       ::name   type-name})

    (is-graphql-type? type)
    {::name type ::schema {}}

    (is-type-ref? type)
    {::name type ::schema {}}

    (vector? type)
    (let [type-def (first type)
          element-type (visit-type path type-def direction)]
      {::name (list 'list (::name element-type))
       ::schema (::schema element-type)})))

(defn visit-arg [path {:keys [type] :as arg-def}]
  (let [type-result (visit-type path type :input)
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

(defn visit-field [path field-name {:keys [type args resolve] :as field} direction]
  (let [type-result (visit-type path type direction)
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
  ([path object-def] (visit-object path object-def :output))
  ([path {:keys [fields] :as object-def} direction]
   (let [obj-result (reduce (fn [acc [field-name field-def]]
                              (let [result (visit-field (conj path field-name) field-name field-def direction)]
                                (-> acc
                                    (update ::schema merge-schemas (::schema result))
                                    (update-in [::object :fields] assoc field-name (::field result)))))
                            {::schema {} ::object {}}
                            fields)
         schema (::schema obj-result)
         out-obj (::object obj-result)
         obj-type-name (path->object-name path)
         obj-schema (if (= direction :output)
                      {:objects {obj-type-name out-obj}}
                      {:input-objects {obj-type-name out-obj}})]
     {::schema (merge-schemas schema obj-schema)
      ::object out-obj})))

(defn visit-query [query-name {:keys [type resolve] :as query-def}]
  (let [path [query-name]
        object-def (visit-object path type :output)
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

(def example-object
  {:fields
   {:observations
    {:type {:fields
            {:sparql        {:type        'String
                             :description "SPARQL query used to retrieve matching observations."
                             :resolve     :resolve-observation-sparql-query}
             :page          {:type        {:fields
                                           {:next_page    {:type :SparqlCursor :description "Cursor to the next page of results"}
                                            :count        {:type 'Int}
                                            :observations {:type [::observation-type-name] :description "List of observations on this page"}}}
                             :args        {:after {:type :SparqlCursor}
                                           :first {:type 'Int}}
                             :description "Page of results to retrieve."
                             :resolve     (fn [context args field] (println "resolving..."))}
             :total_matches {:type 'Int}}}
     :args {:dimensions {:type
                         {:fields {:gender     {:type ::gender_enum}
                                   :ref_area   {:type ::ref_area}
                                   :ref_period {:type ::ref_period}}}}
            :order      {:type [::dimensions-measures-enum]}
            :order_spec {:type
                         {:fields
                          {:gender   {:type ::order_direction}
                           :ref_area {:type ::order_direction}}}}}}}})

(def example-query
  {:dataset_earnings
   {:type example-object
    :resolve :query-resolver}})