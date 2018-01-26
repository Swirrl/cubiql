(ns graphql-qb.schema-model
  (:require [graphql-qb.types :as types]
            [graphql-qb.resolvers :as resolvers]
            [clojure.string :as string]))

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

(defn visit-type [path type]
  (cond
    (map? type)
    (let [obj-result (visit-object path type)
          type-name (path->object-name path)]
      {::schema (merge-schemas (::schema obj-result) {:objects {type-name (::object obj-result)}})
       ::name   type-name})

    (is-graphql-type? type)
    {::name type ::schema {}}

    (is-type-ref? type)
    {::name type ::schema {}}

    (vector? type)
    (let [type-def (first type)
          element-type (visit-type path type-def)]
      {::name (list 'list (::name element-type))
       ::schema (::schema element-type)})))

(defn visit-arg [path {:keys [type] :as arg-def}]
  (let [type-result (visit-type path type)
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

(defn visit-field [path field-name {:keys [type args resolve] :as field}]
  (let [type-result (visit-type path type)
        result {::field (assoc field :type (::name type-result)) ::schema (::schema type-result)}
        result (if (some? args)
                 (let [args-result (visit-args path args)]
                   (-> result
                       (assoc-in [::field :args] (::args args-result))
                       (update ::schema merge-schemas (::schema args-result))))
                 result)]
    ;;TODO: visit resolver
    result))

(defn visit-object [path {:keys [fields] :as object-def}]
  (reduce (fn [acc [field-name field-def]]
            (let [result (visit-field (conj path field-name) field-name field-def)]
              (-> acc
                  (update ::schema merge-schemas (::schema result))
                  (update-in [::object :fields] assoc field-name (::field result)))))
          {::schema {} ::object {}}
          fields))

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
                             :resolve     ::observations-page-resolver-name}
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

(defn visit-objects [])