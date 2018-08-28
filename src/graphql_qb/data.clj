(ns graphql-qb.data
  (:require
    [clojure.string :as string]
    [grafter.rdf.repository :as repo]
    [grafter.rdf.formats :as formats]
    [grafter.rdf.io :as gio]
    [grafter.rdf :refer [add]]
    [graphql-qb.util :as util]
    [grafter.rdf.formats :as formats]
    [clojure.java.io :as io])
  (:import [java.io File FileFilter]
           [java.net URI URISyntaxException]))

(defn- ^FileFilter create-file-filter [p]
  (reify FileFilter
    (accept [_this file]
      (p file))))

(defn- is-rdf-file? [^File file]
  (some? (formats/filename->rdf-format file)))

(defn directory-repo
  "Creates a sail repository from a directory containing RDF files"
  [^File dir]
  {:pre [(and (.isDirectory dir) (.exists dir))]}
  (let [rdf-files (.listFiles dir (create-file-filter is-rdf-file?))]
    (apply repo/fixture-repo rdf-files)))

(defn file->endpoint [^File f]
  (if (.exists f)
    (if (.isDirectory f)
      (directory-repo f)
      (repo/fixture-repo f))
    (throw (IllegalArgumentException. (format "%s does not exist" f)))))

(defmulti uri->endpoint (fn [^URI uri] (keyword (.getScheme uri))))

(defmethod uri->endpoint :http [^URI uri]
  (repo/sparql-repo uri))

(defmethod uri->endpoint :https [^URI uri]
  (repo/sparql-repo uri))

(defmethod uri->endpoint :file [^URI uri]
  (let [f (File. uri)]
    (file->endpoint f)))

(defmethod uri->endpoint :default [^URI uri]
  (let [f (File. (str uri))]
    (file->endpoint f)))

(defn parse-endpoint [^String endpoint-str]
  (try
    (let [uri (URI. endpoint-str)]
      (uri->endpoint uri))
    (catch URISyntaxException ex
      (let [f (io/file endpoint-str)]
        (file->endpoint f)))))

(defn get-test-repo []
  (directory-repo (io/file "data")))

(defn get-scotland-repo []
  (repo/sparql-repo "https://staging-drafter-sg.publishmydata.com/v1/sparql/live"))

(defn fetch-geo-labels
  "Fetches the labels for all areas found across all observations and
  saves them into a resource file."
  []
  (let [live-repo (get-scotland-repo)
        test-repo (get-test-repo)
        q (str
           "SELECT DISTINCT ?area WHERE {"
           "  ?obs <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/linked-data/cube#Observation> ."
           "  ?obs <http://purl.org/linked-data/sdmx/2009/dimension#refArea> ?area ."
           "}")
        geo-uris (map :area (util/eager-query test-repo q))
        values (string/join " " (map (fn [uri] (str "<" uri ">")) geo-uris))
        label-q (str
                 "CONSTRUCT { ?geo <http://www.w3.org/2000/01/rdf-schema#label> ?label } WHERE {"
                 "  VALUES ?geo { " values " }"
                 "  ?geo <http://www.w3.org/2000/01/rdf-schema#label> ?label ."
                 "}")
        quads (util/eager-query live-repo label-q)]
    (add (gio/rdf-serializer "resources/geo-labels.nt" :format formats/rdf-ntriples) quads)))
