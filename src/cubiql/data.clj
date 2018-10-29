(ns cubiql.data
  (:require
    [grafter.rdf.repository :as repo]
    [grafter.rdf.formats :as formats]
    [grafter.rdf :refer [add]]
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
