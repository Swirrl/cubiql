(ns graphql-qb.vocabulary
  (:import [java.net URI]))

(def sdmx:refArea (URI. "http://purl.org/linked-data/sdmx/2009/dimension#refArea"))
(def sdmx:refPeriod (URI. "http://purl.org/linked-data/sdmx/2009/dimension#refPeriod"))
(def time:hasBeginning (URI. "http://www.w3.org/2006/time#hasBeginning"))
(def time:hasEnd (URI. "http://www.w3.org/2006/time#hasEnd"))
(def time:inXSDDateTime (URI. "http://www.w3.org/2006/time#inXSDDateTime"))
(def rdfs:label (URI. "http://www.w3.org/2000/01/rdf-schema#label"))
(def skos:prefLabel (URI. "http://www.w3.org/2004/02/skos/core#prefLabel"))
(def qb:codeList (URI. "http://purl.org/linked-data/cube#codeList"))

(def xsd:decimal (URI. "http://www.w3.org/2001/XMLSchema#decimal"))
(def xsd:string (URI. "http://www.w3.org/2001/XMLSchema#string"))
(def time:DateTime (URI. "http://www.w3.org/2006/time#inXSDDateTime"))