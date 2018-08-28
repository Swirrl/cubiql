(ns graphql-qb.types.scalars
  (:require [graphql-qb.util :as util]
            [com.walmartlabs.lacinia.schema :as lschema])
  (:import [java.net URI]
           [java.util Base64 Date]
           [java.time.format DateTimeFormatter]
           [java.time ZonedDateTime ZoneOffset]
           [org.openrdf.model Literal]
           [javax.xml.datatype XMLGregorianCalendar]))

(defn parse-sparql-cursor [^String base64-str]
  (let [bytes (.decode (Base64/getDecoder) base64-str)
        offset (util/bytes->long bytes)]
    (if (neg? offset)
      (throw (IllegalArgumentException. "Invalid cursor"))
      offset)))

(defn serialise-sparql-cursor [offset]
  {:pre [(>= offset 0)]}
  (let [bytes (util/long->bytes offset)
        enc (Base64/getEncoder)]
    (.encodeToString enc bytes)))

(defn parse-datetime [dt-string]
  (.parse DateTimeFormatter/ISO_OFFSET_DATE_TIME dt-string))

(defn serialise-datetime [dt]
  (.format DateTimeFormatter/ISO_OFFSET_DATE_TIME dt))

(def custom-scalars
  {:SparqlCursor
        {:parse     (lschema/as-conformer parse-sparql-cursor)
         :serialize (lschema/as-conformer serialise-sparql-cursor)}

   :uri {:parse     (lschema/as-conformer #(URI. %))
         :serialize (lschema/as-conformer str)}

   :DateTime
        {:parse     (lschema/as-conformer parse-datetime)
         :serialize (lschema/as-conformer serialise-datetime)}})

(defn date->datetime
  "Converts a java.util.Date to a java.time.ZonedDateTime."
  [^Date date]
  (ZonedDateTime/ofInstant (.toInstant date) ZoneOffset/UTC))

(defn grafter-date->datetime
  "Converts all known date literal representations used by Grafter into the corresponding
   DateTime."
  [dt]
  (cond
    (instance? Date dt)
    (date->datetime dt)

    (instance? Literal dt)
    (let [^Literal dt dt
          ^XMLGregorianCalendar xml-cal (.calendarValue dt)
          cal (.toGregorianCalendar xml-cal)
          date (.getTime cal)]
      (date->datetime date))

    :else
    (throw (IllegalArgumentException. (str "Unexpected date representation: " dt)))))
