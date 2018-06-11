(defproject graphql-qb "0.2.0-SNAPSHOT"
  :description "Query RDF Datacubes with graphQL"
  :url "http://swirrl.com/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [com.walmartlabs/lacinia "0.23.0-rc-1"]
                 [com.walmartlabs/lacinia-pedestal "0.5.0-rc-2"]
                 [org.clojure/data.json "0.2.6"]
                 [grafter "0.9.0"]
                 [org.clojure/tools.cli "0.3.5"]]
  :main ^:skip-aot graphql-qb.main
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
