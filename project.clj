(defproject clj-graphql "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.walmartlabs/lacinia "0.20.0"]
                 [com.walmartlabs/lacinia-pedestal "0.3.0"]
                 [org.clojure/data.json "0.2.6"]
                 [grafter "0.9.0"]]
  :main ^:skip-aot clj-graphql.main
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
