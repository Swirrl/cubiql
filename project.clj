(defproject swirrl/cubiql "0.7.0-SNAPSHOT"
  :description "Query RDF Datacubes with GraphQL"
  :url "https://github.com/Swirrl/cubiql"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [com.walmartlabs/lacinia "0.23.0-rc-1"]
                 [com.walmartlabs/lacinia-pedestal "0.5.0-rc-2"]
                 [org.clojure/data.json "0.2.6"]
                 [grafter "0.11.5"]
                 [org.clojure/tools.cli "0.3.5"]

                 ;; logging
                 [org.slf4j/slf4j-api "1.7.25"]
                 [org.apache.logging.log4j/log4j-slf4j-impl "2.17.1"]
                 
                 [org.slf4j/jul-to-slf4j "1.7.25"]
                 [org.slf4j/jcl-over-slf4j "1.7.25"]
                 
                 ;;configuration
                 [aero "1.1.3"]]
  :main ^:skip-aot cubiql.main
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}}

  :release-tasks [["vcs" "assert-committed"]
                  ["change" "version"
                   "leiningen.release/bump-version" "release"]
                  ["vcs" "commit"]
                  ["vcs" "tag"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ;;["vcs" "push"]
                  ])
