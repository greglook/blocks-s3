(defproject mvxcvi/blocks-s3 "0.3.0-SNAPSHOT"
  :description "Content-addressable S3 block store."
  :url "https://github.com/greglook/blocks-s3"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :deploy-branches ["master"]

  :dependencies
  [[com.amazonaws/aws-java-sdk-s3 "1.11.5"]
   [mvxcvi/blocks "0.7.0"]
   [mvxcvi/multihash "2.0.0"]
   [org.clojure/clojure "1.7.0"]
   [org.clojure/tools.logging "0.3.1"]]

  :aliases {"docs" ["do" ["codox"] ["doc-lit"]]
            "doc-lit" ["marg" "--dir" "doc/marginalia"]
            "coverage" ["with-profile" "+test,+coverage" "cloverage"]}

  :test-selectors {:unit (complement :integration)
                   :integration :integration}

  :codox
  {:metadata {:doc/format :markdown}
   :source-uri "https://github.com/greglook/blocks-s3/blob/master/{filepath}#L{line}"
   :doc-paths ["doc/extra"]
   :output-path "doc/api"}

  :whidbey
  {:tag-types {'multihash.core.Multihash {'data/hash 'multihash.core/base58}
               'blocks.data.Block {'blocks.data.Block (partial into {})}}}

  :profiles
  {:repl {:source-paths ["dev"]
          :dependencies [[org.clojure/tools.namespace "0.2.11"]]}
   :test {:dependencies [[commons-logging "1.2"]]
          :jvm-opts ["-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.NoOpLog"]}
   :coverage {:plugins [[lein-cloverage "1.0.6"]]
              :dependencies [[com.fasterxml.jackson.core/jackson-databind "2.7.4"]]
              :jvm-opts ["-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.SimpleLog"
                         "-Dorg.apache.commons.logging.simplelog.defaultlog=trace"]}})
