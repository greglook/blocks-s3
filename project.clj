(defproject mvxcvi/blocks-s3 "0.1.0-SNAPSHOT"
  :description "Content-addressable S3 block store."
  :url "https://github.com/greglook/blocks-s3"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :deploy-branches ["master"]

  :dependencies
  [[com.amazonaws/aws-java-sdk-s3 "1.10.32"]
   [mvxcvi/blocks "0.4.2"]
   [mvxcvi/multihash "1.1.0"]
   [org.clojure/clojure "1.7.0"]
   [org.clojure/tools.logging "0.3.1"]]

  :aliases {"coverage" ["with-profile" "+test,+coverage" "cloverage"]
            "integration" ["with-profile" "+integration"]}

  :test-selectors {:unit (complement :integration)
                   :integration :integration}

  :whidbey
  {:tag-types {'multihash.core.Multihash {'data/hash 'multihash.core/base58}
               'blocks.data.Block {'blocks.data.Block (partial into {})}}}

  :profiles
  {:repl {:source-paths ["dev"]
          :dependencies [[org.clojure/tools.namespace "0.2.11"]]}
   :test {:dependencies [[commons-logging "1.2"]]
          :jvm-opts ["-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.NoOpLog"]}
   :coverage {:plugins [[lein-cloverage "1.0.6"]]
              :dependencies [[com.fasterxml.jackson.core/jackson-databind "2.6.3"]]
              :jvm-opts ["-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.SimpleLog"
                         "-Dorg.apache.commons.logging.simplelog.defaultlog=trace"]}
   :integration {:test-paths ["test-integration"]}})
