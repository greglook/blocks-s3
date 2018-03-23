(defproject mvxcvi/blocks-s3 "0.3.4-SNAPSHOT"
  :description "Content-addressable S3 block store."
  :url "https://github.com/greglook/blocks-s3"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :aliases
  {"coverage" ["with-profile" "+coverage" "cloverage"]}

  :deploy-branches ["master"]
  :pedantic? :abort

  :dependencies
  [[org.clojure/clojure "1.9.0"]
   [org.clojure/tools.logging "0.4.0"]
   [mvxcvi/blocks "1.1.0"]
   [mvxcvi/multihash "2.0.3"]
   [com.amazonaws/aws-java-sdk-s3 "1.11.271"]
   [commons-logging "1.2"]]

  :test-selectors
  {:default (complement :integration)
   :integration :integration}

  :codox
  {:metadata {:doc/format :markdown}
   :source-uri "https://github.com/greglook/blocks-s3/blob/master/{filepath}#L{line}"
   :output-path "target/doc/api"}

  :whidbey
  {:tag-types {'multihash.core.Multihash {'data/hash 'multihash.core/base58}
               'blocks.data.Block {'blocks.data.Block (partial into {})}}}

  :profiles
  {:dev
   {:dependencies
    [[mvxcvi/test.carly "0.4.1"]]}

   :repl
   {:source-paths ["dev"]
    :dependencies [[org.clojure/tools.namespace "0.2.11"]]}

   :test
   {:jvm-opts ["-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.NoOpLog"]}

   :coverage
   {:plugins [[lein-cloverage "1.0.9"]]
    :dependencies [[riddley "0.1.15"]]
    :jvm-opts ["-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.SimpleLog"
               "-Dorg.apache.commons.logging.simplelog.defaultlog=trace"]}})
