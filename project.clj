(defproject mvxcvi/blocks-s3 "2.0.0"
  :description "Content-addressable S3 block store."
  :url "https://github.com/greglook/blocks-s3"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :aliases
  {"coverage" ["with-profile" "+coverage" "cloverage"]}

  :deploy-branches ["master"]
  :pedantic? :abort

  :dependencies
  [[org.clojure/clojure "1.10.0"]
   [org.clojure/tools.logging "0.4.1"]
   [mvxcvi/blocks "2.0.1"]
   ; TODO: upgrade to software.amazon.awssdk/s3 v2
   [com.amazonaws/aws-java-sdk-s3 "1.11.491"]]

  :test-selectors
  {:default (complement :integration)
   :integration :integration}

  :codox
  {:metadata {:doc/format :markdown}
   :source-uri "https://github.com/greglook/blocks-s3/blob/master/{filepath}#L{line}"
   :output-path "target/doc/api"}

  :whidbey
  {:tag-types {'multiformats.hash.Multihash {'multi/hash 'multiformats.hash/hex}
               'blocks.data.Block {'blocks.data.Block
                                   #(array-map :id (:id %)
                                               :size (:size %)
                                               :stored-at (:stored-at %))}}}

  :profiles
  {:dev
   {:dependencies
    [[mvxcvi/blocks-tests "2.0.1"]
     [commons-logging "1.2"]]}

   :repl
   {:source-paths ["dev"]
    :dependencies
    [[org.clojure/tools.namespace "0.2.11"]]}

   :test
   {:jvm-opts ["-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.NoOpLog"]}

   :coverage
   {:pedantic? false
    :plugins [[lein-cloverage "1.1.1"]]
    :jvm-opts ["-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.SimpleLog"
               "-Dorg.apache.commons.logging.simplelog.defaultlog=trace"]}})
