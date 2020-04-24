(defproject mvxcvi/blocks-s3 "2.0.1-SNAPSHOT"
  :description "Content-addressable S3 block store."
  :url "https://github.com/greglook/blocks-s3"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :aliases
  {"coverage" ["with-profile" "+coverage" "cloverage"]}

  :deploy-branches ["master"]
  :pedantic? :abort

  :dependencies
  [[org.clojure/clojure "1.10.1"]
   [org.clojure/tools.logging "1.0.0"]
   [mvxcvi/blocks "2.0.4"]
   ;; TODO: investigate alternate SDK options:
   ;; [software.amazon.awssdk/s3 "..."]
   ;; https://github.com/cognitect-labs/aws-api
   [com.amazonaws/aws-java-sdk-s3 "1.11.769"]]

  :managed-dependencies
  [[commons-logging "1.2"]]

  :test-selectors
  {:default (complement :integration)
   :integration :integration}

  :whidbey
  {:tag-types {'multiformats.hash.Multihash {'multi/hash 'multiformats.hash/hex}
               'blocks.data.Block {'blocks.data.Block
                                   #(array-map :id (:id %)
                                               :size (:size %)
                                               :stored-at (:stored-at %))}}}

  :profiles
  {:dev
   {:dependencies
    [[mvxcvi/blocks-tests "2.0.4"]
     [org.slf4j/slf4j-simple "1.7.30"]]}

   :repl
   {:source-paths ["dev"]
    :dependencies
    [[org.clojure/tools.namespace "1.0.0"]]
    :jvm-opts ["-Dorg.slf4j.simpleLogger.defaultLogLevel=info"]}

   :test
   {:jvm-opts ["-Dorg.slf4j.simpleLogger.defaultLogLevel=fatal"]}

   :coverage
   {:pedantic? false
    :plugins [[lein-cloverage "1.1.2"]]
    :jvm-opts ["-Dorg.slf4j.simpleLogger.defaultLogLevel=debug"]}})
