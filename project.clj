(defproject mvxcvi/blocks-s3 "0.1.0-SNAPSHOT"
  :description "Content-addressable S3 block store."
  :url "https://github.com/greglook/blocks-s3"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :deploy-branches ["master"]

  :dependencies
  [[com.amazonaws/aws-java-sdk "1.10.32"]
   [mvxcvi/blocks "0.4.0"]
   [mvxcvi/multihash "1.1.0"]
   [org.clojure/clojure "1.7.0"]]

  :whidbey
  {:tag-types {'multihash.core.Multihash {'data/hash 'multihash.core/base58}
               'blocks.data.Block {'blocks.data.Block (partial into {})}}}

  :profiles
  {:repl {:source-paths ["dev"]
          :dependencies [[org.clojure/tools.namespace "0.2.10"]]}})
