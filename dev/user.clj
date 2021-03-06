(ns user
  (:require
    [blocks.core :as block]
    [blocks.store.s3 :refer [s3-block-store]]
    [blocks.store.tests :as tests]
    [clojure.java.io :as io]
    [clojure.repl :refer :all]
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.string :as str]
    [clojure.tools.namespace.repl :refer [refresh]]
    [com.stuartsierra.component :as component]
    [multiformats.hash :as multihash]))


(def store
  (component/start
    (s3-block-store
      (System/getenv "BLOCKS_S3_BUCKET")
      :prefix (str (System/getenv "USER") "/blocks-s3-repl")
      :region :us-west-2
      :sse :aes-256)))
