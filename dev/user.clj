(ns user
  (:require
    [blocks.core :as block]
    (blocks.store
      [s3 :refer [s3-store]]
      [tests :as tests :refer [random-bytes]])
    [clojure.java.io :as io]
    [clojure.repl :refer :all]
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.string :as str]
    [clojure.tools.namespace.repl :refer [refresh]]
    [multihash.core :as multihash])
  (:import
    blocks.data.Block
    multihash.core.Multihash))


(def cloud
  (s3-store "greglook-data"
            :prefix "blocks-test-2"
            :region :us-west-2))

(def s3 (:client cloud))
