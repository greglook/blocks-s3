(ns blocks.store.s3-test
  (:require
    (blocks.store
      [s3 :refer [s3-store]]
      [tests :refer [test-block-store]])
    [clojure.test :refer :all]))


; TODO: unit tests


(deftest ^:integration test-s3-store
  (let [store (s3-store "greglook-data"
                        :prefix (str "s3-block-store."
                                     (System/currentTimeMillis))
                        :region :us-west-2)]
    (test-block-store
      "s3-store" store
      :blocks 25
      :max-size 4096)
    (blocks.store.s3/erase! store)))
