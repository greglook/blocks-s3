(ns blocks.store.s3-integration-test
  (:require
    (blocks.store
      [s3 :refer [s3-store]]
      [tests :refer [test-block-store]])
    [clojure.test :refer :all]))


(deftest ^:integration test-s3-store
  (if (System/getenv "AWS_ACCESS_KEY_ID")
    (let [store (s3-store "greglook-data"
                          :prefix (str "s3-block-store."
                                       (System/currentTimeMillis))
                          :region :us-west-2)]
      (test-block-store
        "s3-store" store
        :blocks 25
        :max-size 1024
        :eraser blocks.store.s3/erase!))
    (println "No AWS_ACCESS_KEY_ID in environment, skipping integration tests.")))
