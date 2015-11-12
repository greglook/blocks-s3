(ns blocks.store.s3-test
  (:require
    (blocks.store
      [s3 :as s3 :refer [s3-store]])
    [clojure.test :refer :all]))


(deftest client-construction
  (is (some? (s3/get-client nil))
      "should return client with no required opts")
  (is (some? (s3/get-client {:credentials {:access-key "foo"
                                           :secret-key "bar"}}))
      "should return client for explicit credentials")
  (is (some? (s3/get-client {:region :us-west-2}))
      "should return client with valid region")
  (is (thrown? IllegalArgumentException
               (s3/get-client {:region :unicorns-and-rainbows}))
      "should throw exception for invalid region"))


(deftest store-construction
  (is (thrown? IllegalArgumentException
               (s3-store nil))
      "bucket name should be required")
  (is (thrown? IllegalArgumentException
               (s3-store "   "))
      "bucket name cannot be empty")
  (is (some? (s3-store "foo-bar-data")))
  (is (nil? (:prefix (s3-store "foo-bucket" :prefix ""))))
  (is (nil? (:prefix (s3-store "foo-bucket" :prefix "/"))))
  (is (= "foo/" (:prefix (s3-store "foo-bucket" :prefix "foo"))))
  (is (= "bar/" (:prefix (s3-store "foo-bucket" :prefix "bar/")))))
