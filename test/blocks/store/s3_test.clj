(ns blocks.store.s3-test
  (:require
    (blocks.store
      [s3 :refer [s3-store]])
    [clojure.test :refer :all]))


(deftest store-construction
  (is (thrown? IllegalArgumentException
               (s3-store nil))
      "bucket name should be required")
  (is (thrown? IllegalArgumentException
               (s3-store "   "))
      "bucket name cannot be empty")
  (is (some? (s3-store "foo-bar-data")))
  (is (some? (s3-store "foo-bar-data" :credentials {:access-key "foo"
                                                    :secret-key "bar"})))
  (is (some? (s3-store "foo-bucket" :region :us-west-2)))
  (is (thrown? IllegalArgumentException
               (s3-store "foo-bucket" :region :unicorns-and-rainbows)))
  (is (nil? (:prefix (s3-store "foo-bucket" :prefix ""))))
  (is (nil? (:prefix (s3-store "foo-bucket" :prefix "/"))))
  (is (= "foo/" (:prefix (s3-store "foo-bucket" :prefix "foo"))))
  (is (= "bar/" (:prefix (s3-store "foo-bucket" :prefix "bar/")))))
