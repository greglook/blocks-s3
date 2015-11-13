(ns blocks.store.s3-test
  (:require
    (blocks.store
      [s3 :as s3 :refer [s3-store]])
    [clojure.test :refer :all]
    [multihash.core :as multihash])
  (:import
    (com.amazonaws.services.s3.model
      ObjectMetadata
      S3ObjectSummary)))


(deftest key-parsing
  (testing "id->key"
    (let [id->key @#'s3/id->key]
      (is (= "foo/bar/11040123abcd" (id->key "foo/bar/" (multihash/decode "11040123abcd")))
          "id maps to hex encoding under prefix")))
  (testing "key->id"
    (let [key->id @#'s3/key->id
          mhash (multihash/decode "11040123abcd")]
      (is (nil? (key->id nil nil))
          "should return nil for nil key")
      (is (nil? (key->id "baz/" "foo/bar/11040123abcd"))
          "should return nil for mismatched prefix")
      (is (nil? (key->id nil "x1040123abcd"))
          "should return nil for non-hex key")
      (is (= mhash (key->id nil "11040123abcd"))
          "should return mhash for valid key with no prefix")
      (is (= mhash (key->id "foo/" "foo/11040123abcd"))
          "should return mhash for valid key with prefix"))))


(deftest stat-conversion
  (let [mhash (multihash/decode "11040123abcd")
        date (java.util.Date.)
        summary-stats @#'s3/summary-stats
        metadata-stats @#'s3/metadata-stats]
    (testing "summary-stats"
      (is (= {:id mhash
              :size 45
              :source (java.net.URI. "s3://test-bucket/foo/bar/11040123abcd")
              :stored-at date}
             (summary-stats
               "foo/bar/"
               (doto (S3ObjectSummary.)
                 (.setBucketName "test-bucket")
                 (.setKey "foo/bar/11040123abcd")
                 (.setSize 45)
                 (.setLastModified date))))))
    (testing "metadata-stats"
      (is (= {:id mhash
              :size 45
              :source (java.net.URI. "s3://test-bucket/foo/bar/11040123abcd")
              :stored-at date}
             (dissoc
               (metadata-stats
                 mhash
                 "test-bucket"
                 "foo/bar/11040123abcd"
                 (doto (ObjectMetadata.)
                   (.setContentLength 45)
                   (.setLastModified date)))
               :s3/metadata))))))


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
