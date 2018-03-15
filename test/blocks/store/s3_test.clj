(ns blocks.store.s3-test
  (:require
    [blocks.core :as block]
    [blocks.store.s3 :as s3 :refer [s3-block-store]]
    [blocks.store.tests :as tests]
    [clojure.test :refer :all]
    [multihash.core :as multihash])
  (:import
    (com.amazonaws.services.s3
      AmazonS3)
    (com.amazonaws.services.s3.model
      ListObjectsRequest
      ObjectListing
      ObjectMetadata
      S3Object
      S3ObjectSummary)))


;; ## Unit Tests

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


(deftest block-creation
  (let [object->block @#'s3/object->block
        reference (block/read! "this is a block")
        calls (atom [])
        client (reify AmazonS3
                 (^S3Object getObject
                   [this ^String bucket ^String object-key]
                   (swap! calls conj [:getObject bucket object-key])
                   (doto (S3Object.)
                     (.setObjectContent (block/open reference)))))
        block (object->block client "blocket" "data/test/"
                             {:id (:id reference), :size (:size reference)})]
    (is (block/lazy? block) "should return lazy block")
    (is (= (:id reference) (:id block)) "returns correct id")
    (is (= (:size reference) (:size block)) "returns correct size")
    (is (empty? @calls) "no calls to S3 on block init")
    (is (= "this is a block" (slurp (block/open block)))
        "returns correct content from mock open")
    (is (= [[:getObject "blocket" (str "data/test/" (multihash/hex (:id reference)))]]
           @calls)
        "makes one call to getObject for open")))


(deftest lazy-object-listing
  (let [list-objects-seq @#'s3/list-objects-seq
        bucket "blocket"
        prefix "data/"
        calls (atom nil)
        object-listing (fn [truncated? summaries]
                         (proxy [ObjectListing] []
                           (getObjectSummaries [] summaries)
                           (getBucketName [] bucket)
                           (getPrefix [] prefix)
                           (isTruncated [] (boolean truncated?))))
        list-client (fn [results]
                      (let [responses (atom (seq results))]
                        (reify AmazonS3
                          (^ObjectListing listObjects
                           [this ^ListObjectsRequest request]
                           (swap! calls conj [:listObject request])
                           (let [result (first @responses)]
                             (swap! responses next)
                             result)))))]
    (testing "empty listing"
      (reset! calls [])
      (let [client (list-client [(object-listing false [])])
            request (doto (ListObjectsRequest.)
                      (.setBucketName bucket)
                      (.setPrefix prefix))
            listing (list-objects-seq client request)]
        (is (not (realized? listing)) "should create a lazy sequence")
        (is (empty? @calls) "should not make any calls until accessed")
        (is (empty? listing) "listing is empty seq")
        (is (= 1 (count @calls)) "should make one call")
        (is (= :listObject (first (first @calls))) "should make one listObjects call")
        (is (= request (second (first @calls))) "should use initial list request")))
    (testing "full listing with no limit"
      (reset! calls [])
      (let [client (list-client [(object-listing false [::one ::two ::three])])
            request (doto (ListObjectsRequest.)
                      (.setBucketName bucket)
                      (.setPrefix prefix))
            listing (list-objects-seq client request)]
        (is (empty? @calls) "should not make any calls until accessed")
        (is (= [::one ::two ::three] listing) "listing has three elements")
        (is (= 1 (count @calls)) "should make one call")))
    (testing "full listing with limited results"
      (reset! calls [])
      (let [client (list-client [(object-listing true  [::one ::two])
                                 (object-listing false [::three ::four])])
            request (doto (ListObjectsRequest.)
                      (.setBucketName bucket)
                      (.setPrefix prefix)
                      (.setMaxKeys (int 4)))
            listing (list-objects-seq client request)]
        (is (empty? @calls) "should not make any calls until accessed")
        (is (= ::one (first listing)) "first element from listing")
        (is (= 1 (count @calls)) "should make one call for first element")
        (is (= [::one ::two ::three ::four] listing) "full listing has four elements")
        (is (= 2 (count @calls)) "should make second call for full listing")
        (let [req (second (second @calls))]
          (is (= 2 (.getMaxKeys req)) "second call should reduce limit"))))
    (testing "full listing with truncated results"
      (reset! calls [])
      (let [client (list-client [(object-listing true  [::one ::two])
                                 (object-listing false [::three ::four])])
            request (doto (ListObjectsRequest.)
                      (.setBucketName bucket)
                      (.setPrefix prefix))
            listing (list-objects-seq client request)]
        (is (empty? @calls) "should not make any calls until accessed")
        (is (= ::one (first listing)) "first element from listing")
        (is (= 1 (count @calls)) "should make one call for first element")
        (is (= [::one ::two ::three ::four] listing) "full listing has four elements")
        (is (= 2 (count @calls)) "should make second call for full listing")
        (let [req (second (second @calls))]
          (is (nil? (.getMaxKeys req)) "second call should not have limit"))))))


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
               (s3-block-store nil))
      "bucket name should be required")
  (is (thrown? IllegalArgumentException
               (s3-block-store "   "))
      "bucket name cannot be empty")
  (is (some? (s3-block-store "foo-bar-data")))
  (is (nil? (:prefix (s3-block-store "foo-bucket" :prefix ""))))
  (is (nil? (:prefix (s3-block-store "foo-bucket" :prefix "/"))))
  (is (= "foo/" (:prefix (s3-block-store "foo-bucket" :prefix "foo"))))
  (is (= "bar/" (:prefix (s3-block-store "foo-bucket" :prefix "bar/")))))


(deftest sse-algorithm-selection
  (is (thrown? Exception
        (s3/select-sse-algorithm nil)))
  (is (thrown? Exception
        (s3/select-sse-algorithm :foo/bar)))
  (is (= ObjectMetadata/AES_256_SERVER_SIDE_ENCRYPTION
         (s3/select-sse-algorithm :aes-256))))


;; ## Integration Tests

(def access-key-var "AWS_ACCESS_KEY_ID")
(def s3-bucket-var  "BLOCKS_S3_BUCKET")


(deftest ^:integration check-behavior
  (if (System/getenv access-key-var)
    (if-let [bucket (System/getenv s3-bucket-var)]
      (let [prefix (str *ns* "/" (System/currentTimeMillis))]
        (tests/check-store
          #(doto (s3-block-store bucket
                   :prefix prefix
                   :region :us-west-2)
             (block/erase!!))))
      (println "No" s3-bucket-var "in environment, skipping integration test!"))
    (println "No" access-key-var "in environment, skipping integration test!")))
