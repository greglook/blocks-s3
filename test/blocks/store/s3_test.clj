(ns blocks.store.s3-test
  (:require
    [blocks.core :as block]
    [blocks.store.s3 :as s3 :refer [s3-block-store]]
    [blocks.store.tests :as tests]
    [clojure.test :refer :all]
    [com.stuartsierra.component :as component]
    [multiformats.hash :as multihash])
  (:import
    (com.amazonaws.auth
      AWSCredentialsProvider
      BasicAWSCredentials)
    (com.amazonaws.services.s3
      AmazonS3)
    (com.amazonaws.services.s3.model
      ListObjectsRequest
      ObjectListing
      ObjectMetadata
      S3Object
      S3ObjectSummary)
    java.net.URI))


;; ## S3 Utilities

(deftest sse-algorithm-selection
  (is (thrown-with-msg? Exception #"Unsupported SSE algorithm :foo-bar"
        (#'s3/get-sse-algorithm :foo-bar)))
  (is (= "AES256" (#'s3/get-sse-algorithm :aes-256))))


(deftest region-selection
  (is (thrown-with-msg? Exception #"No supported region matching :foo"
        (#'s3/aws-region :foo)))
  (is (instance? com.amazonaws.regions.Regions (#'s3/aws-region :us-east-1)))
  (is (= "US_WEST_2" (str (#'s3/aws-region :us-west-2)))))


(deftest auth-credentials
  (testing "default credentials"
    (is (instance? AWSCredentialsProvider
                   (#'s3/credentials-provider nil))))
  (testing "custom provider"
    (let [provider (reify AWSCredentialsProvider)]
      (is (identical? provider (#'s3/credentials-provider provider)))))
  (testing "static credentials"
    (let [creds (BasicAWSCredentials. "key" "secret")
          provider (#'s3/credentials-provider creds)]
      (is (instance? AWSCredentialsProvider provider))
      (is (identical? creds (.getCredentials provider)))))
  (testing "map credentials"
    (is (thrown? Exception
          (#'s3/credentials-provider {:access-key "", :secret-key "secret"})))
    (is (thrown? Exception
          (#'s3/credentials-provider {:access-key "key"})))
    (testing "basic"
      (let [provider (#'s3/credentials-provider
                      {:access-key "key"
                       :secret-key "secret"})]
        (is (instance? AWSCredentialsProvider provider))
        (let [creds (.getCredentials provider)]
          (is (= "key" (.getAWSAccessKeyId creds)))
          (is (= "secret" (.getAWSSecretKey creds))))))
    (testing "session"
      (let [provider (#'s3/credentials-provider
                      {:access-key "key"
                       :secret-key "secret"
                       :session-token "session"})]
        (is (instance? AWSCredentialsProvider provider))
        (let [creds (.getCredentials provider)]
          (is (= "key" (.getAWSAccessKeyId creds)))
          (is (= "secret" (.getAWSSecretKey creds)))
          (is (= "session" (.getSessionToken creds)))))))
  (testing "bad creds"
    (is (thrown-with-msg? Exception #"Unknown credentials value type"
          (#'s3/credentials-provider "unintelligible")))))


(deftest client-construction
  (is (instance? AmazonS3 (#'s3/s3-client nil nil)))
  (is (instance? AmazonS3 (#'s3/s3-client nil :us-west-2))))



;; ## S3 Keys

(deftest uri-manipulation
  (testing "URI construction"
    (is (= (URI. "s3://my-data/foo/blocks/123abc")
           (#'s3/s3-uri "my-data" "foo/blocks/123abc"))))
  (testing "slash-trimming"
    (is (nil? (#'s3/trim-slashes "")))
    (is (nil? (#'s3/trim-slashes " /// ")))
    (is (= "foo/bar" (#'s3/trim-slashes "/foo/bar/  ")))))


(deftest key-parsing
  (testing "id->key"
    (is (= "foo/bar/11040123abcd" (#'s3/id->key "foo/bar/" (multihash/parse "11040123abcd")))
        "id maps to hex encoding under prefix"))
  (testing "key->id"
    (let [mhash (multihash/parse "11040123abcd")]
      (is (nil? (#'s3/key->id "baz/" "foo/bar/11040123abcd"))
          "should return nil for mismatched prefix")
      (is (nil? (#'s3/key->id nil "x1040123abcd"))
          "should return nil for non-hex key")
      (is (= mhash (#'s3/key->id nil "11040123abcd"))
          "should return mhash for valid key with no prefix")
      (is (= mhash (#'s3/key->id "foo/" "foo/11040123abcd"))
          "should return mhash for valid key with prefix"))))



;; ## Stat Metadata

(deftest stat-conversion
  (let [mhash (multihash/parse "11040123abcd")
        instant (java.time.Instant/parse "2019-03-10T19:59:00Z")
        date (java.util.Date. (.toEpochMilli instant))]
    (testing "summary-stats"
      (is (nil? (#'s3/summary-stats
                 "foo/bar/"
                 (doto (S3ObjectSummary.)
                   (.setBucketName "test-bucket")
                   (.setKey "foo/bar/abcxyz")
                   (.setSize 32)
                   (.setLastModified date)))))
      (let [stats (#'s3/summary-stats
                   "foo/bar/"
                   (doto (S3ObjectSummary.)
                     (.setBucketName "test-bucket")
                     (.setKey "foo/bar/11040123abcd")
                     (.setSize 45)
                     (.setLastModified date)))]
        (is (= {:id mhash
                :size 45
                :stored-at instant}
               stats))
        (is (= {::s3/bucket "test-bucket"
                ::s3/key "foo/bar/11040123abcd"}
               (meta stats)))))
    (testing "metadata-stats"
      (let [stats (#'s3/metadata-stats
                   mhash
                   "test-bucket"
                   "foo/bar/11040123abcd"
                   (doto (ObjectMetadata.)
                     (.setContentLength 45)
                     (.setLastModified date)))]
        (is (= {:id mhash
                :size 45
                :stored-at instant}
               stats))
        (is (= {::s3/bucket "test-bucket"
                ::s3/key "foo/bar/11040123abcd"
                ::s3/metadata {}}
               (meta stats)))))))



;; ## Object Content

,,,



;; ## Store Construction

(deftest store-construction
  (is (thrown-with-msg? Exception #"Bucket name must be a non-empty string"
        (s3-block-store nil))
      "bucket name should be required")
  (is (thrown-with-msg? Exception #"Bucket name must be a non-empty string"
        (s3-block-store "   "))
      "bucket name cannot be empty")
  (is (satisfies? blocks.store/BlockStore (s3-block-store "foo-bar-data")))
  (is (nil? (:prefix (s3-block-store "foo-bucket" :prefix ""))))
  (is (nil? (:prefix (s3-block-store "foo-bucket" :prefix "/"))))
  (is (= "foo/" (:prefix (s3-block-store "foo-bucket" :prefix "foo"))))
  (is (= "bar/" (:prefix (s3-block-store "foo-bucket" :prefix "bar/")))))


(deftest uri-initialization
  (testing "simple spec"
    (let [store (block/->store "s3://foo-bar-data/foo/bar")]
      (is (satisfies? blocks.store/BlockStore store))
      (is (= "foo-bar-data" (:bucket store)))
      (is (= "foo/bar/" (:prefix store)))))
  (testing "full spec"
    (let [store (block/->store "s3://key:secret@foo-bar-data/foo/bar?region=us-west-2&sse=aes-256")]
      (is (satisfies? blocks.store/BlockStore store))
      (is (= "foo-bar-data" (:bucket store)))
      (is (= "foo/bar/" (:prefix store)))
      (is (= {:access-key "key", :secret-key "secret"} (:credentials store)))
      (is (= :us-west-2 (:region store)))
      (is (= :aes-256 (:sse store))))))



;; ## Legacy Tests

#_
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


#_
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



;; ## Integration Tests

(def access-key-var "AWS_ACCESS_KEY_ID")
(def s3-bucket-var  "BLOCKS_S3_BUCKET")


(deftest ^:integration check-behavior
  (if (System/getenv access-key-var)
    (if-let [bucket (System/getenv s3-bucket-var)]
      (let [prefix (str *ns* "/" (System/currentTimeMillis))]
        (tests/check-store
          #(let [store (component/start
                         (s3-block-store
                           bucket
                           :prefix prefix
                           :region :us-west-2
                           :sse :aes-256))]
             @(block/erase! store)
             store)))
      (println "No" s3-bucket-var "in environment, skipping integration test!"))
    (println "No" access-key-var "in environment, skipping integration test!")))
