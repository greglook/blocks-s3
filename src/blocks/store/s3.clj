(ns blocks.store.s3
  "Block storage backed by a bucket in Amazon S3."
  (:require
    [blocks.core :as block]
    [clojure.string :as str]
    [multihash.core :as multihash])
  (:import
    (com.amazonaws.auth
      BasicAWSCredentials)
    (com.amazonaws.regions
      Region
      Regions)
    (com.amazonaws.services.s3
      AmazonS3
      AmazonS3Client)
    (com.amazonaws.services.s3.model
      Bucket
      GetObjectRequest
      ListObjectsRequest
      ObjectListing
      ObjectMetadata
      PutObjectRequest
      S3Object
      S3ObjectSummary)))


(defn- id->key
  [prefix id]
  (str prefix "/" (multihash/hex id)))


(defn- key->id
  [prefix ^String object-key]
  (when-not (.startsWith object-key prefix)
    (throw (IllegalStateException.
             (str "S3 object " object-key " is not under prefix " prefix))))
  (-> object-key
      (subs (inc (count prefix)))
      (multihash/decode)))


(defn- summary-stats
  "Generates a metadata map from an S3ObjectSummary."
  [prefix ^S3ObjectSummary object]
  {:id (key->id prefix (.getKey object))
   :size (.getSize object)
   :source (format "s3://%s/%s" (.getBucketName object) (.getKey object))
   :stored-at (.getLastModified object)})


(defn- metadata-stats
  "Generates a metadata map from an ObjectMetadata."
  [bucket object-key id ^ObjectMetadata metadata]
  {:id id
   :size (.getContentLength metadata)
   :source (format "s3://%s/%s" bucket object-key)
   :stored-at (.getLastModified metadata)})


;; Block records in a memory store are held in a map in an atom.
(defrecord S3BlockStore
  [^AmazonS3 client
   ^String bucket
   ^String prefix]

  block/BlockStore

  (stat
    [this id]
    (let [object-key (id->key prefix id)
          response (.getObjectMetadata client bucket object-key)]
      (metadata-stats bucket object-key id response)))


  (-list
    [this opts]
    (let [request (doto (ListObjectsRequest.)
                    (.setBucketName bucket)
                    (.setPrefix prefix)
                    (.setMarker (:after opts))
                    (.setMaxKeys (:limit opts)))
          response (.listObjects client request)]
      ; TODO: check if isTruncated is true, make lazy seq
      (map (partial summary-stats prefix) (.getObjectSummaries response))))


  (-get
    [this id]
    (let [object-key (id->key prefix id)
          s3-object (.getObject client bucket object-key)
          metadata (metadata-stats bucket object-key id (.getObjectMetadata s3-object))]
      ; TODO: lazy blocks, not literals
      (block/with-stats
        (block/read! (.getObjectContent s3-object))
        metadata)))


  (put!
    [this block]
    ; TODO: check if block already exists, if so return updated lazy block
    (let [metadata (doto (ObjectMetadata.)
                     (.setContentLength (:size block)))
          object-key (id->key prefix (:id block))
          result (with-open [content (block/open block)]
                   (.putObject client bucket object-key content metadata))]
      ; TODO: return new lazy block, not just metadata
      (metadata-stats bucket object-key (:id block) (.getMetadata result))))


  (delete!
    [this id]
    (throw (UnsupportedOperationException. "Not Yet Implemented"))))


(defn s3-store
  "Creates a new S3 block store. If credentials are not explicitly provided, the
  AWS SDK will use a number of mechanisms to infer them from the environment.

  Supported options:

  - `:credentials` a map with `:access-key` and `:secret-key` entries providing
    explicit AWS credentials.
  - `:region` a keyword or string designating the region the bucket is in.
  - `:prefix` a string prefix to store the blocks under."
  [bucket & {:as opts}]
  (when (or (not (string? bucket))
            (empty? (str/trim bucket)))
    (throw (IllegalArgumentException.
             (str "Bucket name must be a non-empty string, got: "
                  (pr-str bucket)))))
  (let [client (if-let [creds (:credentials opts)]
                 (AmazonS3Client. (BasicAWSCredentials.
                                    (:access-key creds)
                                    (:secret-key creds)))
                 (AmazonS3Client.))]
    (when-let [region (get-region (:region opts))]
      (.setRegion client region))
    (S3BlockStore. client
                   (str/trim bucket)
                   (trim-slashes (:prefix opts)))))


;; Remove automatic constructor functions.
(ns-unmap *ns* '->S3BlockStore)
(ns-unmap *ns* 'map->S3BlockStore)
