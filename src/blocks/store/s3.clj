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
      AmazonS3Exception
      Bucket
      GetObjectRequest
      ListObjectsRequest
      ObjectListing
      ObjectMetadata
      PutObjectRequest
      S3Object
      S3ObjectSummary)))


;; ## S3 Utilities

(defn s3-uri
  "Constructs a URI referencing an object in S3."
  [bucket object-key]
  (java.net.URI. "s3" bucket (str "/" object-key) nil))


(defn get-region
  "Translates a Clojure keyword into an S3 region instance. Throws an exception
  if the keyword doesn't match a supported region."
  [region]
  (some->
    (if (string? region) (keyword region) region)
    (case
      :us-west-1 Regions/US_WEST_1
      :us-west-2 Regions/US_WEST_2
      (throw (IllegalArgumentException.
               (str "No supported region matching " (pr-str region)))))
    (Region/getRegion)))



;; ## S3 Key Translation

(defn- trim-slashes
  "Cleans a string by removing leading and trailing slashes, then leading and
  trailing whitespace. Returns nil if the resulting string is empty."
  ^String
  [string]
  (when-not (empty? string)
    (let [result (-> string
                     (str/replace #"^/*([^/].*[^/])/*$" "$1")
                     (str/trim))]
      (when-not (empty? result)
        result))))


(defn- get-subkey
  "Checks an object key against a common prefix. If provided, the prefix is
  checked against the key to ensure it actually matches the beginning of the
  key. If prefix is nil, or trims down to an empty string, the key is returned
  unchanged."
  [prefix ^String object-key]
  (if-let [prefix' (trim-slashes prefix)]
    (do
      (when-not (.startsWith object-key (str prefix' "/"))
        (throw (IllegalStateException.
                 (str "S3 object " object-key
                      " is not under prefix " prefix'))))
      (subs object-key (count prefix')))
    object-key))


(defn- id->key
  "Converts a multihash identifier to an S3 object key, potentially applying a
  common prefix. Multihashes are rendered as hex strings."
  [prefix id]
  (let [block-subkey (multihash/hex id)]
    (if-let [prefix' (trim-slashes prefix)]
      (str prefix' "/" block-subkey)
      block-subkey)))


(defn- key->id
  "Converts an S3 object key into a multihash identifier, potentially stripping
  out a common prefix. The block subkey must be a valid hex-encoded multihash."
  [prefix object-key]
  (let [block-subkey (get-subkey prefix object-key)]
    (when (empty? block-subkey)
      (throw (IllegalStateException.
               (str "Cannot parse id from empty block subkey: " object-key))))
    (when-not (re-matches #"^[0-9a-f]+$" block-subkey)
      (throw (IllegalStateException.
               (str "Block subkey " block-subkey " is not valid hexadecimal"))))
    (multihash/decode block-subkey)))



;; ## S3 Block Functions

(defn- summary-stats
  "Generates a metadata map from an S3ObjectSummary."
  [prefix ^S3ObjectSummary object]
  {:id (key->id prefix (.getKey object))
   :size (.getSize object)
   :source (s3-uri (.getBucketName object) (.getKey object))
   :stored-at (.getLastModified object)})


(defn- metadata-stats
  "Generates a metadata map from an ObjectMetadata."
  [id bucket object-key ^ObjectMetadata metadata]
  {:id id
   :size (.getContentLength metadata)
   :source (s3-uri bucket object-key)
   :stored-at (.getLastModified metadata)})


;; Block records are stored in a bucket in S3, under some key prefix.
(defrecord S3BlockStore
  [^AmazonS3 client
   ^String bucket
   ^String prefix]

  block/BlockStore

  (stat
    [this id]
    (let [object-key (id->key prefix id)]
      (try
        (let [response (.getObjectMetadata client bucket object-key)]
          (metadata-stats id bucket object-key response))
        (catch AmazonS3Exception ex
          ; Check for not-found errors and return nil.
          (when (not= 404 (.getStatusCode ex))
            (throw ex))))))


  (-list
    [this opts]
    (let [request (doto (ListObjectsRequest.)
                    (.setBucketName bucket)
                    (.setPrefix prefix)
                    (.setMarker (:after opts))
                    (.setMaxKeys (:limit opts)))
          response (.listObjects client request)]
      ; TODO: check if isTruncated is true, make lazy seq which respects :limit
      (map (partial summary-stats prefix) (.getObjectSummaries response))))


  (-get
    [this id]
    (let [object-key (id->key prefix id)
          object (.getObject client bucket object-key)]
      ; TODO: check metadata to see if block already exists
      ; TODO: return lazy blocks, not literals
      (block/with-stats
        (with-open [content (.getObjectContent object)]
          (block/read! content))
        (metadata-stats bucket object-key (.getObjectMetadata object)))))


  (put!
    [this block]
    ; TODO: check if block already exists, if so return updated lazy block
    (let [metadata (doto (ObjectMetadata.)
                     (.setContentLength (:size block)))
          object-key (id->key prefix (:id block))
          result (with-open [content (block/open block)]
                   (.putObject client bucket object-key content metadata))]
      ; TODO: return new lazy block, not literal
      (block/with-stats
        (block/load! block)
        (metadata-stats bucket object-key (.getMetadata result)))))


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
