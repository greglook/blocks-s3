(ns blocks.store.s3
  "Block storage backed by a bucket in Amazon S3."
  (:require
    (blocks
      [core :as block]
      [data :as data]
      [store :as store])
    [blocks.store.util :as util]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
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
      ListNextBatchOfObjectsRequest
      ListObjectsRequest
      ObjectListing
      ObjectMetadata
      PutObjectRequest
      PutObjectResult
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
  (when region
    ; TODO: use reflection here?
    (Region/getRegion
      (case region
        :us-west-1 Regions/US_WEST_1
        :us-west-2 Regions/US_WEST_2
        (throw (IllegalArgumentException.
                 (str "No supported region matching " (pr-str region))))))))


(defn get-client
  "Constructs an S3 client.

  Supported options:

  - `:credentials` a map with `:access-key` and `:secret-key` entries providing
    explicit AWS credentials.
  - `:region` a keyword or string designating the region to operate in."
  [opts]
  (let [client (if-let [creds (:credentials opts)]
                 (AmazonS3Client. (BasicAWSCredentials.
                                    (:access-key creds)
                                    (:secret-key creds)))
                 (AmazonS3Client.))]
    (when-let [region (get-region (:region opts))]
      (.setRegion client region))
    client))



;; ## S3 Key Translation

(defn- id->key
  "Converts a multihash identifier to an S3 object key, potentially applying a
  common prefix. Multihashes are rendered as hex strings."
  ^String
  [prefix id]
  (str prefix (multihash/hex id)))


(defn- key->id
  "Converts an S3 object key into a multihash identifier, potentially stripping
  out a common prefix. The block subkey must be a valid hex-encoded multihash."
  [prefix object-key]
  (some->
    object-key
    (util/check #(.startsWith ^String % (or prefix ""))
      (log/warnf "S3 object %s is not under prefix %s"
                 object-key (pr-str prefix)))
    (cond-> prefix (subs (count prefix)))
    (util/check #(re-matches #"[0-9a-fA-F]+" %)
      (log/warnf "Encountered block subkey with invalid hex: %s"
                 (pr-str value)))
    (multihash/decode)))



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
   :stored-at (.getLastModified metadata)
   :s3/metadata (into {} (.getRawMetadata metadata))})


(defn- object->block
  "Creates a lazy block to read from the given S3 object."
  [^AmazonS3 client ^String bucket prefix stats]
  (block/with-stats
    (data/lazy-block
      (:id stats) (:size stats)
      (let [object-key (id->key prefix (:id stats))]
        (fn object-reader
          ([]
           (log/debugf "Opening object %s" (s3-uri bucket object-key))
           (.getObjectContent (.getObject client bucket object-key)))
          ([^long start ^long end]
           (log/debugf "Opening object %s byte range [%d, %d)"
                       (s3-uri bucket object-key) start end)
           (let [request (doto (GetObjectRequest. bucket object-key)
                           (.setRange start (dec end)))]
             (.getObjectContent (.getObject client request)))))))
    (dissoc stats :id :size)))


(defn- list-objects-seq
  "Produces a lazy sequence which calls S3 ListObjects API for up to the
  specified number of object summaries."
  [^AmazonS3 client ^ListObjectsRequest request]
  (lazy-seq
    (log/debugf "ListObjects in %s after %s limit %s"
                (s3-uri (.getBucketName request) (.getPrefix request))
                (pr-str (.getMarker request))
                (pr-str (.getMaxKeys request)))
    (let [limit (.getMaxKeys request)
          listing (.listObjects client request)
          summaries (seq (.getObjectSummaries listing))
          new-limit (and limit (- limit (count summaries)))]
      (when summaries
        (concat summaries
                (when (and (.isTruncated listing)
                           (or (nil? new-limit)
                               (pos? new-limit)))
                  (let [next-batch (ListNextBatchOfObjectsRequest. listing)
                        new-request (doto (.toListObjectsRequest next-batch)
                                      (.setMaxKeys (and new-limit
                                                        (int new-limit))))]
                    (list-objects-seq client new-request))))))))


;; Block records are stored in a bucket in S3, under some key prefix.
(defrecord S3BlockStore
  [^AmazonS3 client
   ^String bucket
   ^String prefix]

  store/BlockStore

  (-stat
    [this id]
    (let [object-key (id->key prefix id)]
      (try
        (log/debugf "GetObjectMetadata %s" (s3-uri bucket object-key))
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
                    (.setMarker (str prefix (:after opts))))]
      (when-let [limit (:limit opts)]
        (.setMaxKeys request (int limit)))
      (->> (list-objects-seq client request)
           (map (partial summary-stats prefix))
           (util/select-stats opts))))


  (-get
    [this id]
    (when-let [stats (.-stat this id)]
      (object->block client bucket prefix stats)))


  (-put!
    [this block]
    (data/merge-blocks
      block
      (if-let [stats (.-stat this (:id block))]
        ; Block already exists, return lazy block.
        (object->block client bucket prefix stats)
        ; Otherwise, upload block to S3.
        (let [object-key (id->key prefix (:id block))
              ; TODO: support function which allows you to mutate the ObjectMetadata before it is sent in the putObject call?
              metadata (doto (ObjectMetadata.)
                         (.setContentLength (:size block)))]
          (log/debugf "PutObject %s to %s" block (s3-uri bucket object-key))
          (let [result (with-open [content (block/open block)]
                         (.putObject client bucket object-key content metadata))
                stats (metadata-stats (:id block) bucket object-key
                                      (.getMetadata ^PutObjectResult result))]
            (object->block client bucket prefix
                           (assoc stats
                                  :size (:size block)
                                  :stored-at (java.util.Date.))))))))


  (-delete!
    [this id]
    (when (.-stat this id)
      (let [object-key (id->key prefix id)]
        (log/debugf "DeleteObject %s" (s3-uri bucket object-key))
        (.deleteObject client bucket object-key))
      true)))


(defn erase!
  "Clears all blocks from the S3 store by deleting everything in the bucket
  matching the store prefix."
  [store]
  (let [^AmazonS3 client (:client store)
        request (doto (ListObjectsRequest.)
                  (.setBucketName (:bucket store))
                  (.setPrefix (:prefix store)))]
    (log/warnf "Erasing all objects under %s"
               (s3-uri (:bucket store) (:prefix store)))
    (->> (list-objects-seq client request)
         (map (fn [^S3ObjectSummary object]
                (.deleteObject client (:bucket store) (.getKey object))))
         (dorun))
    nil))



;; ## Store Construction

(defn- trim-slashes
  "Cleans a string by removing leading and trailing slashes, then leading and
  trailing whitespace. Returns nil if the resulting string is empty."
  ^String
  [string]
  (when-not (empty? string)
    (let [result (-> string
                     (str/replace #"^/*" "")
                     (str/replace #"/*$" "")
                     (str/trim))]
      (when-not (empty? result)
        result))))


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
  (S3BlockStore. (get-client opts)
                 (str/trim bucket)
                 (some-> (trim-slashes (:prefix opts))
                         (str "/"))))


;; Remove automatic constructor functions.
(ns-unmap *ns* '->S3BlockStore)
(ns-unmap *ns* 'map->S3BlockStore)
