(ns blocks.store.s3
  "S3 stores provide block storage backed by a bucket in Amazon S3.

  Each block is stored in a separate object in the bucket. Stores may be
  constructed using an `s3://<bucket-name>/<prefix>` URI."
  (:require
    [blocks.data :as data]
    [blocks.store :as store]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [com.stuartsierra.component :as component]
    [manifold.deferred :as d]
    [manifold.stream :as s]
    [multiformats.base.b16 :as hex]
    [multiformats.hash :as multihash])
  (:import
    (com.amazonaws.auth
      AWSCredentials
      AWSCredentialsProvider
      AWSStaticCredentialsProvider
      BasicAWSCredentials
      BasicSessionCredentials
      DefaultAWSCredentialsProviderChain)
    (com.amazonaws.regions
      Region
      Regions)
    (com.amazonaws.services.s3
      AmazonS3
      AmazonS3Client
      AmazonS3ClientBuilder)
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
      S3ObjectSummary)
    (java.io
      FilterInputStream
      InputStream)
    (java.time
      Instant)))


;; ## S3 Utilities

(defn- s3-uri
  "Construct a URI referencing an object in S3."
  [bucket object-key]
  (java.net.URI. "s3" bucket (str "/" object-key) nil))


(defn- aws-region
  "Translate a Clojure keyword into an S3 region instance. Throws an exception
  if the keyword doesn't match a supported region."
  ^Regions
  [region]
  (or (->> (.getEnumConstants Regions)
           (filter #(= (name region) (.getName ^Regions %)))
           (first))
      (throw (IllegalArgumentException.
               (str "No supported region matching " (pr-str region))))))


(def ^:private sse-algorithms
  "Map of supported Server Side Encryption algorithm keys."
  {:aes-256 ObjectMetadata/AES_256_SERVER_SIDE_ENCRYPTION})


(defn- get-sse-algorithm
  "Look up a supported SSE algorithm string constant or throw an exception if
  not supported."
  [algorithm]
  (or (get sse-algorithms algorithm)
      (throw (ex-info
               (str "Unsupported SSE algorithm " (pr-str algorithm))
               {:supported (set (keys sse-algorithms))
                :algorithm algorithm}))))


(defn- s3-credentials
  "Coerce several kinds of credential specs into an `AWSCredentialsProvider`
  that can be used to build a client."
  [creds]
  (cond
    ;; This is explicitly specified so that S3 block stores can use the global
    ;; provider instance, rather than the default S3 client behavior which tries
    ;; to operate in an anonymous mode if no credentials are found.
    (nil? creds)
    (DefaultAWSCredentialsProviderChain/getInstance)

    ;; Input is already a credential provider.
    (instance? AWSCredentialsProvider creds)
    creds

    ;; Static credentials.
    (instance? AWSCredentials creds)
    (AWSStaticCredentialsProvider. creds)

    ;; Static map credentials.
    (map? creds)
    (if (:session-token creds)
      (BasicSessionCredentials.
        (:access-key creds)
        (:secret-key creds)
        (:session-token creds))
      (BasicAWSCredentials.
        (:access-key creds)
        (:secret-key creds)))

    ;; Unknown specification.
    :else
    (throw (ex-info
             (str "Unknown credentials value format: " (pr-str creds))
             {:credentials creds}))))


(defn- s3-client
  "Construct a new S3 client."
  [credentials region]
  (->
    (AmazonS3ClientBuilder/standard)
    (.withCredentials (s3-credentials credentials))
    (cond->
      region
      (.withRegion (aws-region region)))
    (.build)))



;; ## S3 Keys

(defn- trim-slashes
  "Clean a string by removing leading and trailing whitespace and slashes.
  Returns nil if the resulting string is empty."
  ^String
  [path]
  (when-not (str/blank? path)
    (let [result (-> (str/trim path)
                     (str/replace #"^/*" "")
                     (str/replace #"/*$" ""))]
      (when-not (str/blank? result)
        result))))


(defn- id->key
  "Convert a multihash identifier to an S3 object key, potentially applying a
  common prefix. Multihashes are rendered as hex strings."
  ^String
  [prefix id]
  (str prefix (multihash/hex id)))


(defn- key->id
  "Convert an S3 object key into a multihash identifier, potentially stripping
  out a common prefix. The prefix must already be slash-trimmed and the block
  subkey must be a valid hex-encoded multihash."
  [prefix object-key]
  (let [hex (subs object-key (count prefix))]
    (if (re-matches #"[0-9a-fA-F]+" hex)
      (multihash/decode (hex/parse hex))
      (log/warnf "Object %s did not form valid hex entry: %s" object-key hex))))



;; ## Stat Metadata

(defn- summary-stats
  "Generates a metadata map from an S3ObjectSummary."
  [prefix ^S3ObjectSummary summary]
  (when-let [id (key->id prefix (.getKey summary))]
    (with-meta
      {:id id
       :size (.getSize summary)
       :stored-at (Instant/ofEpochMilli (.getTime (.getLastModified summary)))}
      {::bucket (.getBucketName summary)
       ::key (.getKey summary)})))


(defn- metadata-stats
  "Generates a metadata map from an ObjectMetadata."
  [id bucket object-key ^ObjectMetadata metadata]
  (with-meta
    {:id id
     :size (.getContentLength metadata)
     :stored-at (if-let [last-modified (.getLastModified metadata)]
                  (Instant/ofEpochMilli (.getTime last-modified))
                  (Instant/now))}
    {::bucket bucket
     ::key object-key
     ::metadata (into {}
                      (remove
                        (comp #{"Accept-Ranges"
                                "Content-Length"
                                "Last-Modified"}
                              key))
                      (.getRawMetadata metadata))}))



;; ## Object Content

(defn- auto-draining-stream
  "Wraps an `InputStream` in a proxy which will automatically drain the
  underlying stream when it is closed."
  [^InputStream stream]
  (proxy [FilterInputStream] [stream]
    (close
      []
      ;; TODO: be smarter about this; for large remaining payloads it may be
      ;; faster to abort and re-establish the connection than to drain the rest
      ;; of the object.
      (let [start (System/nanoTime)]
        (loop [drained 0]
          (if (pos? (.read stream))
            (recur (inc drained))
            (when (pos? drained)
              (log/tracef "Drained %d bytes in %.2f ms while closing S3 input stream"
                          drained
                          (/ (- (System/nanoTime) start) 1e6))))))
      (.close stream))))


(deftype S3ObjectReader
  [^AmazonS3 client
   ^String bucket
   ^String object-key]

  data/ContentReader

  (read-all
    [this]
    (log/tracef "Opening object %s" (s3-uri bucket object-key))
    (->> (.getObject client bucket object-key)
         (.getObjectContent)
         (auto-draining-stream)))


  (read-range
    [this start end]
    (log/tracef "Opening object %s byte range %d - %d"
                (s3-uri bucket object-key) start end)
    (->> (doto (GetObjectRequest. bucket object-key)
           (.setRange start (dec end)))
         (.getObject client)
         (.getObjectContent)
         (auto-draining-stream))))


(alter-meta! #'->S3ObjectReader assoc :private true)


(defn- object->block
  "Creates a lazy block to read from the given S3 object."
  [client stats]
  (let [stat-meta (meta stats)]
    (with-meta
      (data/create-block
        (:id stats)
        (:size stats)
        (:stored-at stats)
        (->S3ObjectReader
          client
          (::bucket stat-meta)
          (::key stat-meta)))
      stat-meta)))


(defn- get-object-stats
  "Look up a block object in S3. Returns the stats map if it exists, otherwise
  nil."
  [^AmazonS3 client bucket prefix id]
  (let [object-key (id->key prefix id)]
    (try
      (log/tracef "GetObjectMetadata %s" (s3-uri bucket object-key))
      (let [response (.getObjectMetadata client bucket object-key)]
        (metadata-stats id bucket object-key response))
      (catch AmazonS3Exception ex
        ;; Check for not-found errors and return nil.
        (when (not= 404 (.getStatusCode ex))
          (throw ex))))))


(defn- list-objects-seq
  "Produces a lazy sequence which calls S3 ListObjects API for up to the
  specified number of object summaries."
  [^AmazonS3 client ^ListObjectsRequest request]
  (lazy-seq
    (log/tracef "ListObjects in %s after %s limit %s"
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
                                      (.setMaxKeys (and new-limit (int new-limit))))]
                    (list-objects-seq client new-request))))))))


(defn- list-objects
  "Friendlier wrapper around `list-objects-seq` which accepts a map of query
  options."
  [client bucket prefix query]
  (let [request (doto (ListObjectsRequest.)
                  (.setBucketName bucket)
                  (.setPrefix prefix)
                  (.setMarker (str prefix (:after query))))]
    (when-let [limit (:limit query)]
      (.setMaxKeys request (int limit)))
    (list-objects-seq client request)))



;; ## S3 Store

;; Block records are stored in a bucket in S3, under some key prefix.
(defrecord S3BlockStore
  [^AmazonS3 client
   ^String bucket
   ^String prefix
   credentials
   region
   sse
   alter-put-metadata]

  component/Lifecycle

  (start
    [this]
    (if client
      this
      (assoc this :client (s3-client credentials region))))


  (stop
    [this]
    ;; TODO: close client?
    (assoc this :client nil))


  store/BlockStore

  (-list
    [this opts]
    (let [out (s/stream 1000)]
      (store/future'
        (try
          (loop [objects (->> (select-keys opts [:after :limit])
                              (list-objects client bucket prefix)
                              (keep (partial summary-stats prefix)))]
            (when-let [stats (first objects)]
              ;; Check that the id is still before the marker, if set.
              (when (or (nil? (:before opts))
                        (pos? (compare (:before opts) (multihash/hex (:id stats)))))
                ;; Process next block; recur if accepted by the stream.
                (when @(s/put! out (object->block client stats))
                  (recur (next objects))))))
          (catch Exception ex
            (log/error ex "Failure listing S3 blocks")
            (s/put! out ex))
          (finally
            (s/close! out))))
      (s/source-only out)))


  (-stat
    [this id]
    (store/future'
      (get-object-stats client bucket prefix id)))


  (-get
    [this id]
    (store/future'
      (when-let [stats (get-object-stats client bucket prefix id)]
        (object->block client stats))))


  (-put!
    [this block]
    (store/future'
      (if-let [stats (get-object-stats client bucket prefix (:id block))]
        ;; Block already stored, return it.
        (object->block client stats)
        ;; Upload block to S3.
        (let [object-key (id->key prefix (:id block))
              metadata (doto (ObjectMetadata.)
                         (.setContentLength (:size block)))]
          (when sse
            (.setSSEAlgorithm metadata (get-sse-algorithm sse)))
          (when alter-put-metadata
            (alter-put-metadata this metadata))
          (log/tracef "PutObject %s to %s" block (s3-uri bucket object-key))
          (let [result (with-open [content (data/content-stream block nil nil)]
                         (.putObject client bucket object-key content metadata))
                stats (assoc (metadata-stats
                               (:id block) bucket object-key
                               (.getMetadata ^PutObjectResult result))
                             :size (:size block))]
            (object->block client stats))))))


  (-delete!
    [this id]
    (store/future'
      (if (get-object-stats client bucket prefix id)
        (let [object-key (id->key prefix id)]
          (log/tracef "DeleteObject %s" (s3-uri bucket object-key))
          (.deleteObject client bucket object-key)
          true)
        false)))


  store/ErasableStore

  (-erase!
    [this]
    (store/future'
      (log/infof "Erasing all objects under %s"
                 (s3-uri bucket prefix))
      (run!
        (fn delete-object
          [^S3ObjectSummary object]
          (.deleteObject client bucket (.getKey object)))
        (list-objects client bucket prefix {}))
      true)))



;; ## Store Construction

(store/privatize-constructors! S3BlockStore)


(defn s3-block-store
  "Creates a new S3 block store. If credentials are not provided, the AWS SDK
  will use a number of mechanisms to infer them from the environment.

  Supported options:

  - `:credentials`
    Authentication credentials to use for the store. There are several
    possibilities:
      - An `AWSCredentialsProvider` to draw credentials from dynamically.
      - A static `AWSCredentials` object to use directly.
      - A map with `:access-key`, `:secret-key`, and optionally
        `:session-token` entries.
  - `:region`
    A keyword or string designating the region the bucket is in.
    (like `:us-west-2`)
  - `:prefix`
    A string prefix to store the blocks under. A trailing slash is always
    added if not present.
  - `:sse`
    A keyword algorithm selection to use Server Side Encryption when storing
    blocks. Currently only `:aes-256` is supported.
  - `:alter-put-metadata`
    A 2-arg function that will be called with the block store and a block's
    `ObjectMetadata` before it is written. This function may make any desired
    modifications on the metadata, such as custom encryption schemes, attaching
    extra headers, and so on."
  [bucket & {:as opts}]
  (when (or (not (string? bucket)) (str/blank? bucket))
    (throw (IllegalArgumentException.
             (str "Bucket name must be a non-empty string, got: "
                  (pr-str bucket)))))
  (map->S3BlockStore
    (assoc opts
           :bucket (str/trim bucket)
           :prefix (some-> (:prefix opts)
                           (trim-slashes)
                           (str "/")))))


(defmethod store/initialize "s3"
  [location]
  (let [uri (store/parse-uri location)]
    (s3-block-store
      (:host uri)
      :prefix (:path uri)
      :region (keyword (get-in uri [:query :region]))
      :sse (when-let [algorithm (keyword (get-in uri [:query :sse]))]
             ;; Check if the algorithm is supported.
             (get-sse-algorithm algorithm)
             algorithm)
      :credentials (when-let [creds (:user-info uri)]
                     {:access-key (:id creds)
                      :secret-key (:secret creds)}))))
