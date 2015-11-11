(ns blocks.store.s3
  "Block storage backed by a bucket in Amazon S3."
  (:require
    [blocks.core :as block]))


;; Block records in a memory store are held in a map in an atom.
(defrecord S3BlockStore
  [bucket prefix]

  block/BlockStore

  (stat
    [this id]
    (throw (UnsupportedOperationException. "Not Yet Implemented")))


  (-list
    [this opts]
    (throw (UnsupportedOperationException. "Not Yet Implemented")))


  (-get
    [this id]
    (throw (UnsupportedOperationException. "Not Yet Implemented")))


  (put!
    [this block]
    (throw (UnsupportedOperationException. "Not Yet Implemented")))


  (delete!
    [this id]
    (throw (UnsupportedOperationException. "Not Yet Implemented"))))


(defn s3-store
  "Creates a new in-memory block store."
  ([bucket]
   (s3-store bucket ""))
  ([bucket prefix]
   (S3BlockStore. bucket prefix)))


;; Remove automatic constructor functions.
(ns-unmap *ns* '->S3BlockStore)
(ns-unmap *ns* 'map->S3BlockStore)
