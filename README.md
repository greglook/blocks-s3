blocks-s3
=========

[![CircleCI](https://circleci.com/gh/greglook/blocks-s3.svg?style=shield&circle-token=2d7ae95392368c6f66dd93ab63420d0498f2b2fc)](https://circleci.com/gh/greglook/blocks-s3)
[![codecov](https://codecov.io/gh/greglook/blocks-s3/branch/develop/graph/badge.svg)](https://codecov.io/gh/greglook/blocks-s3)
[![API codox](https://img.shields.io/badge/doc-API-blue.svg)](https://greglook.github.io/blocks-s3/api/)
[![marginalia docs](https://img.shields.io/badge/doc-marginalia-blue.svg)](https://greglook.github.io/blocks-s3/marginalia/uberdoc.html)

This library implements a [content-addressable](https://en.wikipedia.org/wiki/Content-addressable_storage)
[block store](//github.com/greglook/blocks) backed by a bucket in
[Amazon S3](https://aws.amazon.com/s3/).


## Installation

Library releases are published on Clojars. To use the latest version with
Leiningen, add the following dependency to your project definition:

[![Clojars Project](http://clojars.org/mvxcvi/blocks-s3/latest-version.svg)](http://clojars.org/mvxcvi/blocks-s3)


## Usage

The `blocks.store.s3` namespace provides the `s3-block-store` constructor, or
you can use the `s3://<bucket>/<prefix>` URI syntax with `block/->store`.
Stores are constructed with a bucket name and should usually include a key
prefix. Each block is stored as a separate object, keyed by the hex-encoded
multihash under the store's prefix.

With no other arguments, this will use the AWS SDK's
[default logic](http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html#credentials-default)
to find credentials. Otherwise, they can be provided by passing the store a
`:credentials` value - this can be a custom credentials provider, a static set
of credentials, or a map with at least an `:access-key` and `:secret-key`
entries.

```clojure
=> (require '[blocks.core :as block]
            '[blocks.store.s3 :refer [s3-block-store]]
            '[com.stuartsierra.component :as component])

; Create a new block store backed by S3:
=> (def store
     (component/start
       (s3-block-store "my-bucket"
                       :prefix "foo/bar/"
                       :region :us-west-2
                       :sse :aes-256)))

=> store
#blocks.store.s3.S3BlockStore
{:bucket "my-bucket",
 :client #<com.amazonaws.services.s3.AmazonS3Client@27107ade>,
 :prefix "foo/bar/",
 :region :us-west-2,
 :sse :aes-256}

; Files can be stored as blocks:
=> (def readme @(block/store! store (io/file "README.md")))

; Returned blocks have S3 storage metadata:
=> (meta readme)
#:blocks.store.s3
{:bucket "my-bucket",
 :key "foo/bar/1220a57d35a4d1b0405b275644fe9f18766a8e662cb56ed48d232a71153a78d81424",
 :metadata {"ETag" "6aa4f9b538ca79110dfdfeeed92da7f2",
            "x-amz-server-side-encryption" "AES256"}}

; Listing blocks finds objects in the bucket:
=> (block/list-seq store :limit 5)
(#blocks.data.Block
 {:id #multi/hash "1220a57d35a4d1b0405b275644fe9f18766a8e662cb56ed48d232a71153a78d81424",
  :size 3152,
  :stored-at #inst "2019-03-11T21:30:24Z"})

; Getting blocks makes a HEAD request to S3 to fetch object metadata.
=> @(block/get store (:id readme))
#blocks.data.Block
{:id #multi/hash "1220a57d35a4d1b0405b275644fe9f18766a8e662cb56ed48d232a71153a78d81424",
 :size 3152,
 :stored-at #inst "2019-03-11T21:30:24Z"}

; Returned blocks are lazy; content is not streamed until the block is opened.
=> (block/lazy? *1)
true
```


## License

This is free and unencumbered software released into the public domain.
See the UNLICENSE file for more information.
