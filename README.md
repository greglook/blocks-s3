blocks-s3
=========

[![Dependency Status](https://www.versioneye.com/user/projects/5646aca2b5b03d001f00081e/badge.svg?style=flat)](https://www.versioneye.com/user/projects/5646aca2b5b03d001f00081e)
[![Build Status](https://travis-ci.org/greglook/blocks-s3.svg?branch=develop)](https://travis-ci.org/greglook/blocks-s3)
[![Coverage Status](https://coveralls.io/repos/greglook/blocks-s3/badge.svg?branch=develop&service=github)](https://coveralls.io/github/greglook/blocks-s3?branch=develop)
[![API codox](https://img.shields.io/badge/doc-API-blue.svg)](https://greglook.github.io/blocks-s3/api/)
[![marginalia docs](https://img.shields.io/badge/doc-marginalia-blue.svg)](https://greglook.github.io/blocks-s3/marginalia/uberdoc.html)
[![Join the chat at https://gitter.im/greglook/blocks](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/greglook/blocks)

This library implements a [content-addressable](https://en.wikipedia.org/wiki/Content-addressable_storage)
[block store](//github.com/greglook/blocks) backed by a bucket in Amazon S3.

## Installation

Library releases are published on Clojars. To use the latest version with
Leiningen, add the following dependency to your project definition:

[![Clojars Project](http://clojars.org/mvxcvi/blocks-s3/latest-version.svg)](http://clojars.org/mvxcvi/blocks-s3)

## Usage

The `blocks.store.s3` namespace provides the `s3-store` constructor. This takes
a bucket name and should usually include a key prefix. Blocks are stored as the
hex-encoded multihash under the key prefix.

With no other arguments, this will use the AWS SDK's
[default logic](http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html#credentials-default)
to find credentials. Otherwise, they can be provided by passing a `:credentials`
map with `:access-key` and `:secret-key` entries.

```clojure
=> (require '[blocks.core :as block]
            '[blocks.store.s3 :refer [s3-store]])

; Create a new block store backed by S3:
=> (def store (s3-store "my-bucket" :prefix "foo/bar/" :region :us-west-2))
#'user/store

=> store
#blocks.store.s3.S3BlockStore
{:bucket "my-bucket",
 :client #<com.amazonaws.services.s3.AmazonS3Client@27107ade>,
 :prefix "foo/bar/"}

; Listing blocks returns a lazy sequence:
=> (block/list store :limit 2)
({:id #data/hash "QmNNULDwCEew2pktA5UAy7qgupHfaXs7sbCi5gvGCKs3nD",
  :size 615,
  :source #whidbey/uri "s3://my-bucket/foo/bar/122000776d9007f2bcd00fb13c149ea1ed005e83bb00bcdaf6e17900194af8004e96",
  :stored-at #inst "2015-11-13T18:05:47.000-00:00"}
 {:id #data/hash "QmNNwLWaPCS7HUodgHPz9zoAsExd4xeuRv9SGWRkYamoQG",
  :size 94,
  :source #whidbey/uri "s3://my-bucket/foo/bar/12200095f66af8572b7cc3e425fa9b3123130eb47095550f0a439e41d68b9d6b0dcd",
  :stored-at #inst "2015-11-13T18:05:14.000-00:00"})

; Fetched blocks are lazy:
=> (block/get store (:id (first *1)))
#blocks.data.Block
{:id #data/hash "QmNNULDwCEew2pktA5UAy7qgupHfaXs7sbCi5gvGCKs3nD",
 :size 615}

=> (realized? *1)
false
```

## License

This is free and unencumbered software released into the public domain.
See the UNLICENSE file for more information.
