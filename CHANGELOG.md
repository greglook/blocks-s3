Change Log
==========

All notable changes to this project will be documented in this file, which
follows the conventions of [keepachangelog.com](http://keepachangelog.com/).
This project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

...

## [0.4.0] - 2018-03-22

### Changed
- Upgrade Clojure to 1.9.0
- Upgrade Blocks to 1.1.0
- Upgrade S3 SDK to 1.11.271

### Added
- S3 Block store supports basic server-side encryption (SSE) by setting the
  `:sse` attribute to `:aes-256`.
  [#4](https://github.com/greglook/blocks-s3/pull/4)
- An arbitrary `alter-put-metadata` function can be provided to mutate block
  object metadata before it is written.

### Fixed
- S3 block content streams will now automatically drain the remaining bytes in
  the stream when they are closed.
  [#2](https://github.com/greglook/blocks-s3/issues/2)

## [0.3.3] - 2017-04-28

### Changed
- Upgrade S3 SDK to version 1.11.124.
- Explicitly use default credentials provider chain for S3 client.

## [0.3.2] - 2017-03-31

### Changed
- Upgrade to blocks 0.9.0.

## [0.3.1] - 2017-02-15

### Changed
- Upgrade AWS S3 SDK to 1.11.91.

## [0.3.0] - 2016-08-14

### Changed
- Upgrade to `[blocks "0.8.0"]`

### Added
- Implement `blocks.store/initialize` for S3BlockStore.

## [0.2.1] - 2016-07-26

### Changed
- Switch to CircleCI for testing.
- Upgrade dependency verisons.

## [0.2.0] - 2016-01-14

### Added
- Add support for optimized ranged block open.

### Changed
- Upgrade AWS SDK to 1.10.45

## 0.1.0 - 2015-11-13

Initial library release.

[Unreleased]: https://github.com/greglook/blocks-s3/compare/0.4.0...HEAD
[0.4.0]: https://github.com/greglook/blocks-s3/compare/0.3.3...0.4.0
[0.3.3]: https://github.com/greglook/blocks-s3/compare/0.3.2...0.3.3
[0.3.2]: https://github.com/greglook/blocks-s3/compare/0.3.1...0.3.2
[0.3.1]: https://github.com/greglook/blocks-s3/compare/0.3.0...0.3.1
[0.3.0]: https://github.com/greglook/blocks-s3/compare/0.2.1...0.3.0
[0.2.1]: https://github.com/greglook/blocks-s3/compare/0.2.0...0.2.1
[0.2.0]: https://github.com/greglook/blocks-s3/compare/0.1.0...0.2.0
