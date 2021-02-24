# Elasticsearch NIO Client

An Elasticsearch client for Swift based on [SwiftNIO](https://github.com/apple/swift-nio) and [AsyncHTTPClient](https://github.com/swift-server/async-http-client). The library also has a dependency on [Soto](https://github.com/soto-project/soto) to sign Elasticsearch requests for AWS. This library works with other Elasticsearch endpoints, including local ones as well as AWS, but it does pull it in as a dependency. If you're interested in not requiring Soto and don't want to pull it in, [open an issue](https://github.com/brokenhandsio/elasticsearch-nio-client/issues/new) and I can look at splitting it out.

## Supported Features

Currently the library supports:

* Document create
* Document update
* Document delete
* Document search
* Document count
* Index delete
* Index exists

If you'd like to add extra functionality, either [open an issue](https://github.com/brokenhandsio/elasticsearch-nio-client/issues/new) and raise a PR. Any contributions are gratefully accepted!

## Elasticsearch Version

The library has been tested again Elasticsearch 7.6.2, but should work for the most part against older versions.