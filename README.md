# Elasticsearch NIO Client

An Elasticsearch client for Swift based on [SwiftNIO](https://github.com/apple/swift-nio) and [AsyncHTTPClient](https://github.com/swift-server/async-http-client). The library also has a dependency on [Soto](https://github.com/soto-project/soto) to sign Elasticsearch requests for AWS. This library works with other Elasticsearch endpoints, including local ones as well as AWS, but it does pull it in as a dependency. If you're interested in not requiring Soto and don't want to pull it in, [open an issue](https://github.com/brokenhandsio/elasticsearch-nio-client/issues/new) and I can look at splitting it out.

## Installation and Usage

First add the library as a dependency in your dependencies array in **Package.swift**:

```swift
.package(url: "https://github.com/brokenhandsio/elasticsearch-nio-client.git", from: "0.1.0"),
```

Then add the dependency to the target you require it in:

```swift
.target(
    name: "App",
    dependencies: [
        // ...
        .product(name: "ElasticsearchNIOClient", package: "elasticsearch-nio-client")
    ],
)
```

Creating an instance of `ElasticsearchClient` depends on your environment, but you should be able to work it out depending on what you need. For Vapor, for example, you'd do something like:

```swift
let elasticsearchClient = ElasticsearchClient(eventLoop: req.eventLoop, logger: req.logger, awsClient: req.application.aws.client, httpClient: req.application.http.client.shared, host: host)
```

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