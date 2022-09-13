# Elasticsearch NIO Client

An Elasticsearch client for Swift based on [SwiftNIO](https://github.com/apple/swift-nio) and [AsyncHTTPClient](https://github.com/swift-server/async-http-client). If you're interested in using this with AWS you may want to check out [Soto Elasticsearch NIO Client](https://github.com/brokenhandsio/soto-elasticsearch-nio-client).

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
let elasticsearchClient = ElasticsearchClient(httpClient: req.application.http.client.shared, eventLoop: req.eventLoop, logger: req.logger, host: host)
```

## Supported Features

Currently the library supports:

* Document create
* Document update
* Document delete
* Document search
* Document count
* Document retrieve
* Bulk create/update/delete/index
* Index delete
* Index exists
* Scripting

If you'd like to add extra functionality, either [open an issue](https://github.com/brokenhandsio/elasticsearch-nio-client/issues/new) and raise a PR. Any contributions are gratefully accepted!

## Elasticsearch Version

The library has been tested again Elasticsearch 8.4, but should work for the most part against older versions.
