// swift-tools-version:6.0
import PackageDescription

let package = Package(
    name: "elasticsearch-swift",
    platforms: [
        .macOS(.v13),
        .iOS(.v15),
    ],
    products: [
        .library(
            name: "Elasticsearch",
            targets: ["Elasticsearch"]
        )
    ],
    dependencies: [
        .package(url: "https://github.com/swift-server/async-http-client.git", from: "1.0.0"),
        .package(url: "https://github.com/apple/swift-http-types.git", from: "1.4.0"),
    ],
    targets: [
        .target(
            name: "Elasticsearch",
            dependencies: [
                .product(name: "AsyncHTTPClient", package: "async-http-client"),
                .product(name: "HTTPTypes", package: "swift-http-types"),
            ]
        ),
        .testTarget(
            name: "ElasticsearchTests",
            dependencies: ["Elasticsearch"]
        ),
    ]
)
