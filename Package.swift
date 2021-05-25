// swift-tools-version:5.3
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "elasticsearch-nio-client",
    platforms: [
       .macOS(.v10_15)
    ],
    products: [
        // Products define the executables and libraries a package produces, and make them visible to other packages.
        .library(
            name: "ElasticsearchNIOClient",
            targets: ["ElasticsearchNIOClient"]),
    ],
    dependencies: [
        .package(url: "https://github.com/slashmo/async-http-client.git", .branch("feature/tracing")),
        .package(url: "https://github.com/apple/swift-distributed-tracing.git", from: "0.1.2"),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages this package depends on.
        .target(
            name: "ElasticsearchNIOClient",
            dependencies: [
                .product(name: "AsyncHTTPClient", package: "async-http-client"),
                .product(name: "Tracing", package: "swift-distributed-tracing"),
            ]),
        .testTarget(
            name: "ElasticsearchNIOClientTests",
            dependencies: ["ElasticsearchNIOClient"]),
    ]
)
