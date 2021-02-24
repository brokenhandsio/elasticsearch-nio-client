import XCTest
import ElasticsearchNIOClient
import NIO
import AsyncHTTPClient
import SotoElasticsearchService

class ElasticSearchIntegrationTests: XCTestCase {

    // MARK: - Properties
    var eventLoopGroup: MultiThreadedEventLoopGroup!
    var client: ElasticsearchClient!
    var httpClient: HTTPClient!
    var awsClient: AWSClient!
    let indexName = "some-index"

    // MARK: - Overrides
    override func setUpWithError() throws {
        eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let logger = Logger(label: "io.brokenhands.swift-soto-elasticsearch.test")
        httpClient = HTTPClient(eventLoopGroupProvider: .shared(eventLoopGroup))
        awsClient = AWSClient(credentialProvider: .static(accessKeyId: "SOMETHING", secretAccessKey: "SOMETHINGLESE"), httpClientProvider: .shared(httpClient), logger: logger)
        client = ElasticsearchClient(eventLoop: eventLoopGroup.next(), logger: logger, awsClient: awsClient, httpClient: httpClient, scheme: "http", host: "localhost", port: 9200)
        _ = try client.deleteIndex("_all").wait()
    }

    override func tearDownWithError() throws {
        try awsClient.syncShutdown()
        try httpClient.syncShutdown()
        try eventLoopGroup.syncShutdownGracefully()
    }

    // MARK: - Tests
    func testSearchingItems() throws {
        try setupItems()

        let results: ESGetMultipleDocumentsResponse<SomeItem> = try client.searchDocuments(from: indexName, searchTerm: "Apples").wait()
        XCTAssertEqual(results.hits.hits.count, 5)
    }

    func testSearchingItemsWithTypeProvided() throws {
        try setupItems()

        let results = try client.searchDocuments(from: indexName, searchTerm: "Apples", type: SomeItem.self).wait()
        XCTAssertEqual(results.hits.hits.count, 5)
    }

    func testSearchItemsCount() throws {
        try setupItems()

        let results = try client.searchDocumentsCount(from: indexName, searchTerm: "Apples").wait()
        XCTAssertEqual(results.count, 5)
    }

    func testCreateDocument() throws {
        let item = SomeItem(id: UUID(), name: "Banana")
        let response = try client.createDocument(item, in: self.indexName).wait()
        XCTAssertNotEqual(item.id.uuidString, response.id)
        XCTAssertEqual(response.index, self.indexName)
        XCTAssertEqual(response.result, "created")
    }

    func testCreateDocumentWithID() throws {
        let item = SomeItem(id: UUID(), name: "Banana")
        let response = try client.createDocumentWithID(item, in: self.indexName).wait()
        XCTAssertEqual(item.id.uuidString, response.id)
        XCTAssertEqual(response.index, self.indexName)
        XCTAssertEqual(response.result, "created")
    }

    func testUpdatingDocument() throws {
        let item = SomeItem(id: UUID(), name: "Banana")
        _ = try client.createDocumentWithID(item, in: self.indexName).wait()
        Thread.sleep(forTimeInterval: 0.5)
        let updatedItem = SomeItem(id: item.id, name: "Bananas")
        let response = try client.updateDocument(updatedItem, id: item.id.uuidString, in: self.indexName).wait()
        XCTAssertEqual(response.result, "updated")
    }

    func testDeletingDocument() throws {
        try setupItems()
        let item = SomeItem(id: UUID(), name: "Banana")
        _ = try client.createDocumentWithID(item, in: self.indexName).wait()
        Thread.sleep(forTimeInterval: 1.0)

        let results = try client.searchDocumentsCount(from: indexName, searchTerm: "Banana").wait()
        XCTAssertEqual(results.count, 1)
        Thread.sleep(forTimeInterval: 0.5)

        let response = try client.deleteDocument(id: item.id.uuidString, from: self.indexName).wait()
        XCTAssertEqual(response.result, "deleted")
        Thread.sleep(forTimeInterval: 0.5)

        let updatedResults = try client.searchDocumentsCount(from: indexName, searchTerm: "Banana").wait()
        XCTAssertEqual(updatedResults.count, 0)
    }

    func testIndexExists() throws {
        let item = SomeItem(id: UUID(), name: "Banana")
        let response = try client.createDocument(item, in: self.indexName).wait()
        XCTAssertEqual(response.index, self.indexName)
        XCTAssertEqual(response.result, "created")
        Thread.sleep(forTimeInterval: 0.5)

        let exists = try client.checkIndexExists(self.indexName).wait()
        XCTAssertTrue(exists)

        let notExists = try client.checkIndexExists("some-random-index").wait()
        XCTAssertFalse(notExists)
    }

    func testDeleteIndex() throws {
        let item = SomeItem(id: UUID(), name: "Banana")
        _ = try client.createDocument(item, in: self.indexName).wait()
        Thread.sleep(forTimeInterval: 0.5)

        let exists = try client.checkIndexExists(self.indexName).wait()
        XCTAssertTrue(exists)

        let response = try client.deleteIndex(self.indexName).wait()
        XCTAssertEqual(response.acknowledged, true)

        let notExists = try client.checkIndexExists(self.indexName).wait()
        XCTAssertFalse(notExists)
    }

    func testBulkCreate() throws {
        var items = [SomeItem]()
        for index in 1...10 {
            let name: String
            if index % 2 == 0 {
                name = "Some \(index) Apples"
            } else {
                name = "Some \(index) Bananas"
            }
            let item = SomeItem(id: UUID(), name: name)
            items.append(item)
        }

        let itemsWithIndex = items.map { ESBulkOperation(operationType: .create, index: self.indexName, id: $0.id.uuidString, document: $0) }
        let response = try client.bulk(itemsWithIndex).wait()
        XCTAssertEqual(response.errors, false)
        XCTAssertEqual(response.items.count, 10)
        XCTAssertEqual(response.items.first?.create?.result, "created")
        Thread.sleep(forTimeInterval: 1.0)

        let results = try client.searchDocumentsCount(from: indexName, searchTerm: nil).wait()
        XCTAssertEqual(results.count, 10)
    }

    // MARK: - Private
    private func setupItems() throws {
        for index in 1...10 {
            let name: String
            if index % 2 == 0 {
                name = "Some \(index) Apples"
            } else {
                name = "Some \(index) Bananas"
            }
            let item = SomeItem(id: UUID(), name: name)
            _ = try client.createDocument(item, in: self.indexName).wait()
        }

        // This is required for ES to settle and load the indexes to return the right results
        Thread.sleep(forTimeInterval: 1.0)
    }
}
