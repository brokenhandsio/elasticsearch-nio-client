import XCTest
import SotoElasticsearch
import NIO
import AsyncHTTPClient
import SotoElasticsearchService

struct SomeItem: Codable {
    let name: String
}

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

    func testSearchItemsCount() throws {
        try setupItems()

        let results = try client.searchDocumentsCount(from: indexName, searchTerm: "Apples").wait()
        XCTAssertEqual(results.count, 5)
    }

//    func testSearchingCategories() throws {
//        let storeID = UUID()
//        let esIndex = "store-\(storeID.uuidString.lowercased())-categories"
//        let category1 = TestDataBuilder.anyCategory(storeID: storeID, name: "Fresh Meat")
//        let category2 = TestDataBuilder.anyCategory(storeID: storeID, name: "Cured Meat")
//        let category3 = TestDataBuilder.anyCategory(storeID: storeID, name: "Bread")
//        let category4 = TestDataBuilder.anyCategory(storeID: storeID, name: "Toiletries")
//
//        _ = try elasticsearchSearchRepository.elasticSearchClient.createDocument(category1, in: esIndex).wait()
//        _ = try elasticsearchSearchRepository.elasticSearchClient.createDocument(category2, in: esIndex).wait()
//        _ = try elasticsearchSearchRepository.elasticSearchClient.createDocument(category3, in: esIndex).wait()
//        _ = try elasticsearchSearchRepository.elasticSearchClient.createDocument(category4, in: esIndex).wait()
//
//        // This is required for ES to settle and load the indexes to return the right results
//        Thread.sleep(forTimeInterval: 1.0)
//
//        let results = try elasticsearchSearchRepository.searchCategories(term: "meat", storeID: storeID).wait()
//        XCTAssertEqual(results.count, 2)
//    }

    // MARK: - Private
    private func setupItems() throws {
        for index in 1...10 {
            let name: String
            if index % 2 == 0 {
                name = "Some \(index) Apples"
            } else {
                name = "Some \(index) Bananas"
            }
            let item = SomeItem(name: name)
            _ = try client.createDocument(item, in: self.indexName).wait()
        }

        // This is required for ES to settle and load the indexes to return the right results
        Thread.sleep(forTimeInterval: 1.0)
    }
}
