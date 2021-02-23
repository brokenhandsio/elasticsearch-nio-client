import XCTest
import SotoElasticsearch
import NIO
import AsyncHTTPClient
import SotoElasticsearchService

class ElasticSearchIntegrationTests: XCTestCase {

    // MARK: - Properties
    var eventLoopGroup: MultiThreadedEventLoopGroup!
    var client: ElasticsearchClient!
    var httpClient: HTTPClient!
    var awsClient: AWSClient!

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
        let storeID = UUID()
        try setupItems(storeID: storeID)

        let results = try elasticsearchSearchRepository.searchItems(term: "Apples", storeID: storeID, departmentID: nil).wait()
        XCTAssertEqual(results.count, 5)
    }

    func testSearchItemsCount() throws {
        let storeID = UUID()
        try setupItems(storeID: storeID)

        let results = try elasticsearchSearchRepository.searchItemsCount(term: "Apples", storeID: storeID, departmentID: nil).wait()
        XCTAssertEqual(results.count, 5)
    }

    func testGettingCountOfAllItems() throws {
        let storeID = UUID()
        try setupItems(storeID: storeID)

        let result = try elasticsearchSearchRepository.searchItemsCount(term: nil, storeID: storeID, departmentID: nil).wait()
        XCTAssertEqual(result.count, 10)
    }

    func testSearchingItemsInDepartment() throws {
        let storeID = UUID()
        let department = UUID()
        try setupItems(storeID: storeID, departmentIDToUse: department)

        let results = try elasticsearchSearchRepository.searchItems(term: "Apples", storeID: storeID, departmentID: department).wait()
        XCTAssertEqual(results.count, 1)
    }

    func testSearchingItemsInDepartmentCount() throws {
        let storeID = UUID()
        let department = UUID()
        try setupItems(storeID: storeID, departmentIDToUse: department)

        let result = try elasticsearchSearchRepository.searchItemsCount(term: "Apples", storeID: storeID, departmentID: department).wait()
        XCTAssertEqual(result.count, 1)
    }

    func testGettingCountOfItemsInDepartment() throws {
        let storeID = UUID()
        let department = UUID()
        try setupItems(storeID: storeID, departmentIDToUse: department)

        let result = try elasticsearchSearchRepository.searchItemsCount(term: nil, storeID: storeID, departmentID: department).wait()
        XCTAssertEqual(result.count, 2)
    }

    func testSearchingCategories() throws {
        let storeID = UUID()
        let esIndex = "store-\(storeID.uuidString.lowercased())-categories"
        let category1 = TestDataBuilder.anyCategory(storeID: storeID, name: "Fresh Meat")
        let category2 = TestDataBuilder.anyCategory(storeID: storeID, name: "Cured Meat")
        let category3 = TestDataBuilder.anyCategory(storeID: storeID, name: "Bread")
        let category4 = TestDataBuilder.anyCategory(storeID: storeID, name: "Toiletries")

        _ = try elasticsearchSearchRepository.elasticSearchClient.createDocument(category1, in: esIndex).wait()
        _ = try elasticsearchSearchRepository.elasticSearchClient.createDocument(category2, in: esIndex).wait()
        _ = try elasticsearchSearchRepository.elasticSearchClient.createDocument(category3, in: esIndex).wait()
        _ = try elasticsearchSearchRepository.elasticSearchClient.createDocument(category4, in: esIndex).wait()

        // This is required for ES to settle and load the indexes to return the right results
        Thread.sleep(forTimeInterval: 1.0)

        let results = try elasticsearchSearchRepository.searchCategories(term: "meat", storeID: storeID).wait()
        XCTAssertEqual(results.count, 2)
    }

    // MARK: - Private
    private func setupItems(storeID: UUID, departmentIDToUse: UUID = UUID()) throws {
        let esIndex = "store-\(storeID.uuidString.lowercased())-items"
        for index in 1...10 {
            let name: String
            if index % 2 == 0 {
                name = "Some \(index) Apples"
            } else {
                name = "Some \(index) Bananas"
            }
            let departmentID: UUID
            if index % 5 == 0 {
                departmentID = departmentIDToUse
            } else {
                departmentID = UUID()
            }
            let item = TestDataBuilder.anySearchItemItem(storeID: storeID, departmentID: departmentID, name: name, upc: "abc-\(index)")
            _ = try elasticsearchSearchRepository.elasticSearchClient.createDocument(item, in: esIndex).wait()
        }

        // This is required for ES to settle and load the indexes to return the right results
        Thread.sleep(forTimeInterval: 1.0)
    }
}
