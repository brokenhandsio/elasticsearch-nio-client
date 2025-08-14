import AsyncHTTPClient
import Elasticsearch
import Foundation
import Logging
import Testing

@Suite(.serialized)
struct ElasticsearchTests {
    let client: ElasticsearchClient
    let httpClient: HTTPClient
    let indexName = "some-index"
    var logger: Logger

    init() async throws {
        httpClient = .shared
        logger = Logger(label: "io.brokenhands.swift-soto-elasticsearch.test")
        logger.logLevel = .debug
        client = try ElasticsearchClient(httpClient: httpClient, logger: logger, scheme: "http", host: "localhost", port: 9200)
        if try await client.checkIndexExists(indexName) {
            _ = try await client.deleteIndex(indexName)
        }
    }

    @Test(
        "URL Setup Validation",
        arguments: [
            ("", ElasticsearchClient.ValidationError.invalidURLString),
            ("://localhost:9200", ElasticsearchClient.ValidationError.missingURLScheme),
            ("localhost:9200", ElasticsearchClient.ValidationError.invalidURLScheme),
            ("http://:9200", ElasticsearchClient.ValidationError.missingURLHost),
            ("http://localhost:9200", nil),
        ])
    func testURLSetup(url: String, error: ElasticsearchClient.ValidationError?) async throws {
        if let error {
            #expect(throws: error) {
                try ElasticsearchClient(httpClient: httpClient, logger: logger, url: url)
            }
        } else {
            #expect(throws: Never.self) {
                try ElasticsearchClient(httpClient: httpClient, logger: logger, url: url)
            }
        }
    }

    @Test(
        "URL Scheme Validation",
        arguments: [
            ("incorrectScheme", ElasticsearchClient.ValidationError.invalidURLScheme),
            ("http", nil),
            ("https", nil),
        ])
    func testURLSchemeValidation(scheme: String, error: ElasticsearchClient.ValidationError?) async throws {
        if let error {
            #expect(throws: error) {
                try ElasticsearchClient(httpClient: httpClient, logger: logger, scheme: scheme, host: "localhost", port: 9200)
            }
        } else {
            #expect(throws: Never.self) {
                try ElasticsearchClient(httpClient: httpClient, logger: logger, scheme: scheme, host: "localhost", port: 9200)
            }
        }
    }

    @Test("Search Items")
    func testSearchingItems() async throws {
        try await setupItems()

        let results: ESGetMultipleDocumentsResponse<SomeItem> = try await client.searchDocuments(from: indexName, searchTerm: "Apples")
        #expect(results.hits.hits.count == 5)
    }

    @Test("Search Items With Type Provided")
    func testSearchingItemsWithTypeProvided() async throws {
        try await setupItems()

        let results = try await client.searchDocuments(from: indexName, searchTerm: "Apples", type: SomeItem.self)
        #expect(results.hits.hits.count == 5)
    }

    @Test("Search Items Count")
    func testSearchItemsCount() async throws {
        try await setupItems()

        let results = try await client.searchDocumentsCount(from: indexName, searchTerm: "Apples")
        #expect(results.count == 5)
    }

    @Test("Search Documents Total")
    func testSearchDocumentsTotal() async throws {
        for index in 1...100 {
            let name = "Some \(index) Apples"
            let item = SomeItem(id: UUID(), name: name)
            _ = try await client.createDocument(item, in: self.indexName)
        }

        // This is required for ES to settle and load the indexes to return the right results
        try await Task.sleep(for: .seconds(1))

        let results = try await client.searchDocuments(from: indexName, searchTerm: "Apples", type: SomeItem.self)
        #expect(results.hits.total!.value == 100)
        #expect(results.hits.total!.relation == .eq)
    }

    @Test("Create Document")
    func testCreateDocument() async throws {
        let item = SomeItem(id: UUID(), name: "Banana")
        let response = try await client.createDocument(item, in: self.indexName)
        #expect(item.id.uuidString != response.id)
        #expect(response.index == self.indexName)
        #expect(response.result == "created")
    }

    @Test("Create Document With ID")
    func testCreateDocumentWithID() async throws {
        let item = SomeItem(id: UUID(), name: "Banana")
        let response = try await client.createDocumentWithID(item, in: self.indexName)
        #expect(item.id == response.id)
        #expect(response.index == self.indexName)
        #expect(response.result == "created")
    }

    @Test("Update Document With Custom ID")
    func testUpdateDocumentWithCustomId() async throws {
        let item = SomeItem(id: UUID(), name: "Banana")
        _ = try await client.createDocumentWithID(item, in: self.indexName)
        try await Task.sleep(for: .seconds(0.5))
        let updatedItem = SomeItem(id: item.id, name: "Bananas")
        let response = try await client.updateDocument(updatedItem, id: item.id, in: self.indexName)
        #expect(response.result == "updated")
    }

    @Test("Update Document With ID")
    func testUpdateDocumentWithID() async throws {
        let item = SomeItem(id: UUID(), name: "Banana")
        _ = try await client.createDocumentWithID(item, in: self.indexName)
        try await Task.sleep(for: .seconds(1))
        let updatedItem = SomeItem(id: item.id, name: "Bananas")
        let response = try await client.updateDocument(updatedItem, in: self.indexName)
        #expect(response.result == "updated")
    }

    @Test("Delete Document")
    func testDeletingDocument() async throws {
        try await setupItems()
        let item = SomeItem(id: UUID(), name: "Banana")
        _ = try await client.createDocumentWithID(item, in: self.indexName)
        try await Task.sleep(for: .seconds(1))

        let results = try await client.searchDocumentsCount(from: indexName, searchTerm: "Banana")
        #expect(results.count == 1)
        try await Task.sleep(for: .seconds(0.5))

        let response = try await client.deleteDocument(id: item.id, from: self.indexName)
        #expect(response.result == "deleted")
        try await Task.sleep(for: .seconds(0.5))

        let updatedResults = try await client.searchDocumentsCount(from: indexName, searchTerm: "Banana")
        #expect(updatedResults.count == 0)
    }

    @Test("Create Index")
    func testCreateIndex() async throws {
        let mappings: [String: Any] = [
            "properties": [
                "keyword_field": [
                    "type": "keyword",
                    "fields": [
                        "test": [
                            "type": "text"
                        ]
                    ],
                ]
            ]
        ]
        let settings: [String: Any] = ["number_of_shards": 3]

        let response = try await client.createIndex(indexName, mappings: mappings, settings: settings)
        #expect(response.acknowledged == true)

        let exists = try await client.checkIndexExists(self.indexName)
        #expect(exists == true)
    }

    @Test("Index Exists")
    func testIndexExists() async throws {
        let item = SomeItem(id: UUID(), name: "Banana")
        let response = try await client.createDocument(item, in: self.indexName)
        #expect(response.index == self.indexName)
        #expect(response.result == "created")
        try await Task.sleep(for: .seconds(0.5))

        let exists = try await client.checkIndexExists(self.indexName)
        #expect(exists == true)

        let notExists = try await client.checkIndexExists("some-random-index")
        #expect(notExists == false)
    }

    @Test("Delete Index")
    func testDeleteIndex() async throws {
        let item = SomeItem(id: UUID(), name: "Banana")
        _ = try await client.createDocument(item, in: self.indexName)
        try await Task.sleep(for: .seconds(0.5))

        let exists = try await client.checkIndexExists(self.indexName)
        #expect(exists == true)

        let response = try await client.deleteIndex(self.indexName)
        #expect(response.acknowledged == true)

        let notExists = try await client.checkIndexExists(self.indexName)
        #expect(notExists == false)
    }

    @Test("Bulk Create")
    func testBulkCreate() async throws {
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

        let itemsWithIndex = items.map { ESBulkOperation(operationType: .create, index: self.indexName, id: $0.id, document: $0) }
        let response = try await client.bulk(itemsWithIndex)
        #expect(response.errors == false)
        #expect(response.items.count == 10)
        #expect(response.items.first?.create?.result == "created")
        try await Task.sleep(for: .seconds(1))

        let results = try await client.searchDocumentsCount(from: indexName, searchTerm: nil)
        #expect(results.count == 10)
    }

    @Test("Bulk Create, Update, Delete Index")
    func testBulkCreateUpdateDeleteIndex() async throws {
        let item1 = SomeItem(id: UUID(), name: "Item 1")
        let item2 = SomeItem(id: UUID(), name: "Item 2")
        let item3 = SomeItem(id: UUID(), name: "Item 3")
        let item4 = SomeItem(id: UUID(), name: "Item 4")
        let bulkOperation = [
            ESBulkOperation(operationType: .create, index: self.indexName, id: item1.id, document: item1),
            ESBulkOperation(operationType: .index, index: self.indexName, id: item2.id, document: item2),
            ESBulkOperation(operationType: .update, index: self.indexName, id: item3.id, document: item3),
            ESBulkOperation(operationType: .delete, index: self.indexName, id: item4.id, document: item4),
        ]

        let response = try await client.bulk(bulkOperation)
        #expect(response.items.count == 4)
        #expect(response.items[0].create != nil)
        #expect(response.items[1].index != nil)
        #expect(response.items[2].update != nil)
        #expect(response.items[3].delete != nil)
    }

    @Test("Search Items Paginated")
    func testSearchingItemsPaginated() async throws {
        for index in 1...100 {
            let name = "Some \(index) Apples"
            let item = SomeItem(id: UUID(), name: name)
            _ = try await client.createDocument(item, in: self.indexName)
        }

        // This is required for ES to settle and load the indexes to return the right results
        try await Task.sleep(for: .seconds(2))

        let results: ESGetMultipleDocumentsResponse<SomeItem> = try await client.searchDocumentsPaginated(
            from: indexName, searchTerm: "Apples", size: 20, offset: 10
        )
        #expect(results.hits.hits.count == 20)
        #expect(results.hits.hits.contains(where: { $0.source.name == "Some 11 Apples" }) == true)
        #expect(results.hits.hits.contains(where: { $0.source.name == "Some 29 Apples" }) == true)
    }

    @Test("Search Items With Type Provided Paginated")
    func testSearchingItemsWithTypeProvidedPaginated() async throws {
        for index in 1...100 {
            let name = "Some \(index) Apples"
            let item = SomeItem(id: UUID(), name: name)
            _ = try await client.createDocument(item, in: self.indexName)
        }

        // This is required for ES to settle and load the indexes to return the right results
        try await Task.sleep(for: .seconds(1))

        let results = try await client.searchDocumentsPaginated(
            from: indexName, searchTerm: "Apples", size: 20, offset: 10, type: SomeItem.self)

        #expect(results.hits.hits.count == 20)
        #expect(results.hits.hits.contains(where: { $0.source.name == "Some 11 Apples" }) == true)
        #expect(results.hits.hits.contains(where: { $0.source.name == "Some 29 Apples" }) == true)
    }

    @Test("Get Item")
    func testGetItem() async throws {
        let item = SomeItem(id: UUID(), name: "Some item")
        _ = try await client.createDocumentWithID(item, in: self.indexName)

        try await Task.sleep(for: .seconds(1))

        let retrievedItem: ESGetSingleDocumentResponse<SomeItem> = try await client.get(id: item.id, from: self.indexName)
        #expect(retrievedItem.source.name == item.name)
    }

    @Test("Bulk Update With Script")
    func testBulkUpdateWithScript() async throws {
        var items = [SomeItem]()
        for index in 1...10 {
            let name: String
            if index % 2 == 0 {
                name = "Some \(index) Apples"
            } else {
                name = "Some \(index) Bananas"
            }
            let item = SomeItem(id: UUID(), name: name, count: 0)
            _ = try await client.createDocumentWithID(item, in: self.indexName)
            items.append(item)
        }

        // This is required for ES to settle and load the indexes to return the right results
        try await Task.sleep(for: .seconds(1))

        struct ScriptBody: Codable {
            let inline: String
        }

        let scriptBody = ScriptBody(inline: "ctx._source.count = ctx._source.count += 1")

        let bulkOperation = [
            ESBulkOperation(operationType: .updateScript, index: self.indexName, id: items[0].id, document: scriptBody)
        ]

        let response = try await client.bulk(bulkOperation)
        #expect(response.items.count == 1)
        #expect(response.items.first?.update != nil)
        #expect(response.errors == false)

        let retrievedItem: ESGetSingleDocumentResponse<SomeItem> = try await client.get(id: items[0].id, from: self.indexName)
        #expect(retrievedItem.source.count == 1)
    }

    @Test("Update With Script")
    func testUpdateWithScript() async throws {
        let item = SomeItem(id: UUID(), name: "Some Item", count: 0)
        _ = try await client.createDocumentWithID(item, in: self.indexName)

        // This is required for ES to settle and load the indexes to return the right results
        try await Task.sleep(for: .seconds(1))

        struct ScriptRequest: Codable {
            let script: ScriptBody
        }

        struct ScriptBody: Codable {
            let inline: String
        }

        let scriptBody = ScriptBody(inline: "ctx._source.count = ctx._source.count += 1")
        let request = ScriptRequest(script: scriptBody)

        let response = try await client.updateDocumentWithScript(request, id: item.id, in: self.indexName)
        #expect(response.result == "updated")

        let retrievedItem: ESGetSingleDocumentResponse<SomeItem> = try await client.get(id: item.id, from: self.indexName)
        #expect(retrievedItem.source.count == 1)
    }

    @Test("Update With Non-Existent Field Script")
    func testUpdateWithNonExistentFieldScript() async throws {
        let item = SomeItem(id: UUID(), name: "Some Item")
        _ = try await client.createDocumentWithID(item, in: self.indexName)

        // This is required for ES to settle and load the indexes to return the right results
        try await Task.sleep(for: .seconds(1))

        struct ScriptRequest: Codable {
            let script: ScriptBody
        }

        struct ScriptBody: Codable {
            let inline: String
        }

        let scriptBody = ScriptBody(
            inline: "if(ctx._source.containsKey('count')) { ctx._source.count += 1 } else { ctx._source.count = 1 }")
        let request = ScriptRequest(script: scriptBody)

        let response = try await client.updateDocumentWithScript(request, id: item.id, in: self.indexName)
        #expect(response.result == "updated")

        let retrievedItem: ESGetSingleDocumentResponse<SomeItem> = try await client.get(id: item.id, from: self.indexName)
        #expect(retrievedItem.source.count == 1)
    }

    @Test("Bulk Update With Non-Existent Field Script")
    func testBulkUpdateWithNonExistentFieldScript() async throws {
        var items = [SomeItem]()
        for index in 1...10 {
            let name: String
            if index % 2 == 0 {
                name = "Some \(index) Apples"
            } else {
                name = "Some \(index) Bananas"
            }
            let item = SomeItem(id: UUID(), name: name)
            _ = try await client.createDocumentWithID(item, in: self.indexName)
            items.append(item)
        }

        // This is required for ES to settle and load the indexes to return the right results
        try await Task.sleep(for: .seconds(2))

        struct ScriptBody: Codable {
            let inline: String
        }

        let scriptBody = ScriptBody(
            inline: "if(ctx._source.containsKey('count')) { ctx._source.count += 1 } else { ctx._source.count = 1 }")

        let bulkOperation = [
            ESBulkOperation(operationType: .updateScript, index: self.indexName, id: items[0].id, document: scriptBody)
        ]

        let response = try await client.bulk(bulkOperation)
        #expect(response.items.count == 1)
        #expect(response.items.first?.update != nil)
        #expect(response.errors == false)

        let retrievedItem: ESGetSingleDocumentResponse<SomeItem> = try await client.get(id: items[0].id, from: self.indexName)
        #expect(retrievedItem.source.count == 1)
    }

    @Test("Count With Query Body")
    func testCountWithQueryBody() async throws {
        try await setupItems()

        struct SearchQuery: Encodable {
            let query: QueryBody
        }

        struct QueryBody: Encodable {
            let queryString: QueryString

            enum CodingKeys: String, CodingKey {
                case queryString = "query_string"
            }
        }

        struct QueryString: Encodable {
            let query: String
        }

        let queryString = QueryString(query: "Apples")
        let queryBody = QueryBody(queryString: queryString)
        let searchQuery = SearchQuery(query: queryBody)
        let results = try await client.searchDocumentsCount(from: indexName, query: searchQuery)
        #expect(results.count == 5)
    }

    @Test("Pagination Query With Query Body")
    func testPaginationQueryWithQueryBody() async throws {
        for index in 1...100 {
            let name = "Some \(index) Apples"
            let item = SomeItem(id: UUID(), name: name)
            _ = try await client.createDocument(item, in: self.indexName)
        }

        // This is required for ES to settle and load the indexes to return the right results
        try await Task.sleep(for: .seconds(2))

        struct QueryBody: Encodable {
            let queryString: QueryString

            enum CodingKeys: String, CodingKey {
                case queryString = "query_string"
            }
        }

        struct QueryString: Encodable {
            let query: String
        }

        let queryString = QueryString(query: "Apples")
        let queryBody = QueryBody(queryString: queryString)

        let results: ESGetMultipleDocumentsResponse<SomeItem> = try await client.searchDocumentsPaginated(
            from: indexName, queryBody: queryBody, size: 20, offset: 10
        )
        #expect(results.hits.hits.count == 20)
        #expect(results.hits.total!.value == 100)
        #expect(results.hits.hits.contains(where: { $0.source.name == "Some 11 Apples" }) == true)
        #expect(results.hits.hits.contains(where: { $0.source.name == "Some 29 Apples" }) == true)
    }

    @Test("Custom Search")
    func testCustomSearch() async throws {
        for index in 1...100 {
            let name = "Some \(index) Apples"
            let item = SomeItem(id: UUID(), name: name)
            _ = try await client.createDocument(item, in: self.indexName)
        }

        // This is required for ES to settle and load the indexes to return the right results
        try await Task.sleep(for: .seconds(2))

        struct Query: Encodable {
            let query: QueryBody
            let from: Int
            let size: Int
        }

        struct QueryBody: Encodable {
            let queryString: QueryString

            enum CodingKeys: String, CodingKey {
                case queryString = "query_string"
            }
        }

        struct QueryString: Encodable {
            let query: String
        }

        let queryString = QueryString(query: "Apples")
        let queryBody = QueryBody(queryString: queryString)
        let query = Query(query: queryBody, from: 10, size: 20)

        let results: ESGetMultipleDocumentsResponse<SomeItem> = try await client.customSearch(from: indexName, query: query)
        #expect(results.hits.hits.count == 20)
        #expect(results.hits.total!.value == 100)
        #expect(results.hits.hits.contains(where: { $0.source.name == "Some 11 Apples" }) == true)
        #expect(results.hits.hits.contains(where: { $0.source.name == "Some 29 Apples" }) == true)
    }

    @Test("Custom Search With Track Total Hits False")
    func testCustomSearchWithTrackTotalHitsFalse() async throws {
        for index in 1...100 {
            let name = "Some \(index) Apples"
            let item = SomeItem(id: UUID(), name: name)
            _ = try await client.createDocument(item, in: self.indexName)
        }

        // This is required for ES to settle and load the indexes to return the right results
        try await Task.sleep(for: .seconds(2))

        struct Query: Encodable {
            let query: QueryBody
            let from: Int
            let size: Int
            let trackTotalHits: Bool

            enum CodingKeys: String, CodingKey {
                case query
                case from
                case size
                case trackTotalHits = "track_total_hits"
            }
        }

        struct QueryBody: Encodable {
            let queryString: QueryString

            enum CodingKeys: String, CodingKey {
                case queryString = "query_string"
            }
        }

        struct QueryString: Encodable {
            let query: String
        }

        let queryString = QueryString(query: "Apples")
        let queryBody = QueryBody(queryString: queryString)
        let query = Query(query: queryBody, from: 10, size: 20, trackTotalHits: false)

        let results: ESGetMultipleDocumentsResponse<SomeItem> = try await client.customSearch(from: indexName, query: query)
        #expect(results.hits.total == nil)
        #expect(results.hits.hits.count == 20)
        #expect(results.hits.hits.contains(where: { $0.source.name == "Some 11 Apples" }) == true)
        #expect(results.hits.hits.contains(where: { $0.source.name == "Some 29 Apples" }) == true)
    }

    @Test("Custom Request")
    func testCustomRequest() async throws {
        for index in 1...100 {
            let name = "Some \(index) Apples"
            let item = SomeItem(id: UUID(), name: name, count: index)
            _ = try await client.createDocument(item, in: self.indexName)
        }

        // This is required for ES to settle and load the indexes to return the right results
        try await Task.sleep(for: .seconds(2))

        let query: [String: Any] = [
            "from": 0,
            "size": 10,
            "collapse": [
                "field": "id.keyword"
            ],
            "aggs": [
                "count-objects": [
                    "cardinality": [
                        "field": "id.keyword"
                    ]
                ],
                "count": [
                    "avg": [
                        "field": "count"
                    ]
                ],
            ],
        ]
        let queryData = try JSONSerialization.data(withJSONObject: query)

        let resultData = try await client.custom("/\(indexName)/_search", method: .get, body: queryData)

        let results = try JSONSerialization.jsonObject(with: resultData) as! [String: Any]

        let aggregations = results["aggregations"] as! [String: Any]
        let countObjects = aggregations["count-objects"] as! [String: Any]
        #expect(countObjects["value"] as! Double == 100)
        let count = aggregations["count"] as! [String: Any]
        #expect(count["value"] as! Double == 50.5)
    }

    @Test("Custom Request With Query Items")
    func testCustomRequestWithQueryItems() async throws {
        // create index
        let mappings: [String: Any] = [
            "properties": [
                "keyword_field": [
                    "type": "keyword",
                    "fields": [
                        "test": [
                            "type": "text"
                        ]
                    ],
                ]
            ]
        ]
        let settings: [String: Any] = ["number_of_shards": 3]
        let createResponse = try await client.createIndex(indexName, mappings: mappings, settings: settings)
        #expect(createResponse.acknowledged == true)

        // get indices in json format
        struct ESGetSingleIndexResponse: Decodable {
            let index: String
        }
        let resultData = try await client.custom(
            "/_cat/indices", queryItems: [URLQueryItem(name: "format", value: "json")], method: .get, body: "".data(using: .utf8)!
        )
        let results = try JSONDecoder().decode([ESGetSingleIndexResponse].self, from: resultData)
        #expect(results.map { $0.index }.first { $0 == indexName } != nil)

        // delete index
        let deleteResponse = try await client.deleteIndex(self.indexName)
        #expect(deleteResponse.acknowledged == true)
    }

    @Test("Custom Search With Data Query")
    func testCustomSearchWithDataQuery() async throws {
        for index in 1...100 {
            let name = "Some \(index) Apples"
            let item = SomeItem(id: UUID(), name: name)
            _ = try await client.createDocument(item, in: self.indexName)
        }

        // This is required for ES to settle and load the indexes to return the right results
        try await Task.sleep(for: .seconds(1))

        let query = """
            {
                "from": 10,
                "size": 20,
                "query": {
                    "query_string": {
                        "query": "Apples"
                    }
                }
            }
            """.data(using: .utf8)!

        let results: ESGetMultipleDocumentsResponse<SomeItem> = try await client.customSearch(from: indexName, query: query)
        #expect(results.hits.hits.count == 20)
        #expect(results.hits.hits.contains(where: { $0.source.name == "Some 11 Apples" }) == true)
        #expect(results.hits.hits.contains(where: { $0.source.name == "Some 29 Apples" }) == true)
    }

    // MARK: - Private
    private func setupItems() async throws {
        for index in 1...10 {
            let name: String
            if index % 2 == 0 {
                name = "Some \(index) Apples"
            } else {
                name = "Some \(index) Bananas"
            }
            let item = SomeItem(id: UUID(), name: name)
            _ = try await client.createDocument(item, in: self.indexName)
        }

        // This is required for ES to settle and load the indexes to return the right results
        try await Task.sleep(for: .seconds(1))
    }
}
