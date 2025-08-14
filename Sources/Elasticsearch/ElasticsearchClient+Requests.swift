import AsyncHTTPClient
import Foundation
import HTTPTypes

extension ElasticsearchClient {
    public func get<Document: Decodable, ID: Hashable>(
        id: ID,
        from indexName: String
    ) async throws -> ESGetSingleDocumentResponse<Document> {
        let url = try buildURL(path: "/\(indexName)/_doc/\(id)")
        return try await sendRequest(url: url, method: .get, headers: .init(), body: nil)
    }

    public func bulk<Document: Encodable, ID: Hashable>(_ operations: [ESBulkOperation<Document, ID>]) async throws -> ESBulkResponse {
        guard operations.count > 0 else {
            throw ElasticsearchClientError(message: "No operations to perform for the bulk API", status: nil)
        }
        let url = try buildURL(path: "/_bulk")
        var bodyString = ""
        for operation in operations {
            let bulkOperationBody = BulkOperationBody(index: operation.index, id: "\(operation.id)")
            switch operation.operationType {
            case .create:
                guard let document = operation.document else {
                    throw ElasticsearchClientError(message: "No document provided for create bulk operation", status: nil)
                }
                let createInfo = BulkCreate(create: bulkOperationBody)
                let createLine = try self.jsonEncoder.encode(createInfo)
                let dataLine = try self.jsonEncoder.encode(document)
                guard let createLineString = String(data: createLine, encoding: .utf8),
                    let dataLineString = String(data: dataLine, encoding: .utf8)
                else {
                    throw ElasticsearchClientError(message: "Failed to convert bulk data from Data to String", status: nil)
                }
                bodyString.append("\(createLineString)\n\(dataLineString)\n")
            case .delete:
                let deleteInfo = BulkDelete(delete: bulkOperationBody)
                let deleteLine = try self.jsonEncoder.encode(deleteInfo)
                guard let deleteLineString = String(data: deleteLine, encoding: .utf8) else {
                    throw ElasticsearchClientError(message: "Failed to convert bulk data from Data to String", status: nil)
                }
                bodyString.append("\(deleteLineString)\n")
            case .index:
                guard let document = operation.document else {
                    throw ElasticsearchClientError(message: "No document provided for index bulk operation", status: nil)
                }
                let indexInfo = BulkIndex(index: bulkOperationBody)
                let indexLine = try self.jsonEncoder.encode(indexInfo)
                let dataLine = try self.jsonEncoder.encode(document)
                guard let indexLineString = String(data: indexLine, encoding: .utf8),
                    let dataLineString = String(data: dataLine, encoding: .utf8)
                else {
                    throw ElasticsearchClientError(message: "Failed to convert bulk data from Data to String", status: nil)
                }
                bodyString.append("\(indexLineString)\n\(dataLineString)\n")
            case .update:
                guard let document = operation.document else {
                    throw ElasticsearchClientError(message: "No document provided for update bulk operation", status: nil)
                }
                let updateInfo = BulkUpdate(update: bulkOperationBody)
                let updateLine = try self.jsonEncoder.encode(updateInfo)
                let dataLine = try self.jsonEncoder.encode(BulkUpdateDocument(doc: document))
                guard let updateLineString = String(data: updateLine, encoding: .utf8),
                    let dataLineString = String(data: dataLine, encoding: .utf8)
                else {
                    throw ElasticsearchClientError(message: "Failed to convert bulk data from Data to String", status: nil)
                }
                bodyString.append("\(updateLineString)\n\(dataLineString)\n")
            case .updateScript:
                guard let document = operation.document else {
                    throw ElasticsearchClientError(message: "No script provided for update script bulk operation", status: nil)
                }
                let updateInfo = BulkUpdateScript(update: bulkOperationBody)
                let updateLine = try self.jsonEncoder.encode(updateInfo)
                let dataLine = try self.jsonEncoder.encode(BulkUpdateScriptDocument(script: document))
                guard let updateLineString = String(data: updateLine, encoding: .utf8),
                    let dataLineString = String(data: dataLine, encoding: .utf8)
                else {
                    throw ElasticsearchClientError(message: "Failed to convert bulk data from Data to String", status: nil)
                }
                bodyString.append("\(updateLineString)\n\(dataLineString)\n")
            }
        }
        var headers = HTTPFields()
        headers[.contentType] = "application/x-ndjson"
        return try await sendRequest(url: url, method: .post, headers: headers, body: .bytes(.init(string: bodyString)))
    }

    public func createDocument<Document: Encodable>(
        _ document: Document,
        in indexName: String
    ) async throws -> ESCreateDocumentResponse<String> {
        let url = try buildURL(path: "/\(indexName)/_doc")
        let body = try self.jsonEncoder.encode(document)
        var headers = HTTPFields()
        headers[.contentType] = "application/json"
        return try await sendRequest(url: url, method: .post, headers: headers, body: .bytes(body))
    }

    public func createDocumentWithID<Document: Encodable & Identifiable>(
        _ document: Document,
        in indexName: String
    ) async throws -> ESCreateDocumentResponse<Document.ID> {
        let url = try buildURL(path: "/\(indexName)/_doc/\(document.id)")
        let body = try self.jsonEncoder.encode(document)
        var headers = HTTPFields()
        headers[.contentType] = "application/json"
        return try await sendRequest(url: url, method: .post, headers: headers, body: .bytes(body))
    }

    public func updateDocument<Document: Encodable, ID: Hashable>(
        _ document: Document, id: ID, in indexName: String
    ) async throws -> ESUpdateDocumentResponse<ID> {
        let url = try buildURL(path: "/\(indexName)/_doc/\(id)")
        let body = try self.jsonEncoder.encode(document)
        var headers = HTTPFields()
        headers[.contentType] = "application/json"
        return try await sendRequest(url: url, method: .put, headers: headers, body: .bytes(body))
    }

    public func updateDocument<Document: Encodable & Identifiable>(
        _ document: Document, in indexName: String
    ) async throws -> ESUpdateDocumentResponse<Document.ID> {
        let url = try buildURL(path: "/\(indexName)/_doc/\(document.id)")
        let body = try self.jsonEncoder.encode(document)
        var headers = HTTPFields()
        headers[.contentType] = "application/json"
        return try await sendRequest(url: url, method: .put, headers: headers, body: .bytes(body))
    }

    public func updateDocumentWithScript<Script: Encodable, ID: Hashable>(
        _ script: Script, id: ID, in indexName: String
    ) async throws -> ESUpdateDocumentResponse<ID> {
        let url = try buildURL(path: "/\(indexName)/_update/\(id)")
        let body = try self.jsonEncoder.encode(script)
        var headers = HTTPFields()
        headers[.contentType] = "application/json"
        return try await sendRequest(url: url, method: .post, headers: headers, body: .bytes(body))
    }

    public func deleteDocument<ID: Hashable>(
        id: ID, from indexName: String
    ) async throws -> ESDeleteDocumentResponse {
        let url = try buildURL(path: "/\(indexName)/_doc/\(id)")
        return try await sendRequest(url: url, method: .delete, headers: .init(), body: nil)
    }

    public func searchDocuments<Document: Decodable>(
        from indexName: String, searchTerm: String, type: Document.Type = Document.self
    ) async throws -> ESGetMultipleDocumentsResponse<Document> {
        let url = try buildURL(path: "/\(indexName)/_search", queryItems: [URLQueryItem(name: "q", value: searchTerm)])
        return try await sendRequest(url: url, method: .get, headers: .init(), body: nil)
    }

    public func searchDocumentsCount(
        from indexName: String, searchTerm: String?
    ) async throws -> ESCountResponse {
        var queryItems = [URLQueryItem]()
        if let searchTermToUse = searchTerm {
            queryItems.append(URLQueryItem(name: "q", value: searchTermToUse))
        }
        let url = try buildURL(path: "/\(indexName)/_count", queryItems: queryItems)
        return try await sendRequest(url: url, method: .get, headers: .init(), body: nil)
    }

    public func searchDocumentsPaginated<Document: Decodable>(
        from indexName: String, searchTerm: String, size: Int = 10, offset: Int = 0, type: Document.Type = Document.self
    ) async throws -> ESGetMultipleDocumentsResponse<Document> {
        let url = try buildURL(path: "/\(indexName)/_search")
        let query = ESSearchRequest(searchQuery: searchTerm, size: size, from: offset)
        let body = try self.jsonEncoder.encode(query)
        var headers = HTTPFields()
        headers[.contentType] = "application/json"
        return try await sendRequest(url: url, method: .get, headers: headers, body: .bytes(body))
    }

    public func searchDocumentsCount<Query: Encodable>(
        from indexName: String, query: Query
    ) async throws -> ESCountResponse {
        let url = try buildURL(path: "/\(indexName)/_count")
        let body = try self.jsonEncoder.encode(query)
        var headers = HTTPFields()
        headers[.contentType] = "application/json"
        return try await sendRequest(url: url, method: .get, headers: headers, body: .bytes(body))
    }

    public func searchDocumentsPaginated<Document: Decodable, QueryBody: Encodable>(
        from indexName: String, queryBody: QueryBody, size: Int = 10, offset: Int = 0, type: Document.Type = Document.self
    ) async throws -> ESGetMultipleDocumentsResponse<Document> {
        let url = try buildURL(path: "/\(indexName)/_search")
        let queryBody = ESComplexSearchRequest(from: offset, size: size, query: queryBody)
        let body = try self.jsonEncoder.encode(queryBody)
        var headers = HTTPFields()
        headers[.contentType] = "application/json"
        return try await sendRequest(url: url, method: .get, headers: headers, body: .bytes(body))
    }

    public func customSearch<Document: Decodable, Query: Encodable>(
        from indexName: String, query: Query, type: Document.Type = Document.self
    ) async throws -> ESGetMultipleDocumentsResponse<Document> {
        let body = try self.jsonEncoder.encode(query)
        return try await sendCustomRequest(from: indexName, body: body)
    }

    public func customSearch<Document: Decodable>(
        from indexName: String, query: Data, type: Document.Type = Document.self
    ) async throws -> ESGetMultipleDocumentsResponse<Document> {
        return try await sendCustomRequest(from: indexName, body: query)
    }

    private func sendCustomRequest<Document: Decodable>(
        from indexName: String, body: Data
    ) async throws -> ESGetMultipleDocumentsResponse<Document> {
        let url = try buildURL(path: "/\(indexName)/_search")
        var headers = HTTPFields()
        headers[.contentType] = "application/json"
        return try await sendRequest(url: url, method: .get, headers: headers, body: .bytes(body))
    }

    public func createIndex(
        _ indexName: String, mappings: [String: Any], settings: [String: Any]
    ) async throws -> ESAcknowledgedResponse {
        let url = try buildURL(path: "/\(indexName)")
        let jsonBase: [String: Any] = [
            "mappings": mappings,
            "settings": settings,
        ]
        let body = try JSONSerialization.data(withJSONObject: jsonBase)
        var headers = HTTPFields()
        headers[.contentType] = "application/json"
        return try await sendRequest(url: url, method: .put, headers: headers, body: .bytes(body))
    }

    public func deleteIndex(
        _ name: String
    ) async throws -> ESAcknowledgedResponse {
        let url = try buildURL(path: "/\(name)")
        return try await sendRequest(url: url, method: .delete, headers: .init(), body: nil)
    }

    public func checkIndexExists(
        _ name: String
    ) async throws -> Bool {
        let url = try buildURL(path: "/\(name)")
        let response = try await requester.executeRequest(url: url, method: .head, headers: .init(), body: nil)
        guard response.status == .ok || response.status == .notFound else {
            throw ElasticsearchClientError(
                message: "Invalid response from index exists API - \(response)", status: .init(code: Int(response.status.code)))
        }
        return response.status == .ok
    }

    public func custom(
        _ path: String, queryItems: [URLQueryItem] = [], method: HTTPRequest.Method, body: Data
    ) async throws -> Data {
        let url = try buildURL(path: path, queryItems: queryItems)
        var headers = HTTPFields()
        headers[.contentType] = "application/json"
        let response = try await sendRequest(url: url, method: method, headers: headers, body: .bytes(body))
        let responseBody = try await response.body.collect(upTo: 1024 * 1024)
        return Data(buffer: responseBody)
    }
}
