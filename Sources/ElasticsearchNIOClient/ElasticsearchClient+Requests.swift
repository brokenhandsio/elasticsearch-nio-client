import Foundation
import NIO
import NIOHTTP1
import NIOFoundationCompat

extension ElasticsearchClient {
    public func get<Document: Decodable, ID: Hashable>(id: ID, from indexName: String) -> EventLoopFuture<ESGetSingleDocumentResponse<Document>> {
        do {
            let url = try buildURL(path: "/\(indexName)/_doc/\(id)")
            return sendRequest(url: url, method: .GET, headers: .init(), body: nil)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    public func bulk<Document: Encodable, ID: Hashable>(_ operations: [ESBulkOperation<Document, ID>]) -> EventLoopFuture<ESBulkResponse> {
        guard operations.count > 0 else {
            return self.eventLoop.makeFailedFuture(ElasticSearchClientError(message: "No operations to perform for the bulk API", status: nil))
        }
        do {
            let url = try buildURL(path: "/_bulk")
            var bodyString = ""
            for operation in operations {
                let bulkOperationBody = BulkOperationBody(index: operation.index, id: "\(operation.id)")
                switch operation.operationType {
                case .create:
                    guard let document = operation.document else {
                        return self.eventLoop.makeFailedFuture(ElasticSearchClientError(message: "No document provided for create bulk operation", status: nil))
                    }
                    let createInfo = BulkCreate(create: bulkOperationBody)
                    let createLine = try self.jsonEncoder.encode(createInfo)
                    let dataLine = try self.jsonEncoder.encode(document)
                    guard let createLineString = String(data: createLine, encoding: .utf8), let dataLineString = String(data: dataLine, encoding: .utf8) else {
                        throw ElasticSearchClientError(message: "Failed to convert bulk data from Data to String", status: nil)
                    }
                    bodyString.append("\(createLineString)\n\(dataLineString)\n")
                case .delete:
                    let deleteInfo = BulkDelete(delete: bulkOperationBody)
                    let deleteLine = try self.jsonEncoder.encode(deleteInfo)
                    guard let deleteLineString = String(data: deleteLine, encoding: .utf8) else {
                        throw ElasticSearchClientError(message: "Failed to convert bulk data from Data to String", status: nil)
                    }
                    bodyString.append("\(deleteLineString)\n")
                case .index:
                    guard let document = operation.document else {
                        return self.eventLoop.makeFailedFuture(ElasticSearchClientError(message: "No document provided for index bulk operation", status: nil))
                    }
                    let indexInfo = BulkIndex(index: bulkOperationBody)
                    let indexLine = try self.jsonEncoder.encode(indexInfo)
                    let dataLine = try self.jsonEncoder.encode(document)
                    guard let indexLineString = String(data: indexLine, encoding: .utf8), let dataLineString = String(data: dataLine, encoding: .utf8) else {
                        throw ElasticSearchClientError(message: "Failed to convert bulk data from Data to String", status: nil)
                    }
                    bodyString.append("\(indexLineString)\n\(dataLineString)\n")
                case .update:
                    guard let document = operation.document else {
                        return self.eventLoop.makeFailedFuture(ElasticSearchClientError(message: "No document provided for update bulk operation", status: nil))
                    }
                    let updateInfo = BulkUpdate(update: bulkOperationBody)
                    let updateLine = try self.jsonEncoder.encode(updateInfo)
                    let dataLine = try self.jsonEncoder.encode(BulkUpdateDocument(doc: document))
                    guard let updateLineString = String(data: updateLine, encoding: .utf8), let dataLineString = String(data: dataLine, encoding: .utf8) else {
                        throw ElasticSearchClientError(message: "Failed to convert bulk data from Data to String", status: nil)
                    }
                    bodyString.append("\(updateLineString)\n\(dataLineString)\n")
                case .updateScript:
                    guard let document = operation.document else {
                        return self.eventLoop.makeFailedFuture(ElasticSearchClientError(message: "No script provided for update script bulk operation", status: nil))
                    }
                    let updateInfo = BulkUpdateScript(update: bulkOperationBody)
                    let updateLine = try self.jsonEncoder.encode(updateInfo)
                    let dataLine = try self.jsonEncoder.encode(BulkUpdateScriptDocument(script: document))
                    guard let updateLineString = String(data: updateLine, encoding: .utf8), let dataLineString = String(data: dataLine, encoding: .utf8) else {
                        throw ElasticSearchClientError(message: "Failed to convert bulk data from Data to String", status: nil)
                    }
                    bodyString.append("\(updateLineString)\n\(dataLineString)\n")
                }
            }
            let body = ByteBuffer(string: bodyString)
            var headers = HTTPHeaders()
            headers.add(name: "content-type", value: "application/x-ndjson")
            return sendRequest(url: url, method: .POST, headers: headers, body: body)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    public func createDocument<Document: Encodable>(_ document: Document, in indexName: String) -> EventLoopFuture<ESCreateDocumentResponse<String>> {
        do {
            let url = try buildURL(path: "/\(indexName)/_doc")
            let body = try ByteBuffer(data: self.jsonEncoder.encode(document))
            var headers = HTTPHeaders()
            headers.add(name: "content-type", value: "application/json")
            return sendRequest(url: url, method: .POST, headers: headers, body: body)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    public func createDocumentWithID<Document: Encodable & Identifiable>(_ document: Document, in indexName: String) -> EventLoopFuture<ESCreateDocumentResponse<Document.ID>> {
        do {
            let url = try buildURL(path: "/\(indexName)/_doc/\(document.id)")
            let body = try ByteBuffer(data: self.jsonEncoder.encode(document))
            var headers = HTTPHeaders()
            headers.add(name: "content-type", value: "application/json")
            return sendRequest(url: url, method: .POST, headers: headers, body: body)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    public func updateDocument<Document: Encodable, ID: Hashable>(_ document: Document, id: ID, in indexName: String) -> EventLoopFuture<ESUpdateDocumentResponse<ID>> {
        do {
            let url = try buildURL(path: "/\(indexName)/_doc/\(id)")
            let body = try ByteBuffer(data: self.jsonEncoder.encode(document))
            var headers = HTTPHeaders()
            headers.add(name: "content-type", value: "application/json")
            return sendRequest(url: url, method: .PUT, headers: headers, body: body)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    public func updateDocument<Document: Encodable & Identifiable>(_ document: Document, in indexName: String) -> EventLoopFuture<ESUpdateDocumentResponse<Document.ID>> {
        do {
            let url = try buildURL(path: "/\(indexName)/_doc/\(document.id)")
            let body = try ByteBuffer(data: self.jsonEncoder.encode(document))
            var headers = HTTPHeaders()
            headers.add(name: "content-type", value: "application/json")
            return sendRequest(url: url, method: .PUT, headers: headers, body: body)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    public func updateDocumentWithScript<Script: Encodable, ID: Hashable>(_ script: Script, id: ID, in indexName: String) -> EventLoopFuture<ESUpdateDocumentResponse<ID>> {
        do {
            let url = try buildURL(path: "/\(indexName)/_update/\(id)")
            let body = try ByteBuffer(data: self.jsonEncoder.encode(script))
            var headers = HTTPHeaders()
            headers.add(name: "content-type", value: "application/json")
            return sendRequest(url: url, method: .POST, headers: headers, body: body)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    public func deleteDocument<ID: Hashable>(id: ID, from indexName: String) -> EventLoopFuture<ESDeleteDocumentResponse> {
        do {
            let url = try buildURL(path: "/\(indexName)/_doc/\(id)")
            return sendRequest(url: url, method: .DELETE, headers: .init(), body: nil)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    public func searchDocuments<Document: Decodable>(from indexName: String, searchTerm: String, type: Document.Type = Document.self) -> EventLoopFuture<ESGetMultipleDocumentsResponse<Document>> {
        do {
            let url = try buildURL(path: "/\(indexName)/_search", queryItems: [URLQueryItem(name: "q", value: searchTerm)])
            return sendRequest(url: url, method: .GET, headers: .init(), body: nil)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    public func searchDocumentsCount(from indexName: String, searchTerm: String?) -> EventLoopFuture<ESCountResponse> {
        do {
            var queryItems = [URLQueryItem]()
            if let searchTermToUse = searchTerm {
                queryItems.append(URLQueryItem(name: "q", value: searchTermToUse))
            }
            let url = try buildURL(path: "/\(indexName)/_count", queryItems: queryItems)
            return sendRequest(url: url, method: .GET, headers: .init(), body: nil)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    public func searchDocumentsPaginated<Document: Decodable>(from indexName: String, searchTerm: String, size: Int = 10, offset: Int = 0, type: Document.Type = Document.self) -> EventLoopFuture<ESGetMultipleDocumentsResponse<Document>> {
        do {
            let url = try buildURL(path: "/\(indexName)/_search")
            let query = ESSearchRequest(searchQuery: searchTerm, size: size, from: offset)
            let body = try ByteBuffer(data: self.jsonEncoder.encode(query))
            var headers = HTTPHeaders()
            headers.add(name: "content-type", value: "application/json")
            return sendRequest(url: url, method: .GET, headers: headers, body: body)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    public func searchDocumentsCount<Query: Encodable>(from indexName: String, query: Query) -> EventLoopFuture<ESCountResponse> {
        do {
            let url = try buildURL(path: "/\(indexName)/_count")
            let body = try ByteBuffer(data: self.jsonEncoder.encode(query))
            var headers = HTTPHeaders()
            headers.add(name: "content-type", value: "application/json")
            return sendRequest(url: url, method: .GET, headers: headers, body: body)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    public func searchDocumentsPaginated<Document: Decodable, QueryBody: Encodable>(from indexName: String, queryBody: QueryBody, size: Int = 10, offset: Int = 0, type: Document.Type = Document.self) -> EventLoopFuture<ESGetMultipleDocumentsResponse<Document>> {
        do {
            let url = try buildURL(path: "/\(indexName)/_search")
            let queryBody = ESComplexSearchRequest(from: offset, size: size, query: queryBody)
            let body = try ByteBuffer(data: self.jsonEncoder.encode(queryBody))
            var headers = HTTPHeaders()
            headers.add(name: "content-type", value: "application/json")
            return sendRequest(url: url, method: .GET, headers: headers, body: body)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    public func customSearch<Document: Decodable, Query: Encodable>(from indexName: String, query: Query, type: Document.Type = Document.self) -> EventLoopFuture<ESGetMultipleDocumentsResponse<Document>> {
        do {
            let body = try ByteBuffer(data: self.jsonEncoder.encode(query))
            return sendCustomRequest(from: indexName, body: body)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }
    public func customSearch<Document: Decodable>(from indexName: String, query: Data, type: Document.Type = Document.self) -> EventLoopFuture<ESGetMultipleDocumentsResponse<Document>> {
        let body = ByteBuffer(data: query)
        return sendCustomRequest(from: indexName, body: body)
    }
    private func sendCustomRequest<Document: Decodable>(from indexName: String, body: ByteBuffer) -> EventLoopFuture<ESGetMultipleDocumentsResponse<Document>> {
        do {
            let url = try buildURL(path: "/\(indexName)/_search")
            var headers = HTTPHeaders()
            headers.add(name: "content-type", value: "application/json")
            return sendRequest(url: url, method: .GET, headers: headers, body: body)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    public func createIndex(_ indexName: String, mappings: [String: Any], settings: [String: Any]) -> EventLoopFuture<ESAcknowledgedResponse> {
        do {
            let url = try buildURL(path: "/\(indexName)")
            let jsonBase: [String: Any] = [
                "mappings": mappings,
                "settings": settings
            ]
            let body = try ByteBuffer(data: JSONSerialization.data(withJSONObject: jsonBase))
            var headers = HTTPHeaders()
            headers.add(name: "content-type", value: "application/json")
            return sendRequest(url: url, method: .PUT, headers: headers, body: body)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    public func deleteIndex(_ name: String) -> EventLoopFuture<ESAcknowledgedResponse> {
        do {
            let url = try buildURL(path: "/\(name)")
            return sendRequest(url: url, method: .DELETE, headers: .init(), body: nil)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    public func checkIndexExists(_ name: String) -> EventLoopFuture<Bool> {
        do {
            let url = try buildURL(path: "/\(name)")
            return requester.executeRequest(url: url, method: .HEAD, headers: .init(), body: nil).flatMapThrowing { response in
                guard response.status == .ok || response.status == .notFound else {
                    throw ElasticSearchClientError(message: "Invalid response from index exists API - \(response)", status: response.status)
                }
                return response.status == .ok
            }
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    public func custom(_ path: String, queryItems: [URLQueryItem] = [], method: HTTPMethod, body: Data) -> EventLoopFuture<Data> {
        do {
            let url = try buildURL(path: path, queryItems: queryItems)
            let body = ByteBuffer(data: body)
            var headers = HTTPHeaders()
            headers.add(name: "content-type", value: "application/json")
            return sendRequest(url: url, method: method, headers: headers, body: body).flatMapThrowing { return Data(buffer: $0) }
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }
}
