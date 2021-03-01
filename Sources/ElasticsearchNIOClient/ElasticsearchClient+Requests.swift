import Foundation
import NIO
import SotoElasticsearchService

extension ElasticsearchClient {
    public func bulk<Document: Encodable>(_ operations: [ESBulkOperation<Document>]) -> EventLoopFuture<ESBulkResponse> {
        guard operations.count > 0 else {
            return self.eventLoop.makeFailedFuture(ElasticSearchClientError(message: "No operations to perform for the bulk API"))
        }
        do {
            let url = try buildURL(path: "/_bulk")
            var bodyString = ""
            for operation in operations {
                let bulkOperationBody = BulkOperationBody(index: operation.index, id: "\(operation.id)")
                switch operation.operationType {
                case .create:
                    guard let document = operation.document else {
                        return self.eventLoop.makeFailedFuture(ElasticSearchClientError(message: "No document provided for create bulk operation"))
                    }
                    let createInfo = BulkCreate(create: bulkOperationBody)
                    let createLine = try self.jsonEncoder.encode(createInfo)
                    let dataLine = try self.jsonEncoder.encode(document)
                    guard let createLineString = String(data: createLine, encoding: .utf8), let dataLineString = String(data: dataLine, encoding: .utf8) else {
                        throw ElasticSearchClientError(message: "Failed to convert bulk data from Data to String")
                    }
                    bodyString.append("\(createLineString)\n\(dataLineString)\n")
                case .delete:
                    let deleteInfo = BulkDelete(delete: bulkOperationBody)
                    let deleteLine = try self.jsonEncoder.encode(deleteInfo)
                    guard let deleteLineString = String(data: deleteLine, encoding: .utf8) else {
                        throw ElasticSearchClientError(message: "Failed to convert bulk data from Data to String")
                    }
                    bodyString.append("\(deleteLineString)\n")
                case .index:
                    guard let document = operation.document else {
                        return self.eventLoop.makeFailedFuture(ElasticSearchClientError(message: "No document provided for create bulk operation"))
                    }
                    let indexInfo = BulkIndex(index: bulkOperationBody)
                    let indexLine = try self.jsonEncoder.encode(indexInfo)
                    let dataLine = try self.jsonEncoder.encode(document)
                    guard let indexLineString = String(data: indexLine, encoding: .utf8), let dataLineString = String(data: dataLine, encoding: .utf8) else {
                        throw ElasticSearchClientError(message: "Failed to convert bulk data from Data to String")
                    }
                    bodyString.append("\(indexLineString)\n\(dataLineString)\n")
                case .update:
                    guard let document = operation.document else {
                        return self.eventLoop.makeFailedFuture(ElasticSearchClientError(message: "No document provided for create bulk operation"))
                    }
                    let updateInfo = BulkUpdate(update: bulkOperationBody)
                    let updateLine = try self.jsonEncoder.encode(updateInfo)
                    let dataLine = try self.jsonEncoder.encode(BulkUpdateDocument(doc: document))
                    guard let updateLineString = String(data: updateLine, encoding: .utf8), let dataLineString = String(data: dataLine, encoding: .utf8) else {
                        throw ElasticSearchClientError(message: "Failed to convert bulk data from Data to String")
                    }
                    bodyString.append("\(updateLineString)\n\(dataLineString)\n")
                }
            }
            let body = AWSPayload.string(bodyString)
            var headers = HTTPHeaders()
            headers.add(name: "content-type", value: "application/x-ndjson")
            return sendRequest(url: url, method: .POST, headers: headers, body: body)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    public func createDocument<Document: Encodable>(_ document: Document, in indexName: String) -> EventLoopFuture<ESCreateDocumentResponse> {
        do {
            let url = try buildURL(path: "/\(indexName)/_doc")
            let body = try AWSPayload.data(self.jsonEncoder.encode(document))
            var headers = HTTPHeaders()
            headers.add(name: "content-type", value: "application/json")
            return sendRequest(url: url, method: .POST, headers: headers, body: body)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    public func createDocumentWithID<Document: Encodable & Identifiable>(_ document: Document, in indexName: String) -> EventLoopFuture<ESCreateDocumentResponse> {
        do {
            let url = try buildURL(path: "/\(indexName)/_doc/\(document.id)")
            let body = try AWSPayload.data(self.jsonEncoder.encode(document))
            var headers = HTTPHeaders()
            headers.add(name: "content-type", value: "application/json")
            return sendRequest(url: url, method: .POST, headers: headers, body: body)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    public func updateDocument<Document: Encodable>(_ document: Document, id: String, in indexName: String) -> EventLoopFuture<ESUpdateDocumentResponse> {
        do {
            let url = try buildURL(path: "/\(indexName)/_doc/\(id)")
            let body = try AWSPayload.data(self.jsonEncoder.encode(document))
            var headers = HTTPHeaders()
            headers.add(name: "content-type", value: "application/json")
            return sendRequest(url: url, method: .PUT, headers: headers, body: body)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    public func deleteDocument(id: String, from indexName: String) -> EventLoopFuture<ESDeleteDocumentResponse> {
        do {
            let url = try buildURL(path: "/\(indexName)/_doc/\(id)")
            return sendRequest(url: url, method: .DELETE, headers: .init())
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    public func searchDocuments<Document: Decodable>(from indexName: String, searchTerm: String, type: Document.Type = Document.self) -> EventLoopFuture<ESGetMultipleDocumentsResponse<Document>> {
        do {
            let url = try buildURL(path: "/\(indexName)/_search", queryItems: [URLQueryItem(name: "q", value: searchTerm)])
            return sendRequest(url: url, method: .GET, headers: .init())
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
            return sendRequest(url: url, method: .GET, headers: .init())
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    public func deleteIndex(_ name: String) -> EventLoopFuture<ESDeleteIndexResponse> {
        do {
            let url = try buildURL(path: "/\(name)")
            return sendRequest(url: url, method: .DELETE, headers: .init())
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    public func checkIndexExists(_ name: String) -> EventLoopFuture<Bool> {
        do {
            let url = try buildURL(path: "/\(name)")
            return signAndExecuteRequest(url: url, method: .HEAD, headers: .init(), body: .empty).flatMapThrowing { response in
                guard response.status == .ok || response.status == .notFound else {
                    throw ElasticSearchClientError(message: "Invalid response from index exists API - \(response)")
                }
                return response.status == .ok
            }
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }
}
