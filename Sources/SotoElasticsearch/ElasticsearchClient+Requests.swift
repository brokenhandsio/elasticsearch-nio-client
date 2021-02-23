import Foundation
import NIO
import SotoElasticsearchService
//import AsyncHTTPClient

extension ElasticsearchClient {
    func createDocument<Document: Encodable>(_ document: Document, in indexName: String) -> EventLoopFuture<ESCreateDocumentResponse> {
        let url = baseURL(path: "/\(indexName)/_doc")
        do {
            let body = try AWSPayload.data(JSONEncoder().encode(document))
            var headers = HTTPHeaders()
            headers.add(name: "content-type", value: "application/json")
            return sendRequest(url: url, method: .POST, headers: headers, body: body)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    func updateDocument<Document: Encodable>(_ document: Document, id: String, in indexName: String) -> EventLoopFuture<ESUpdateDocumentResponse> {
        let url = baseURL(path: "/\(indexName)/_doc/\(id)")
        do {
            let body = try AWSPayload.data(JSONEncoder().encode(document))
            var headers = HTTPHeaders()
            headers.add(name: "content-type", value: "application/json")
            return sendRequest(url: url, method: .PUT, headers: headers, body: body)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    func deleteDocument(id: String, from indexName: String) -> EventLoopFuture<ESDeleteDocumentResponse> {
        let url = baseURL(path: "/\(indexName)/_doc/\(id)")
        return sendRequest(url: url, method: .DELETE, headers: .init())
    }

    func searchDocuments<Document: Decodable>(from indexName: String, searchTerm: String) -> EventLoopFuture<ESGetMultipleDocumentsResponse<Document>> {
        let url = baseURL(
            path: "/\(indexName)/_search",
            queryItems: [URLQueryItem(name: "q", value: searchTerm)]
        )
        return sendRequest(url: url, method: .GET, headers: .init())
    }

    func searchDocumentsCount(from indexName: String, searchTerm: String?) -> EventLoopFuture<ESCountResponse> {
        var queryItems = [URLQueryItem]()
        if let searchTermToUse = searchTerm {
            queryItems.append(URLQueryItem(name: "q", value: searchTermToUse))
        }
        let url = baseURL(
            path: "/\(indexName)/_count",
            queryItems: queryItems
        )
        return sendRequest(url: url, method: .GET, headers: .init())
    }

    func deleteIndex(_ name: String) -> EventLoopFuture<ESDeleteIndexResponse> {
        let url = baseURL(path: "/\(name)")
        return sendRequest(url: url, method: .DELETE, headers: .init())
    }
}
