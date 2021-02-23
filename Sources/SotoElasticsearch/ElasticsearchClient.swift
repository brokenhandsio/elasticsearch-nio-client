import NIO
import SotoElasticsearchService
import AsyncHTTPClient
import Foundation

public struct ElasticsearchClient {

    private let client: HTTPClient
    private let awsClient: AWSClient
    private let eventLoop: EventLoop
    private let logger: Logger
    private let scheme: String
    private let host: String
    private let port: Int?
    private let username: String?
    private let password: String?

    public init(eventLoop: EventLoop, logger: Logger, awsClient: AWSClient, httpClient: HTTPClient, scheme: String = "http", host: String, port: Int? = 9200, username: String? = nil, password: String? = nil) {
        self.eventLoop = eventLoop
        self.logger = logger
        self.awsClient = awsClient
        self.client = httpClient
        self.scheme = scheme
        self.host = host
        self.port = port
        self.username = username
        self.password = password
    }

    private func sendRequest<D: Decodable>(url: String, method: HTTPMethod, headers: HTTPHeaders, body: AWSPayload = .empty) -> EventLoopFuture<D> {
        elasticSearchExecute(url: url, method: method, headers: headers, body: body).flatMapThrowing { clientResponse in
            self.logger.trace("Response: \(clientResponse)")
            if let responseBody = clientResponse.body {
                self.logger.trace("Response body: \(String(decoding: responseBody.readableBytesView, as: UTF8.self))")
            }
            switch clientResponse.status.code {
            case 200...299:
                guard var body = clientResponse.body else {
                    self.logger.debug("No body from ElasticSearch response")
                    throw ElasticSearchClientError(message: "No body from ElasticSearch response")
                }
                guard let response = try body.readJSONDecodable(D.self, length: body.readableBytes) else {
                    self.logger.debug("Failed to convert \(D.self)")
                    throw ElasticSearchClientError(message: "Failed to convert \(D.self)")
                }
                return response
            default:
                let responseBody: String
                if let body = clientResponse.body {
                    responseBody = String(decoding: body.readableBytesView, as: UTF8.self)
                } else {
                    responseBody = "Empty"
                }
                self.logger.trace("Got response status \(clientResponse.status) from ElasticSearch with response \(clientResponse) when trying \(method) request to \(url). Request body was \(body.asString() ?? "Empty") and response body was \(responseBody)")
                throw ElasticSearchClientError(message: "Bad status code from ElasticSearch")
            }
        }
    }

    private func elasticSearchExecute(url: String, method: HTTPMethod, headers: HTTPHeaders, body: AWSPayload) -> EventLoopFuture<HTTPClient.Response> {
        let es = ElasticsearchService(client: awsClient, region: .useast1)
        return es.signHeaders(
            url: URL(string: url)!,
            httpMethod: method,
            headers: headers,
            body: body
        ).flatMap { headers in
            let request = try! HTTPClient.Request(
                url: url,
                method: method,
                headers: headers,
                body: body.asByteBuffer().map { .byteBuffer($0) }
            )
            self.logger.trace("Request: \(request)")
            if let requestBody = body.asString() {
                self.logger.trace("Request body: \(requestBody)")
            }
            return self.client.execute(request: request, eventLoop: HTTPClient.EventLoopPreference.delegateAndChannel(on: self.eventLoop), logger: self.logger)
        }
    }
}

//// MARK: - Helper
extension ElasticsearchClient {
    private func baseURL(path: String, queryItems: [URLQueryItem] = []) -> String {
        var urlComponents = URLComponents()
        urlComponents.scheme = scheme
        urlComponents.host = host
        if let port = self.port {
            urlComponents.port = port
        }
        urlComponents.path = path
        urlComponents.queryItems = queryItems
        guard let url = urlComponents.url else {
            self.logger.critical("malformed url: \(urlComponents)")
            fatalError()
        }
        return url.absoluteString
    }
}

// MARK: Requests
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
