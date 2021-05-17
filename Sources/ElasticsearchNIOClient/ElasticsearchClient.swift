import NIO
import AsyncHTTPClient
import Foundation
import Logging
import NIOHTTP1

public struct ElasticsearchClient {

    let requester: ElasticsearchRequester
    let eventLoop: EventLoop
    let logger: Logger
    let scheme: String
    let host: String
    let port: Int?
    let username: String?
    let password: String?
    let jsonEncoder: JSONEncoder
    let jsonDecoder: JSONDecoder

    public init(httpClient: HTTPClient, eventLoop: EventLoop, logger: Logger, scheme: String = "http", host: String, port: Int? = 9200, username: String? = nil, password: String? = nil, jsonEncoder: JSONEncoder = JSONEncoder(), jsonDecoder: JSONDecoder = JSONDecoder()) {
        self.eventLoop = eventLoop
        self.logger = logger
        self.scheme = scheme
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.jsonEncoder = jsonEncoder
        self.jsonDecoder = jsonDecoder
        self.requester = HTTPClientElasticsearchRequester(eventLoop: eventLoop, logger: logger, client: httpClient)
    }

    public init(requester: ElasticsearchRequester, eventLoop: EventLoop, logger: Logger, scheme: String = "http", host: String, port: Int? = 9200, username: String? = nil, password: String? = nil, jsonEncoder: JSONEncoder = JSONEncoder(), jsonDecoder: JSONDecoder = JSONDecoder()) {
        self.requester = requester
        self.eventLoop = eventLoop
        self.logger = logger
        self.scheme = scheme
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.jsonEncoder = jsonEncoder
        self.jsonDecoder = jsonDecoder
    }

    func sendRequest<D: Decodable>(url: String, method: HTTPMethod, headers: HTTPHeaders, body: ByteBuffer?) -> EventLoopFuture<D> {
        requester.executeRequest(url: url, method: method, headers: headers, body: body).flatMapThrowing { clientResponse in
            self.logger.trace("Response: \(clientResponse)")
            if let responseBody = clientResponse.body {
                self.logger.trace("Response body: \(String(decoding: responseBody.readableBytesView, as: UTF8.self))")
            }
            switch clientResponse.status.code {
            case 200...299:
                guard var body = clientResponse.body else {
                    self.logger.debug("No body from ElasticSearch response")
                    throw ElasticSearchClientError(message: "No body from ElasticSearch response", status: clientResponse.status.code)
                }
                guard let response = try body.readJSONDecodable(D.self, decoder: jsonDecoder, length: body.readableBytes) else {
                    self.logger.debug("Failed to convert \(D.self)")
                    throw ElasticSearchClientError(message: "Failed to convert \(D.self)", status: clientResponse.status.code)
                }
                return response
            default:
                let requestBody: String
                if let body = body {
                    requestBody = String(buffer: body)
                } else {
                    requestBody = ""
                }
                let responseBody: String
                if let body = clientResponse.body {
                    responseBody = String(decoding: body.readableBytesView, as: UTF8.self)
                } else {
                    responseBody = "Empty"
                }
                self.logger.trace("Got response status \(clientResponse.status) from ElasticSearch with response \(clientResponse) when trying \(method) request to \(url). Request body was \(requestBody) and response body was \(responseBody)")
                throw ElasticSearchClientError(message: "Bad status code from ElasticSearch", status: clientResponse.status.code)
            }
        }
    }
}

//// MARK: - Helper
extension ElasticsearchClient {
    func buildURL(path: String, queryItems: [URLQueryItem] = []) throws -> String {
        var urlComponents = URLComponents()
        urlComponents.scheme = scheme
        urlComponents.host = host
        if let port = self.port {
            urlComponents.port = port
        }
        urlComponents.path = path
        urlComponents.queryItems = queryItems
        guard let url = urlComponents.url else {
            self.logger.debug("malformed url: \(urlComponents)")
            throw ElasticSearchClientError(message: "malformed url: \(urlComponents)", status: nil)
        }
        return url.absoluteString
    }
}
