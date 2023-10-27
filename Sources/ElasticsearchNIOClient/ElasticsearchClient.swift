import NIO
import NIOFoundationCompat
import AsyncHTTPClient
import Foundation
import Logging
import NIOHTTP1

public struct ElasticsearchClient {
    
    public static let defaultPort = 9200
    public static let allowedUrlSchemes = ["http", "https"]
    
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
    
    public init(httpClient: HTTPClient, eventLoop: EventLoop, logger: Logger, url string: String, username: String? = nil, password: String? = nil, jsonEncoder: JSONEncoder = JSONEncoder(), jsonDecoder: JSONDecoder = JSONDecoder()) throws {
        guard let url = URL(string: string) else { throw ValidationError.invalidURLString }
        try self.init(
            httpClient: httpClient,
            eventLoop: eventLoop,
            logger: logger,
            url: url,
            username: username,
            password: password,
            jsonEncoder: jsonEncoder,
            jsonDecoder: jsonDecoder
        )
    }

    public init(httpClient: HTTPClient, eventLoop: EventLoop, logger: Logger, url: URL, username: String? = nil, password: String? = nil, jsonEncoder: JSONEncoder = JSONEncoder(), jsonDecoder: JSONDecoder = JSONDecoder()) throws {
        guard
            let scheme = url.scheme,
            !scheme.isEmpty
        else { throw ValidationError.missingURLScheme }
        guard Self.allowedUrlSchemes.contains(scheme) else { throw ValidationError.invalidURLScheme }
        guard let host = url.host, !host.isEmpty else { throw ValidationError.missingURLHost }
        
        try self.init(
            requester: HTTPClientElasticsearchRequester(eventLoop: eventLoop, logger: logger, username: username, password: password, client: httpClient),
            eventLoop: eventLoop,
            logger: logger,
            scheme: scheme,
            host: host,
            port: url.port,
            username: username,
            password: password,
            jsonEncoder: jsonEncoder,
            jsonDecoder: jsonDecoder
        )
    }
    
    public init(httpClient: HTTPClient, eventLoop: EventLoop, logger: Logger, scheme: String? = nil, host: String, port: Int? = defaultPort, username: String? = nil, password: String? = nil, jsonEncoder: JSONEncoder = JSONEncoder(), jsonDecoder: JSONDecoder = JSONDecoder()) throws {
        try self.init(
            requester: HTTPClientElasticsearchRequester(eventLoop: eventLoop, logger: logger, username: username, password: password, client: httpClient),
            eventLoop: eventLoop,
            logger: logger,
            scheme: scheme,
            host: host,
            port: port,
            username: username,
            password: password,
            jsonEncoder: jsonEncoder,
            jsonDecoder: jsonDecoder
        )
    }

    public init(requester: ElasticsearchRequester, eventLoop: EventLoop, logger: Logger, scheme: String? = nil, host: String, port: Int? = defaultPort, username: String? = nil, password: String? = nil, jsonEncoder: JSONEncoder = JSONEncoder(), jsonDecoder: JSONDecoder = JSONDecoder()) throws {
        self.requester = requester
        self.eventLoop = eventLoop
        self.logger = logger
        if let scheme = scheme {
            guard Self.allowedUrlSchemes.contains(scheme) else { throw ValidationError.invalidURLScheme }
            self.scheme = scheme
        } else {
            self.scheme = Self.allowedUrlSchemes.first!
        }
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.jsonEncoder = jsonEncoder
        self.jsonDecoder = jsonDecoder
    }

    func sendRequest(url: String, method: HTTPMethod, headers: HTTPHeaders, body: ByteBuffer?) -> EventLoopFuture<ByteBuffer> {
        requester.executeRequest(url: url, method: method, headers: headers, body: body).flatMapThrowing { clientResponse in
            self.logger.trace("Response: \(clientResponse)")
            if let responseBody = clientResponse.body {
                self.logger.trace("Response body: \(String(decoding: responseBody.readableBytesView, as: UTF8.self))")
            }
            switch clientResponse.status.code {
            case 200...299:
                guard let body = clientResponse.body else {
                    self.logger.debug("No body from ElasticSearch response")
                    throw ElasticSearchClientError(message: "No body from ElasticSearch response", status: clientResponse.status)
                }
                return body
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
                throw ElasticSearchClientError(message: "Bad status code from ElasticSearch", status: clientResponse.status)
            }
        }
    }

    func sendRequest<D: Decodable>(url: String, method: HTTPMethod, headers: HTTPHeaders, body: ByteBuffer?) -> EventLoopFuture<D> {
        sendRequest(url: url, method: method, headers: headers, body: body).flatMapThrowing { body in
            var body = body
            guard let response = try body.readJSONDecodable(D.self, decoder: jsonDecoder, length: body.readableBytes) else {
                self.logger.debug("Failed to convert \(D.self)")
                throw ElasticSearchClientError(message: "Failed to convert \(D.self)", status: nil)
            }
            return response
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
