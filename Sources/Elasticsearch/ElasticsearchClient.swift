import AsyncHTTPClient
import Foundation
import HTTPTypes
import Logging
import NIOFoundationCompat

public struct ElasticsearchClient {
    public static let defaultPort = 9200
    public static let allowedUrlSchemes = ["http", "https"]

    let requester: ElasticsearchRequester
    let logger: Logger
    let scheme: String
    let host: String
    let port: Int?
    let username: String?
    let password: String?
    let jsonEncoder: JSONEncoder
    let jsonDecoder: JSONDecoder

    public init(
        httpClient: HTTPClient,
        logger: Logger,
        url string: String,
        username: String? = nil,
        password: String? = nil,
        jsonEncoder: JSONEncoder = JSONEncoder(),
        jsonDecoder: JSONDecoder = JSONDecoder()
    ) throws {
        guard let url = URL(string: string) else { throw ValidationError.invalidURLString }

        try self.init(
            httpClient: httpClient,
            logger: logger,
            url: url,
            username: username,
            password: password,
            jsonEncoder: jsonEncoder,
            jsonDecoder: jsonDecoder
        )
    }

    public init(
        httpClient: HTTPClient,
        logger: Logger,
        url: URL,
        username: String? = nil,
        password: String? = nil,
        jsonEncoder: JSONEncoder = JSONEncoder(),
        jsonDecoder: JSONDecoder = JSONDecoder()
    ) throws {
        guard
            let scheme = url.scheme,
            !scheme.isEmpty
        else {
            throw ValidationError.missingURLScheme
        }

        guard Self.allowedUrlSchemes.contains(scheme) else {
            throw ValidationError.invalidURLScheme
        }

        guard let host = url.host, !host.isEmpty else { throw ValidationError.missingURLHost }

        try self.init(
            requester: HTTPClientElasticsearchRequester(logger: logger, username: username, password: password, client: httpClient),
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

    public init(
        httpClient: HTTPClient,
        logger: Logger,
        scheme: String? = nil,
        host: String,
        port: Int? = defaultPort,
        username: String? = nil,
        password: String? = nil,
        jsonEncoder: JSONEncoder = JSONEncoder(),
        jsonDecoder: JSONDecoder = JSONDecoder()
    ) throws {
        try self.init(
            requester: HTTPClientElasticsearchRequester(logger: logger, username: username, password: password, client: httpClient),
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

    public init(
        requester: ElasticsearchRequester,
        logger: Logger,
        scheme: String? = nil,
        host: String,
        port: Int? = defaultPort,
        username: String? = nil,
        password: String? = nil,
        jsonEncoder: JSONEncoder = JSONEncoder(),
        jsonDecoder: JSONDecoder = JSONDecoder()
    ) throws {
        self.requester = requester
        self.logger = logger
        if let scheme = scheme {
            guard Self.allowedUrlSchemes.contains(scheme) else {
                throw ValidationError.invalidURLScheme
            }
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

    func sendRequest(
        url: String,
        method: HTTPRequest.Method,
        headers: HTTPFields,
        body: HTTPClientRequest.Body?
    ) async throws -> HTTPClientResponse.Body {
        let clientResponse = try await requester.executeRequest(url: url, method: method, headers: headers, body: body)
        self.logger.trace("Response: \(clientResponse)")

        switch clientResponse.status.code {
        case 200...299:
            return clientResponse.body
        default:
            let requestBody = try await body?.collect(upTo: 1024 * 1024) ?? .init()
            let responseBody = try await clientResponse.body.collect(upTo: 1024 * 1024)
            self.logger.trace(
                "Got response status \(clientResponse.status) from Elasticsearch with response \(clientResponse) when trying \(method) request to \(url). Request body was \(requestBody) and response body was \(responseBody)"
            )
            throw ElasticsearchClientError(
                message: "Bad status code from Elasticsearch", status: .init(code: Int(clientResponse.status.code)))
        }
    }

    func sendRequest<D: Decodable>(
        url: String,
        method: HTTPRequest.Method,
        headers: HTTPFields,
        body: HTTPClientRequest.Body?
    ) async throws -> D {
        let body = try await sendRequest(url: url, method: method, headers: headers, body: body)
        let bodyBytes = try await body.collect(upTo: 1024 * 1024)

        let response: D
        do {
            response = try jsonDecoder.decode(D.self, from: bodyBytes)
        } catch {
            let string = String(buffer: bodyBytes)
            self.logger.debug("Failed to convert \(D.self). Bytes: \(string)")
            throw ElasticsearchClientError(message: "Failed to convert \(D.self)", status: nil)
        }
        return response
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
            throw ElasticsearchClientError(message: "malformed url: \(urlComponents)", status: nil)
        }
        return url.absoluteString
    }
}
