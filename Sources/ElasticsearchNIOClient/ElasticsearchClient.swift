import NIO
import SotoElasticsearchService
import AsyncHTTPClient
import Foundation

public struct ElasticsearchClient {

    let client: HTTPClient
    let awsClient: AWSClient
    let eventLoop: EventLoop
    let logger: Logger
    let scheme: String
    let host: String
    let port: Int?
    let username: String?
    let password: String?
    let region: Region?

    public init(eventLoop: EventLoop, logger: Logger, awsClient: AWSClient, httpClient: HTTPClient, scheme: String = "http", host: String, port: Int? = 9200, username: String? = nil, password: String? = nil, region: Region? = nil) {
        self.eventLoop = eventLoop
        self.logger = logger
        self.awsClient = awsClient
        self.client = httpClient
        self.scheme = scheme
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.region = region
    }

    func sendRequest<D: Decodable>(url: String, method: HTTPMethod, headers: HTTPHeaders, body: AWSPayload = .empty) -> EventLoopFuture<D> {
        signAndExecuteRequest(url: url, method: method, headers: headers, body: body).flatMapThrowing { clientResponse in
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

    func signAndExecuteRequest(url urlString: String, method: HTTPMethod, headers: HTTPHeaders, body: AWSPayload) -> EventLoopFuture<HTTPClient.Response> {
        let es = ElasticsearchService(client: awsClient, region: self.region)
        guard let url = URL(string: urlString) else {
            return self.eventLoop.makeFailedFuture(ElasticSearchClientError(message: "Failed to convert \(urlString) to a URL"))
        }
        return es.signHeaders(url: url, httpMethod: method, headers: headers, body: body).flatMap { headers in
            let request: HTTPClient.Request
            do {
                request = try HTTPClient.Request(url: url, method: method, headers: headers, body: body.asByteBuffer().map { .byteBuffer($0) }
                )
            } catch {
                return self.eventLoop.makeFailedFuture(error)
            }
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
            throw ElasticSearchClientError(message: "malformed url: \(urlComponents)")
        }
        return url.absoluteString
    }
}
