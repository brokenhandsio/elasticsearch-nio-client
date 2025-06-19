import AsyncHTTPClient
import HTTPTypes

public protocol ElasticsearchRequester: Sendable {
    func executeRequest(
        url urlString: String,
        method: HTTPRequest.Method,
        headers: HTTPFields,
        body: HTTPClientRequest.Body?
    ) async throws -> HTTPClientResponse
}
