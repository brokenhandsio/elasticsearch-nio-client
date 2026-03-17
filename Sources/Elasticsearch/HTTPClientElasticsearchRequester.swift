import AsyncHTTPClient
import HTTPTypes
import Logging

public struct HTTPClientElasticsearchRequester: ElasticsearchRequester {
    let logger: Logger
    let username: String?
    let password: String?
    let client: HTTPClient

    public func executeRequest(
        url urlString: String,
        method: HTTPRequest.Method,
        headers: HTTPFields,
        body: HTTPClientRequest.Body?
    ) async throws -> HTTPClientResponse {
        var clientRequest = HTTPClientRequest(url: urlString)
        clientRequest.method = .init(rawValue: method.rawValue)

        var headers = headers
        if let username = self.username, let password = self.password {
            let pair = "\(username):\(password)"
            if let data = pair.data(using: .utf8) {
                let basic = data.base64EncodedString()
                headers[.authorization] = "Basic \(basic)"
            }
        }

        for header in headers {
            clientRequest.headers.add(name: header.name.canonicalName, value: header.value)
        }

        if let body {
            clientRequest.body = body
            self.logger.trace("Sending request", metadata: ["method": "\(method)", "headers": "\(headers)", "body": "\(body)"])
        } else {
            self.logger.trace("Sending request", metadata: ["method": "\(method)", "headers": "\(headers)", "body": "empty"])
        }

        return try await client.execute(clientRequest, timeout: .seconds(30))
    }
}
