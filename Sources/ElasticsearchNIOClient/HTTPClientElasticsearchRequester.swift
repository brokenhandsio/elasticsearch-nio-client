import Foundation
import NIO
import Logging
import AsyncHTTPClient
import NIOHTTP1

public struct HTTPClientElasticsearchRequester: ElasticsearchRequester {
    let eventLoop: EventLoop
    let logger: Logger
    let client: HTTPClient

    public func executeRequest(url urlString: String, method: HTTPMethod, headers: HTTPHeaders, body: ByteBuffer?) -> EventLoopFuture<HTTPClient.Response> {
        guard let url = URL(string: urlString) else {
            return self.eventLoop.makeFailedFuture(ElasticSearchClientError(message: "Failed to convert \(urlString) to a URL", status: nil))
        }
        let httpClientBody: HTTPClient.Body?
        if let body = body {
            httpClientBody = .byteBuffer(body)
        } else {
            httpClientBody = nil
        }
        let request: HTTPClient.Request
        do {
            request = try HTTPClient.Request(url: url, method: method, headers: headers, body: httpClientBody)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
        self.logger.trace("Request: \(request)")
        if let requestBody = body {
            let bodyString = String(buffer: requestBody)
            self.logger.trace("Request body: \(bodyString)")
        }
        return self.client.execute(request: request, eventLoop: HTTPClient.EventLoopPreference.delegateAndChannel(on: self.eventLoop), logger: self.logger)
    }
}
