import ElasticsearchNIOClient
import SotoElasticsearchService
import AsyncHTTPClient
import Foundation
import Logging

struct SotoElasticsearchRequester: ElasticsearchRequester {
    let awsClient: AWSClient
    let region: Region?
    let eventLoop: EventLoop
    let logger: Logger
    let client: HTTPClient

    func executeRequest(url urlString: String, method: HTTPMethod, headers: HTTPHeaders, body: ByteBuffer?) -> EventLoopFuture<HTTPClient.Response> {
        let es = ElasticsearchService(client: awsClient, region: self.region)
        guard let url = URL(string: urlString) else {
            return self.eventLoop.makeFailedFuture(ElasticSearchClientError(message: "Failed to convert \(urlString) to a URL", status: nil))
        }
        let awsBody: AWSPayload
        if let body = body {
            awsBody = AWSPayload.byteBuffer(body)
        } else {
            awsBody = .empty
        }
        return es.signHeaders(url: url, httpMethod: method, headers: headers, body: awsBody).flatMap { headers in
            let request: HTTPClient.Request
            do {
                request = try HTTPClient.Request(url: url, method: method, headers: headers, body: awsBody.asByteBuffer().map { .byteBuffer($0) }
                )
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
}
