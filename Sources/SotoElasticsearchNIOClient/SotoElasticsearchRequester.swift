import SotoElasticsearchService

struct SotoElasticsearchRequester: ElasticsearchRequester {
    let awsClient: AWSClient
    let region: Region?

    func executeRequest(url urlString: String, method: HTTPMethod, headers: HTTPHeaders, body: AWSPayload) -> EventLoopFuture<HTTPClient.Response> {
        let es = ElasticsearchService(client: awsClient, region: self.region)
        guard let url = URL(string: urlString) else {
            return self.eventLoop.makeFailedFuture(ElasticSearchClientError(message: "Failed to convert \(urlString) to a URL", status: nil))
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
