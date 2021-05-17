import NIOHTTP1
import AsyncHTTPClient
import NIO

public protocol ElasticsearchRequester {
    func executeRequest(url urlString: String, method: HTTPMethod, headers: HTTPHeaders, body: ByteBuffer?) -> EventLoopFuture<HTTPClient.Response>
}
