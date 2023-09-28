import NIOHTTP1

public struct ElasticSearchClientError: Error {
    public let message: String
    public let status: HTTPResponseStatus?

    public init(message: String, status: HTTPResponseStatus?) {
        self.message = message
        self.status = status
    }
}
