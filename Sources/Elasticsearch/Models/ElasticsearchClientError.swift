import HTTPTypes

public struct ElasticsearchClientError: Error {
    public let message: String
    public let status: HTTPResponse.Status?

    public init(message: String, status: HTTPResponse.Status?) {
        self.message = message
        self.status = status
    }
}
