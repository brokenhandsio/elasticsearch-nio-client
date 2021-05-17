public struct ElasticSearchClientError: Error {
    public let message: String
    public let status: UInt?

    public init(message: String, status: UInt?) {
        self.message = message
        self.status = status
    }
}
