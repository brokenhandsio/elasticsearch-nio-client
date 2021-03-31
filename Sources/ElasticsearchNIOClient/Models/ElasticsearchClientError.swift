public struct ElasticSearchClientError: Error {
    public let message: String
    public let status: UInt?
}
