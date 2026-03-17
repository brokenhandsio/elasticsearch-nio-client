public struct ESBulkResponse: Codable {
    public let took: Int
    public let errors: Bool
    public let items: [ESBulkResponseItem]
}

public struct ESBulkResponseItem: Codable {
    public let create: ESBulkResponseItemAction?
    public let delete: ESBulkResponseItemAction?
    public let index: ESBulkResponseItemAction?
    public let update: ESBulkResponseItemAction?
}

public struct ESBulkResponseItemAction: Codable {
    public let index: String
    public let id: String
    public let version: Int?
    public let result: String?
    public let shards: ESShardsResponse?
    public let status: Int?
    public let error: ESBulkResponseError?

    enum CodingKeys: String, CodingKey {
        case index = "_index"
        case id = "_id"
        case version = "_version"
        case result
        case shards = "_shards"
        case status
        case error
    }
}

public struct ESBulkResponseError: Codable {
    public let type: String
    public let reason: String
}
