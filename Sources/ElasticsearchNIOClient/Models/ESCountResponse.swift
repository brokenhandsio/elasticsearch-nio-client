import Foundation

public struct ESCountResponse: Codable {
    public let count: Int
    public let shards: ESShardsResponse

    enum CodingKeys: String, CodingKey {
        case count
        case shards = "_shards"
    }
}
