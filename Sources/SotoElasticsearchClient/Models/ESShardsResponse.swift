import Foundation

public struct ESShardsResponse: Codable {
    public let total: Int
    public let successful: Int
    public let skipped: Int
    public let failed: Int
}
