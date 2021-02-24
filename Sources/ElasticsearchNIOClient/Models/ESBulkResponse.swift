import Foundation

public struct ESBulkResponse: Codable {
    public let took: Int
    public let errors: Bool
}
