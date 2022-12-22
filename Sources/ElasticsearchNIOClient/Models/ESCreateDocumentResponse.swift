import Foundation

public struct ESCreateDocumentResponse<ID>: Codable where ID: Hashable & Codable {
    public let id: ID
    public let index: String
    public let version: Int?
    public let result: String
    
    enum CodingKeys: String, CodingKey {
        case id = "_id"
        case index = "_index"
        case version = "_version"
        case result
    }
}

public typealias ESUpdateDocumentResponse = ESCreateDocumentResponse
