import Foundation

public struct ESCreateDocumentResponse: Codable {
    public let id: String
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
