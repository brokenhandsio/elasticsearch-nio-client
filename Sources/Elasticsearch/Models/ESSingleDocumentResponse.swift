import Foundation

public struct ESGetSingleDocumentResponse<Document: Decodable>: Decodable {
    public let id: String
    public let index: String
    public let version: Int?
    public let source: Document

    enum CodingKeys: String, CodingKey {
        case id = "_id"
        case index = "_index"
        case version = "_version"
        case source = "_source"
    }
}
