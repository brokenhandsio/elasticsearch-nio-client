import Foundation

public struct ESGetSingleDocumentResponse<Document: Decodable>: Decodable {
    let id: String
    let index: String
    let version: Int?
    let source: Document
    
    enum CodingKeys: String, CodingKey {
        case id = "_id"
        case index = "_index"
        case version = "_version"
        case source = "_source"
    }
}
