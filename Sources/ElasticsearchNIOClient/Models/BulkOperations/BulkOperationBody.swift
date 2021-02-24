import Foundation

struct BulkOperationBody: Codable {
    let index: String
    let id: String

    enum CodingKeys: String, CodingKey {
        case id = "_id"
        case index = "_index"
    }
}
