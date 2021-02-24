import Foundation

struct BulkCreate: Codable {
    let create: BulkCreateBody
}

struct BulkCreateBody: Codable {
    let index: String
    let id: String

    enum CodingKeys: String, CodingKey {
        case id = "_id"
        case index = "_index"
    }
}
