import Foundation

struct BulkUpdate: Codable {
    let update: BulkOperationBody
}

struct BulkUpdateDocument<Document: Encodable>: Encodable {
    let doc: Document
}
