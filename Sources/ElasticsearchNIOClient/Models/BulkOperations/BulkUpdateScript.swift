import Foundation

struct BulkUpdateScript: Codable {
    let update: BulkOperationBody
}

struct BulkUpdateScriptDocument<Script: Encodable>: Encodable {
    let script: Script
}
