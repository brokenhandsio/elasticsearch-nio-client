import Foundation

public struct ESGetMultipleDocumentsResponse<Document: Decodable>: Decodable {
    public let hits: Hits
}

extension ESGetMultipleDocumentsResponse {
    public struct Hits: Decodable {
        public let total: Total?
        public let hits: [ESGetSingleDocumentResponse<Document>]
    }
}

extension ESGetMultipleDocumentsResponse.Hits {
    public struct Total: Decodable {
        public let value: Int
        public let relation: Relation
    }
}

extension ESGetMultipleDocumentsResponse.Hits.Total {
    public enum Relation: String, Decodable {
        case eq, gte
    }
}
