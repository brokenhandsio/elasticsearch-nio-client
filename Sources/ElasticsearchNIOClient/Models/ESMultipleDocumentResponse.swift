import Foundation

public struct ESGetMultipleDocumentsResponse<Document: Decodable>: Decodable {
    public struct Hits: Decodable {
        public struct Total: Decodable {
            public let value: Int
            public let relation: Relation

            public enum Relation: String, Decodable {
                case eq, gte
            }
        }

        public let total: Total
        public let hits: [ESGetSingleDocumentResponse<Document>]
    }

    public let hits: Hits
}
