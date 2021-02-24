import Foundation

public struct ESGetMultipleDocumentsResponse<Document: Decodable>: Decodable {
    public struct Hits: Decodable {
        public let hits: [ESGetSingleDocumentResponse<Document>]
    }

    public let hits: Hits
}
