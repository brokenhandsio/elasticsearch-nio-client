import Foundation

public struct ESGetMultipleDocumentsResponse<Document: Decodable>: Decodable {
    struct Hits: Decodable {
        let hits: [ESGetSingleDocumentResponse<Document>]
    }

    let hits: Hits
}
