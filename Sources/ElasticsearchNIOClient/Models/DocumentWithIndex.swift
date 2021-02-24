import Foundation

public struct DocumentWithIndex<Document> where Document: Encodable & Identifiable {
    public let index: String
    public let document: Document

    public init(index: String, document: Document) {
        self.index = index
        self.document = document
    }
}
