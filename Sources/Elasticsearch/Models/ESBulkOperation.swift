import Foundation

public struct ESBulkOperation<Document, ID> where Document: Encodable, ID: Hashable & Encodable {
    public let operationType: BulkOperationType
    public let document: Document?
    public let id: ID
    public let index: String

    public init(operationType: BulkOperationType, index: String, id: ID, document: Document?) {
        self.operationType = operationType
        self.index = index
        self.id = id
        self.document = document
    }
}

public enum BulkOperationType {
    case create
    case delete
    case index
    case update
    case updateScript
}
