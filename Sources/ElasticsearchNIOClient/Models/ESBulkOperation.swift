import Foundation

public struct ESBulkOperation<Document> where Document: Encodable {
    public let operationType: BulkOperationType
    public let document: Document?
    public let id: String
    public let index: String

    public init(operationType: BulkOperationType, index: String, id: String, document: Document?) {
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
