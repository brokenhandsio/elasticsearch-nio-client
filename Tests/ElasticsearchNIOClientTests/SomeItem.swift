import Foundation

struct SomeItem: Codable, Identifiable {
    let id: UUID
    let name: String
    let count: Int?

    init(id: UUID, name: String, count: Int? = nil) {
        self.id = id
        self.name = name
        self.count = count
    }
}
