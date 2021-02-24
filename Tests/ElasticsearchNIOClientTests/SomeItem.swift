import Foundation

struct SomeItem: Codable, Identifiable {
    let id: UUID
    let name: String
}
