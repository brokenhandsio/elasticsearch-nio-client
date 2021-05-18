import Foundation

struct ESSearchRequest: Codable {
    let from: Int
    let size: Int
    let query: ESSearchQueryString

    init(searchQuery: String, size: Int, from: Int) {
        self.from = from
        self.size = size
        self.query = ESSearchQueryString(queryString: ESSearchQuery(query: searchQuery))
    }
}

struct ESComplexSearchRequest<Query: Encodable>: Encodable {
    let from: Int
    let size: Int
    let query: Query
}

struct ESSearchQueryString: Codable {
    let queryString: ESSearchQuery

    enum CodingKeys: String, CodingKey {
        case queryString = "query_string"
    }
}

struct ESSearchQuery: Codable {
    let query: String
}
