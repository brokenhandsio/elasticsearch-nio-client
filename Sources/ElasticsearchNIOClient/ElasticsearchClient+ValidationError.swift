import Foundation

extension ElasticsearchClient {
    public struct ValidationError: LocalizedError, Equatable {
        public static let invalidURLString = ValidationError(.invalidURLString)
        public static let missingURLScheme = ValidationError(.missingURLScheme)
        public static let invalidURLScheme = ValidationError(.invalidURLScheme)
        public static let missingURLHost = ValidationError(.missingURLHost)

        var localizedDescription: String { self.kind.localizedDescription }

        private let kind: Kind
        
        private init(_ kind: Kind) { self.kind = kind }
        
        public static func ==(lhs: ValidationError, rhs: ValidationError) -> Bool {
            return lhs.kind == rhs.kind
        }
        
        private enum Kind: LocalizedError {
            case invalidURLString
            case missingURLScheme
            case invalidURLScheme
            case missingURLHost
            
            var localizedDescription: String {
                let message: String = {
                    switch self {
                    case .invalidURLString: return "invalid URL string"
                    case .missingURLScheme: return "URL scheme is missing"
                    case .invalidURLScheme: return "invalid URL scheme, expected 'http' or 'https'"
                    case .missingURLHost: return "missing remote hostname"
                    }
                }()
                return "Elasticsearch connection configuration validation failed: \(message)"
            }
        }
    }
}
