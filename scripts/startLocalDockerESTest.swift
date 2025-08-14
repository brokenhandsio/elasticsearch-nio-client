#!/usr/bin/swift
import Foundation

let port = 9200
let containerName = "soto-esclient-test"

print("Starting local Elasticsearch instance in container \(containerName)")

@discardableResult
func shell(_ args: String..., returnStdOut: Bool = false) -> (Int32, Pipe) {
    let task = Process()
    task.launchPath = "/usr/bin/env"
    task.arguments = args
    let pipe = Pipe()
    if returnStdOut {
        task.standardOutput = pipe
    }
    task.launch()
    task.waitUntilExit()
    return (task.terminationStatus, pipe)
}

extension Pipe {
    func string() -> String? {
        let data = self.fileHandleForReading.readDataToEndOfFile()
        let result: String?
        if let string = String(data: data, encoding: String.Encoding.utf8) {
            result = string
        } else {
            result = nil
        }
        return result
    }
}

let (dockerResult, _) = shell(
    "docker", "run", "--name", containerName, "-p", "\(port):9200",
    "-e", "discovery.type=single-node",
    "-e", "ES_JAVA_OPTS=-Xms256m -Xmx256m",
    "-e", "xpack.security.enabled=false",
    "-d", "docker.elastic.co/elasticsearch/elasticsearch:9.1.0")

guard dockerResult == 0 else {
    print("❌ ERROR: Failed to create the Elasticsearch instance")
    exit(1)
}

print("Elasticsearch created in Docker 🐳")
