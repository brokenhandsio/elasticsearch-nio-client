#!/usr/bin/swift
import Foundation

@discardableResult
func shell(_ args: String...) -> Int32 {
    let task = Process()
    task.launchPath = "/usr/bin/env"
    task.arguments = args
    task.launch()
    task.waitUntilExit()
    return task.terminationStatus
}

let containerName = "soto-esclient-test"
print("Stopping existing Elasticsearch instance in container \(containerName)")

let stopResult = shell("docker", "stop", containerName)

guard stopResult == 0 else {
    print("❌ ERROR: Failed to stop the ES instance")
    exit(1)
}

let removeResult = shell("docker", "rm", containerName)

guard removeResult == 0 else {
    print("❌ ERROR: Failed to remove the ES container")
    exit(1)
}

let startResult = shell("swift", "scripts/startLocalDockerESTest.swift")

guard startResult == 0 else {
    print("❌ ERROR: Failed to start the new ES instance container")
    exit(1)
}

print("✅ Database container \(containerName) reset")
