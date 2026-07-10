// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "LaneMessengerKit",
    platforms: [.iOS(.v17), .macOS(.v14)],
    products: [
        .library(name: "LaneMessengerKit", targets: ["LaneMessengerKit"]),
        .executable(name: "lane-messenger-kit-smoke", targets: ["LaneMessengerKitSmoke"]),
    ],
    targets: [
        .target(
            name: "LaneMessengerKit",
            path: "Sources/LaneMessengerKit"
        ),
        .executableTarget(
            name: "LaneMessengerKitSmoke",
            dependencies: ["LaneMessengerKit"],
            path: "Sources/LaneMessengerKitSmoke"
        ),
        .testTarget(
            name: "LaneMessengerKitTests",
            dependencies: ["LaneMessengerKit"],
            path: "Tests/LaneMessengerKitTests"
        ),
    ]
)
