// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "LaneMessengerFFI",
    platforms: [.iOS(.v15), .macOS(.v13)],
    products: [
        .library(name: "LaneMessengerFFI", targets: ["LaneMessengerFFI"]),
    ],
    targets: [
        .target(
            name: "LaneMessengerFFI",
            dependencies: ["LaneMessengerC"],
            path: "Sources/LaneMessengerFFI"
        ),
        .systemLibrary(
            name: "LaneMessengerC",
            path: "Sources/LaneMessengerC",
            pkgConfig: nil
        ),
    ]
)
