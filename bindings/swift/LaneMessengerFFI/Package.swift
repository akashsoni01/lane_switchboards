// swift-tools-version: 5.9
import PackageDescription

// Hand-written C ABI Swift wrappers ONLY.
// UniFFI `Sources/LaneMessengerFFI/generated/` is excluded — it defines a second
// `LaneSession` / `LaneE2eeDevice` that would conflict with this module and with
// the Rust C ABI naming used by apps/ios.
//
// Link the Rust library at the *app* (or use XCFramework):
//   cargo build -p lane_messenger_ffi --release
//   .linkedLibrary("lane_messenger_ffi") + librarySearchPaths

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
            path: "Sources/LaneMessengerFFI",
            exclude: [
                "generated", // UniFFI — do not compile into this product
            ]
        ),
        .systemLibrary(
            name: "LaneMessengerC",
            path: "Sources/LaneMessengerC",
            pkgConfig: nil
        ),
    ]
)
