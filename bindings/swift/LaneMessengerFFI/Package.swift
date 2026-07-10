// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "LaneMessengerFFI",
    platforms: [.iOS(.v15), .macOS(.v13)],
    products: [
        .library(name: "LaneMessengerFFI", targets: ["LaneMessengerFFI"]),
    ],
    targets: [
        // Hand-written C ABI convenience wrapper (no UniFFI runtime required).
        .target(
            name: "LaneMessengerFFI",
            dependencies: ["LaneMessengerC"],
            path: "Sources/LaneMessengerFFI",
            exclude: ["generated"]
        ),
        // UniFFI-generated Swift lives under Sources/LaneMessengerFFI/generated/
        // and is consumed after linking LaneMessengerFFI.xcframework
        // (see scripts/build_xcframework.sh + scripts/generate_uniffi_bindings.sh).
        .systemLibrary(
            name: "LaneMessengerC",
            path: "Sources/LaneMessengerC",
            pkgConfig: nil
        ),
    ]
)
