import SwiftUI
import LaneMessengerKit

@main
struct LaneMessengerApp: App {
    @State private var model = makeModel()

    var body: some Scene {
        WindowGroup {
            RootView(model: model)
        }
    }

    private static func makeModel() -> AppModel {
        #if canImport(LaneMessengerFFI)
        // Prefer real FFI when the XCFramework / SPM module is linked.
        return AppModel(transport: LaneFFITransport())
        #else
        // Simulator/dev without native lib: mock transport so UI is exercisable.
        return AppModel(transport: MockMessengerTransport())
        #endif
    }
}
