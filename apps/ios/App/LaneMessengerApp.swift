import SwiftUI
import LaneMessengerKit
#if canImport(UIKit) && os(iOS)
import UIKit
import UserNotifications
#endif

@main
struct LaneMessengerApp: App {
    #if os(iOS)
    @UIApplicationDelegateAdaptor(LaneAppDelegate.self) private var appDelegate
    #endif
    @State private var model = makeModel()

    var body: some Scene {
        WindowGroup {
            RootView(model: model)
                .onAppear {
                    #if os(iOS)
                    LaneAppDelegate.sharedModel = model
                    #endif
                }
                .onOpenURL { url in
                    Task { await model.handleDeepLink(url) }
                }
        }
    }

    private static func makeModel() -> AppModel {
        #if canImport(LaneMessengerFFI)
        return AppModel(transport: LaneFFITransport())
        #else
        return AppModel(transport: MockMessengerTransport())
        #endif
    }
}

#if os(iOS)
/// Handles APNs device token + notification taps. Uses **alert** pushes only (never VoIP).
final class LaneAppDelegate: NSObject, UIApplicationDelegate, UNUserNotificationCenterDelegate {
    static weak var sharedModel: AppModel?

    func application(
        _ application: UIApplication,
        didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]? = nil
    ) -> Bool {
        UNUserNotificationCenter.current().delegate = self
        application.registerForRemoteNotifications()
        if let userInfo = launchOptions?[.remoteNotification] as? [AnyHashable: Any] {
            Task { @MainActor in
                await Self.sharedModel?.handleNotificationOpen(userInfo: userInfo)
            }
        }
        return true
    }

    func application(_ application: UIApplication, didRegisterForRemoteNotificationsWithDeviceToken deviceToken: Data) {
        Task { @MainActor in
            await Self.sharedModel?.didRegisterForRemoteNotifications(deviceToken: deviceToken)
        }
    }

    func application(_ application: UIApplication, didFailToRegisterForRemoteNotificationsWithError error: Error) {
        LaneLog.ui.error("APNs registration failed")
    }

    func userNotificationCenter(
        _ center: UNUserNotificationCenter,
        didReceive response: UNNotificationResponse,
        withCompletionHandler completionHandler: @escaping () -> Void
    ) {
        let info = response.notification.request.content.userInfo
        Task { @MainActor in
            await Self.sharedModel?.handleNotificationOpen(userInfo: info)
            completionHandler()
        }
    }

    func userNotificationCenter(
        _ center: UNUserNotificationCenter,
        willPresent notification: UNNotification,
        withCompletionHandler completionHandler: @escaping (UNNotificationPresentationOptions) -> Void
    ) {
        // Foreground: rely on in-app UI; still allow banner if desired.
        completionHandler([.banner, .sound, .badge])
    }
}
#endif
