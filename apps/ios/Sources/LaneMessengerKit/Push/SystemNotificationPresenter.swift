import Foundation
#if canImport(UserNotifications)
import UserNotifications
#endif
#if canImport(UIKit) && os(iOS)
import UIKit
#endif

#if canImport(UserNotifications)
/// System notification center + badge (iOS / macOS).
public final class SystemNotificationPresenter: NSObject, NotificationPresenting, @unchecked Sendable {
    public var previewPolicy: NotificationPreviewPolicy = .default
    private let center = UNUserNotificationCenter.current()

    public override init() {
        super.init()
    }

    public func requestAuthorization() async -> Bool {
        do {
            return try await center.requestAuthorization(options: [.alert, .badge, .sound])
        } catch {
            LaneLog.ui.error("notification auth failed")
            return false
        }
    }

    public func presentLocal(payload: PushPayload) async {
        let content = UNMutableNotificationContent()
        content.title = previewPolicy.displayTitle(for: payload)
        content.body = previewPolicy.displayBody(for: payload)
        content.sound = .default
        content.userInfo = [
            "lane": [
                "conversation_id": payload.conversationId,
                "message_id": payload.messageId,
                "sender_id": payload.senderId,
                "preview": payload.preview,
                "encrypted": payload.encrypted,
            ] as [String: Any],
        ]
        content.threadIdentifier = payload.conversationId
        let id = payload.messageId.isEmpty
            ? "lane-\(payload.conversationId)-\(UUID().uuidString)"
            : "lane-\(payload.messageId)"
        let req = UNNotificationRequest(identifier: id, content: content, trigger: nil)
        do {
            try await center.add(req)
        } catch {
            LaneLog.ui.error("local notification failed")
        }
    }

    public func setBadge(_ count: Int) async {
        let value = max(0, count)
        #if canImport(UIKit) && os(iOS)
        await MainActor.run {
            UIApplication.shared.applicationIconBadgeNumber = value
        }
        #endif
        if #available(iOS 16.0, macOS 13.0, *) {
            try? await center.setBadgeCount(value)
        }
    }

    public func clearNotifications(forConversationId conversationId: String) async {
        let delivered = await center.deliveredNotifications()
        let ids = delivered.compactMap { note -> String? in
            let info = note.request.content.userInfo
            guard let payload = PushPayload.parse(userInfo: info),
                  payload.conversationId == conversationId
            else { return nil }
            return note.request.identifier
        }
        center.removeDeliveredNotifications(withIdentifiers: ids)
        center.removePendingNotificationRequests(withIdentifiers: ids)
    }
}
#endif
