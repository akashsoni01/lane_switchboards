import SwiftUI

/// WhatsApp-style ticks: clock → single → double → blue double.
public struct MessageTicksView: View {
    public var status: MessageStatus
    public var isOutbound: Bool

    public init(status: MessageStatus, isOutbound: Bool) {
        self.status = status
        self.isOutbound = isOutbound
    }

    public var body: some View {
        Group {
            if isOutbound {
                switch status {
                case .pending:
                    Image(systemName: "clock")
                case .failed:
                    Image(systemName: "exclamationmark.circle")
                        .foregroundStyle(.red)
                case .sent:
                    Image(systemName: "checkmark")
                case .delivered:
                    Image(systemName: "checkmark.circle")
                case .read:
                    Image(systemName: "checkmark.circle.fill")
                        .foregroundStyle(.blue)
                }
            }
        }
        .font(.caption2)
        .accessibilityLabel(accessibility)
    }

    private var accessibility: String {
        switch status {
        case .pending: return "Sending"
        case .failed: return "Failed"
        case .sent: return "Sent"
        case .delivered: return "Delivered"
        case .read: return "Read"
        }
    }
}

public struct PresenceDot: View {
    public var kind: PresenceKind?

    public init(kind: PresenceKind?) {
        self.kind = kind
    }

    public var body: some View {
        Circle()
            .fill(color)
            .frame(width: 10, height: 10)
            .accessibilityLabel(kind?.isOnline == true ? "Online" : "Offline")
    }

    private var color: Color {
        switch kind {
        case .available: return .green
        case .lastSeen, .unavailable, .none: return .gray.opacity(0.5)
        }
    }
}

public struct AvatarView: View {
    public var title: String

    public init(title: String) {
        self.title = title
    }

    public var body: some View {
        ZStack {
            Circle().fill(Color.accentColor.opacity(0.2))
            Text(initials)
                .font(.headline.weight(.semibold))
                .foregroundStyle(Color.accentColor)
        }
        .frame(width: 44, height: 44)
        .accessibilityHidden(true)
    }

    private var initials: String {
        let parts = title.split(separator: " ")
        if parts.count >= 2 {
            return String(parts[0].prefix(1) + parts[1].prefix(1)).uppercased()
        }
        return String(title.prefix(2)).uppercased()
    }
}
