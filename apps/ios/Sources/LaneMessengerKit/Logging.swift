import Foundation
import os

public enum LaneLog {
    public static let session = Logger(subsystem: "com.lane.messenger", category: "session")
    public static let chat = Logger(subsystem: "com.lane.messenger", category: "chat")
    public static let media = Logger(subsystem: "com.lane.messenger", category: "media")
    public static let e2ee = Logger(subsystem: "com.lane.messenger", category: "e2ee")
    public static let ui = Logger(subsystem: "com.lane.messenger", category: "ui")
    public static let auth = Logger(subsystem: "com.lane.messenger", category: "auth")
}
