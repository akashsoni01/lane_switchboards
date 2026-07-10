# ProGuard / R8 keep rules for JNI entry points
-keep class com.lane.messenger.LaneSession { *; }
-keepclassmembers class com.lane.messenger.LaneSession {
    native <methods>;
}
