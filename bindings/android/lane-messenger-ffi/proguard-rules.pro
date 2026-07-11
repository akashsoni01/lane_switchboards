# ProGuard / R8 keep rules for JNI + UniFFI/JNA entry points
-keep class com.lane.messenger.** { *; }
-keepclassmembers class com.lane.messenger.** {
    native <methods>;
}
-keep class uniffi.lane_messenger.** { *; }
-keep class com.sun.jna.** { *; }
-keepclassmembers class * extends com.sun.jna.** { public *; }
