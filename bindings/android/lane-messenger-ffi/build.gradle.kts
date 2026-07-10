plugins {
    id("com.android.library")
    id("org.jetbrains.kotlin.android")
}

android {
    namespace = "com.lane.messenger"
    compileSdk = 34
    defaultConfig {
        minSdk = 24
        ndk {
            abiFilters += listOf("arm64-v8a", "armeabi-v7a", "x86_64")
        }
        consumerProguardFiles("proguard-rules.pro")
    }
    // Place cargo-ndk outputs under src/main/jniLibs/<abi>/liblane_messenger_ffi.so
    //   scripts/build_android_ndk.sh
    //
    // Two Kotlin surfaces ship in this module:
    //   1) com.lane.messenger.LaneSession — thin JNI over the C ABI (`--features jni`)
    //   2) uniffi.lane_messenger.* — UniFFI/JNA (`scripts/generate_uniffi_bindings.sh`)
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.8.1")
    // UniFFI Kotlin bindings (uniffi.lane_messenger) load the .so via JNA.
    implementation("net.java.dev.jna:jna:5.14.0@aar")
}
