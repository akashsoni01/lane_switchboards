plugins {
    id("com.android.library")
    // Kotlin optional (demo / UniFFI); production API is Java under com.lane.messenger
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
    //   FEATURES="c-api,tls,ws,jni" ./scripts/build_android_ndk.sh
    //
    // Surfaces in this module:
    //   1) com.lane.messenger.* — production Java JNI over C ABI (`--features jni`)
    //   2) uniffi.lane_messenger.* — UniFFI/JNA (`scripts/generate_uniffi_bindings.sh`)
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.8.1")
    implementation("net.java.dev.jna:jna:5.14.0@aar")
}
