//! JNI bindings for `com.lane.messenger.LaneSession` (`feature = "jni"`).

use std::ptr;

use jni::objects::{JByteArray, JClass, JObject, JString};
use jni::sys::{jboolean, jint, jlong, jstring};
use jni::JNIEnv;

use crate::c_api::{
    lane_send_chat, lane_session_close, lane_session_connect, lane_session_free,
    lane_session_ping, lane_session_poll_event, lane_string_free, LaneSession,
};

fn jstring_to_string(env: &mut JNIEnv<'_>, s: &JString) -> Result<String, jni::errors::Error> {
    Ok(env.get_string(s)?.into())
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneSession_nativeConnect(
    mut env: JNIEnv,
    _class: JClass,
    host: JString,
    port: jint,
    use_tls: jboolean,
    user_id: JString,
    device_id: JString,
    auth_token: JString,
    client_version: JString,
    resume_after_seq: jlong,
    ping_interval_secs: jlong,
) -> jlong {
    let Ok(host) = jstring_to_string(&mut env, &host) else {
        return 0;
    };
    let Ok(user_id) = jstring_to_string(&mut env, &user_id) else {
        return 0;
    };
    let Ok(device_id) = jstring_to_string(&mut env, &device_id) else {
        return 0;
    };
    let Ok(auth_token) = jstring_to_string(&mut env, &auth_token) else {
        return 0;
    };
    let Ok(client_version) = jstring_to_string(&mut env, &client_version) else {
        return 0;
    };

    let host_c = match std::ffi::CString::new(host) {
        Ok(s) => s,
        Err(_) => return 0,
    };
    let user_c = match std::ffi::CString::new(user_id) {
        Ok(s) => s,
        Err(_) => return 0,
    };
    let device_c = match std::ffi::CString::new(device_id) {
        Ok(s) => s,
        Err(_) => return 0,
    };
    let token_c = match std::ffi::CString::new(auth_token) {
        Ok(s) => s,
        Err(_) => return 0,
    };
    let ver_c = match std::ffi::CString::new(client_version) {
        Ok(s) => s,
        Err(_) => return 0,
    };

    let mut err: *mut std::os::raw::c_char = ptr::null_mut();
    let session = lane_session_connect(
        host_c.as_ptr(),
        port as u16,
        if use_tls != 0 { 1 } else { 0 },
        user_c.as_ptr(),
        device_c.as_ptr(),
        token_c.as_ptr(),
        ver_c.as_ptr(),
        resume_after_seq as u64,
        ping_interval_secs as u64,
        &mut err,
    );
    if !err.is_null() {
        lane_string_free(err);
    }
    session as jlong
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneSession_nativePing(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
) -> jint {
    lane_session_ping(handle as *mut LaneSession)
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneSession_nativeClose(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
) -> jint {
    lane_session_close(handle as *mut LaneSession)
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneSession_nativeFree(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
) {
    lane_session_free(handle as *mut LaneSession);
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneSession_nativePollEvent(
    env: JNIEnv,
    _this: JObject,
    handle: jlong,
    timeout_ms: jlong,
) -> jstring {
    let mut out: *mut std::os::raw::c_char = ptr::null_mut();
    let n = lane_session_poll_event(handle as *mut LaneSession, timeout_ms as u64, &mut out);
    if n != 1 || out.is_null() {
        return ptr::null_mut();
    }
    let s = unsafe { std::ffi::CStr::from_ptr(out) };
    let java = env
        .new_string(s.to_string_lossy().as_ref())
        .map(|js| js.into_raw())
        .unwrap_or(ptr::null_mut());
    lane_string_free(out);
    java
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneSession_nativeSendChat(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    to: JString,
    message_id: JString,
    body: JByteArray,
) -> jlong {
    let Ok(to) = jstring_to_string(&mut env, &to) else {
        return -1;
    };
    let Ok(message_id) = jstring_to_string(&mut env, &message_id) else {
        return -1;
    };
    let Ok(bytes) = env.convert_byte_array(&body) else {
        return -1;
    };
    let to_c = match std::ffi::CString::new(to) {
        Ok(s) => s,
        Err(_) => return -1,
    };
    let mid_c = match std::ffi::CString::new(message_id) {
        Ok(s) => s,
        Err(_) => return -1,
    };
    let mut seq: u64 = 0;
    let code = lane_send_chat(
        handle as *mut LaneSession,
        to_c.as_ptr(),
        mid_c.as_ptr(),
        bytes.as_ptr(),
        bytes.len(),
        &mut seq,
    );
    if code != 0 {
        return -1;
    }
    seq as jlong
}
