//! JNI bindings for `com.lane.messenger.*` (`feature = "jni"`).
//!
//! Mirrors the C ABI in `lane_messenger_ffi.h` for production Android / JVM hosts:
//! session, chat, presence, groups, media, and E2EE.

use std::cell::RefCell;
use std::ptr;

use jni::objects::{JByteArray, JClass, JObject, JString};
use jni::sys::{jboolean, jint, jlong, jobjectArray, jstring};
use jni::JNIEnv;

use crate::c_api::{
    lane_ack_delivered, lane_ack_read, lane_add_member, lane_create_group, lane_decrypt_chat,
    lane_decrypt_group, lane_e2ee_create_group_session, lane_e2ee_distribute_group_key,
    lane_e2ee_export_pickle, lane_e2ee_free, lane_e2ee_generate, lane_e2ee_identity_key,
    lane_e2ee_import_pickle, lane_e2ee_publish, lane_e2ee_safety_number,
    lane_e2ee_try_import_group_key, lane_fetch_media, lane_leave_group, lane_protocol_version,
    lane_remove_member, lane_runtime_init, lane_send_chat, lane_send_chat_retry,
    lane_send_chat_with_media, lane_send_encrypted_chat, lane_send_encrypted_group,
    lane_send_group, lane_send_group_with_media, lane_send_presence, lane_session_close,
    lane_session_connect, lane_session_free, lane_session_ping, lane_session_poll_event,
    lane_session_set_resume_seq, lane_string_free, lane_subscribe_presence, lane_upload_media,
    lane_version, lane_bytes_free, LaneE2eeDevice, LaneSession,
};

thread_local! {
    static LAST_ERROR: RefCell<Option<String>> = const { RefCell::new(None) };
}

fn set_last_error(msg: impl Into<String>) {
    LAST_ERROR.with(|c| *c.borrow_mut() = Some(msg.into()));
}

fn clear_last_error() {
    LAST_ERROR.with(|c| *c.borrow_mut() = None);
}

fn jstring_to_string(env: &mut JNIEnv<'_>, s: &JString) -> Result<String, jni::errors::Error> {
    Ok(env.get_string(s)?.into())
}

fn cstr_owned(s: String) -> Option<std::ffi::CString> {
    std::ffi::CString::new(s).ok()
}

fn take_c_string(ptr: *mut std::os::raw::c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    let s = unsafe { std::ffi::CStr::from_ptr(ptr) }
        .to_string_lossy()
        .into_owned();
    lane_string_free(ptr);
    Some(s)
}

#[no_mangle]
pub extern "system" fn JNI_OnLoad(_vm: *mut jni::sys::JavaVM, _reserved: *mut std::ffi::c_void) -> jint {
    lane_runtime_init();
    jni::sys::JNI_VERSION_1_6
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneNative_nativeVersion(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    let v = unsafe { std::ffi::CStr::from_ptr(lane_version()) };
    env.new_string(v.to_string_lossy().as_ref())
        .map(|js| js.into_raw())
        .unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneNative_nativeProtocolVersion(
    _env: JNIEnv,
    _class: JClass,
) -> jint {
    lane_protocol_version() as jint
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneNative_nativeLastError(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    let msg = LAST_ERROR.with(|c| c.borrow_mut().take());
    match msg {
        Some(m) => env
            .new_string(m)
            .map(|js| js.into_raw())
            .unwrap_or(ptr::null_mut()),
        None => ptr::null_mut(),
    }
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
    clear_last_error();
    let Ok(host) = jstring_to_string(&mut env, &host) else {
        set_last_error("invalid host");
        return 0;
    };
    let Ok(user_id) = jstring_to_string(&mut env, &user_id) else {
        set_last_error("invalid user_id");
        return 0;
    };
    let Ok(device_id) = jstring_to_string(&mut env, &device_id) else {
        set_last_error("invalid device_id");
        return 0;
    };
    let Ok(auth_token) = jstring_to_string(&mut env, &auth_token) else {
        set_last_error("invalid auth_token");
        return 0;
    };
    let Ok(client_version) = jstring_to_string(&mut env, &client_version) else {
        set_last_error("invalid client_version");
        return 0;
    };

    let Some(host_c) = cstr_owned(host) else {
        set_last_error("host contains NUL");
        return 0;
    };
    let Some(user_c) = cstr_owned(user_id) else {
        set_last_error("user_id contains NUL");
        return 0;
    };
    let Some(device_c) = cstr_owned(device_id) else {
        set_last_error("device_id contains NUL");
        return 0;
    };
    let Some(token_c) = cstr_owned(auth_token) else {
        set_last_error("auth_token contains NUL");
        return 0;
    };
    let Some(ver_c) = cstr_owned(client_version) else {
        set_last_error("client_version contains NUL");
        return 0;
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
    if session.is_null() {
        if let Some(msg) = take_c_string(err) {
            set_last_error(msg);
        } else {
            set_last_error("lane_session_connect failed");
        }
        return 0;
    }
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
pub extern "system" fn Java_com_lane_messenger_LaneSession_nativeSetResumeSeq(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
    seq: jlong,
) -> jint {
    lane_session_set_resume_seq(handle as *mut LaneSession, seq as u64)
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
    send_chat_inner(&mut env, handle, to, message_id, body, None, false, 0)
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneSession_nativeSendChatWithMedia(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    to: JString,
    message_id: JString,
    body: JByteArray,
    media_id: JString,
) -> jlong {
    let Ok(media) = jstring_to_string(&mut env, &media_id) else {
        return -1;
    };
    send_chat_inner(&mut env, handle, to, message_id, body, Some(media), false, 0)
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneSession_nativeSendChatRetry(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    to: JString,
    message_id: JString,
    body: JByteArray,
    max_attempts: jint,
) -> jlong {
    send_chat_inner(
        &mut env,
        handle,
        to,
        message_id,
        body,
        None,
        true,
        max_attempts as u32,
    )
}

fn send_chat_inner(
    env: &mut JNIEnv<'_>,
    handle: jlong,
    to: JString,
    message_id: JString,
    body: JByteArray,
    media_id: Option<String>,
    retry: bool,
    max_attempts: u32,
) -> jlong {
    clear_last_error();
    let Ok(to) = jstring_to_string(env, &to) else {
        set_last_error("invalid to");
        return -1;
    };
    let Ok(message_id) = jstring_to_string(env, &message_id) else {
        set_last_error("invalid message_id");
        return -1;
    };
    let Ok(bytes) = env.convert_byte_array(&body) else {
        set_last_error("invalid body");
        return -1;
    };
    let Some(to_c) = cstr_owned(to) else {
        set_last_error("to contains NUL");
        return -1;
    };
    let Some(mid_c) = cstr_owned(message_id) else {
        set_last_error("message_id contains NUL");
        return -1;
    };
    let mut seq: u64 = 0;
    let code = if retry {
        lane_send_chat_retry(
            handle as *mut LaneSession,
            to_c.as_ptr(),
            mid_c.as_ptr(),
            bytes.as_ptr(),
            bytes.len(),
            max_attempts,
            &mut seq,
        )
    } else if let Some(media) = media_id {
        let Some(media_c) = cstr_owned(media) else {
            set_last_error("media_id contains NUL");
            return -1;
        };
        lane_send_chat_with_media(
            handle as *mut LaneSession,
            to_c.as_ptr(),
            mid_c.as_ptr(),
            bytes.as_ptr(),
            bytes.len(),
            media_c.as_ptr(),
            &mut seq,
        )
    } else {
        lane_send_chat(
            handle as *mut LaneSession,
            to_c.as_ptr(),
            mid_c.as_ptr(),
            bytes.as_ptr(),
            bytes.len(),
            &mut seq,
        )
    };
    if code != 0 {
        set_last_error(format!("send_chat failed code={code}"));
        return -1;
    }
    seq as jlong
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneSession_nativeAckDelivered(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    message_id: JString,
) -> jint {
    let Ok(mid) = jstring_to_string(&mut env, &message_id) else {
        return -1;
    };
    let Some(mid_c) = cstr_owned(mid) else {
        return -1;
    };
    lane_ack_delivered(handle as *mut LaneSession, mid_c.as_ptr())
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneSession_nativeAckRead(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    message_id: JString,
) -> jint {
    let Ok(mid) = jstring_to_string(&mut env, &message_id) else {
        return -1;
    };
    let Some(mid_c) = cstr_owned(mid) else {
        return -1;
    };
    lane_ack_read(handle as *mut LaneSession, mid_c.as_ptr())
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneSession_nativeSubscribePresence(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    contact_ids_csv: JString,
) -> jint {
    let Ok(csv) = jstring_to_string(&mut env, &contact_ids_csv) else {
        return -1;
    };
    let Some(csv_c) = cstr_owned(csv) else {
        return -1;
    };
    lane_subscribe_presence(handle as *mut LaneSession, csv_c.as_ptr())
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneSession_nativeSendPresence(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
    kind: jint,
) -> jint {
    lane_send_presence(handle as *mut LaneSession, kind)
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneSession_nativeCreateGroup(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    group_id: JString,
) -> jlong {
    versioned_group_op(&mut env, handle, group_id, None, GroupOp::Create)
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneSession_nativeAddMember(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    group_id: JString,
    user: JString,
) -> jlong {
    let Ok(user) = jstring_to_string(&mut env, &user) else {
        return -1;
    };
    versioned_group_op(&mut env, handle, group_id, Some(user), GroupOp::Add)
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneSession_nativeRemoveMember(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    group_id: JString,
    user: JString,
) -> jlong {
    let Ok(user) = jstring_to_string(&mut env, &user) else {
        return -1;
    };
    versioned_group_op(&mut env, handle, group_id, Some(user), GroupOp::Remove)
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneSession_nativeLeaveGroup(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    group_id: JString,
) -> jlong {
    versioned_group_op(&mut env, handle, group_id, None, GroupOp::Leave)
}

enum GroupOp {
    Create,
    Add,
    Remove,
    Leave,
}

fn versioned_group_op(
    env: &mut JNIEnv<'_>,
    handle: jlong,
    group_id: JString,
    user: Option<String>,
    op: GroupOp,
) -> jlong {
    clear_last_error();
    let Ok(gid) = jstring_to_string(env, &group_id) else {
        set_last_error("invalid group_id");
        return -1;
    };
    let Some(gid_c) = cstr_owned(gid) else {
        set_last_error("group_id contains NUL");
        return -1;
    };
    let mut version: u64 = 0;
    let code = match op {
        GroupOp::Create => {
            lane_create_group(handle as *mut LaneSession, gid_c.as_ptr(), &mut version)
        }
        GroupOp::Add => {
            let Some(user) = user else {
                return -1;
            };
            let Some(user_c) = cstr_owned(user) else {
                return -1;
            };
            lane_add_member(
                handle as *mut LaneSession,
                gid_c.as_ptr(),
                user_c.as_ptr(),
                &mut version,
            )
        }
        GroupOp::Remove => {
            let Some(user) = user else {
                return -1;
            };
            let Some(user_c) = cstr_owned(user) else {
                return -1;
            };
            lane_remove_member(
                handle as *mut LaneSession,
                gid_c.as_ptr(),
                user_c.as_ptr(),
                &mut version,
            )
        }
        GroupOp::Leave => {
            lane_leave_group(handle as *mut LaneSession, gid_c.as_ptr(), &mut version)
        }
    };
    if code != 0 {
        set_last_error(format!("group op failed code={code}"));
        return -1;
    }
    version as jlong
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneSession_nativeSendGroup(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    group_id: JString,
    message_id: JString,
    body: JByteArray,
) -> jint {
    send_group_inner(&mut env, handle, group_id, message_id, body, None)
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneSession_nativeSendGroupWithMedia(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    group_id: JString,
    message_id: JString,
    body: JByteArray,
    media_id: JString,
) -> jint {
    let Ok(media) = jstring_to_string(&mut env, &media_id) else {
        return -1;
    };
    send_group_inner(&mut env, handle, group_id, message_id, body, Some(media))
}

fn send_group_inner(
    env: &mut JNIEnv<'_>,
    handle: jlong,
    group_id: JString,
    message_id: JString,
    body: JByteArray,
    media_id: Option<String>,
) -> jint {
    clear_last_error();
    let Ok(gid) = jstring_to_string(env, &group_id) else {
        return -1;
    };
    let Ok(mid) = jstring_to_string(env, &message_id) else {
        return -1;
    };
    let Ok(bytes) = env.convert_byte_array(&body) else {
        return -1;
    };
    let Some(gid_c) = cstr_owned(gid) else {
        return -1;
    };
    let Some(mid_c) = cstr_owned(mid) else {
        return -1;
    };
    let code = if let Some(media) = media_id {
        let Some(media_c) = cstr_owned(media) else {
            return -1;
        };
        lane_send_group_with_media(
            handle as *mut LaneSession,
            gid_c.as_ptr(),
            mid_c.as_ptr(),
            bytes.as_ptr(),
            bytes.len(),
            media_c.as_ptr(),
        )
    } else {
        lane_send_group(
            handle as *mut LaneSession,
            gid_c.as_ptr(),
            mid_c.as_ptr(),
            bytes.as_ptr(),
            bytes.len(),
        )
    };
    if code != 0 {
        set_last_error(format!("send_group failed code={code}"));
    }
    code
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneSession_nativeUploadMedia(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    media_id: JString,
    file_name: JString,
    mime_type: JString,
    data: JByteArray,
) -> jlong {
    clear_last_error();
    let Ok(media_id) = jstring_to_string(&mut env, &media_id) else {
        return -1;
    };
    let Ok(file_name) = jstring_to_string(&mut env, &file_name) else {
        return -1;
    };
    let Ok(mime_type) = jstring_to_string(&mut env, &mime_type) else {
        return -1;
    };
    let Ok(bytes) = env.convert_byte_array(&data) else {
        return -1;
    };
    let Some(media_c) = cstr_owned(media_id) else {
        return -1;
    };
    let Some(name_c) = cstr_owned(file_name) else {
        return -1;
    };
    let Some(mime_c) = cstr_owned(mime_type) else {
        return -1;
    };
    let mut out: u64 = 0;
    let code = lane_upload_media(
        handle as *mut LaneSession,
        media_c.as_ptr(),
        name_c.as_ptr(),
        mime_c.as_ptr(),
        bytes.as_ptr(),
        bytes.len(),
        &mut out,
    );
    if code != 0 {
        set_last_error(format!("upload_media failed code={code}"));
        return -1;
    }
    out as jlong
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneSession_nativeFetchMedia(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    media_id: JString,
) -> jobjectArray {
    clear_last_error();
    let Ok(media_id) = jstring_to_string(&mut env, &media_id) else {
        return ptr::null_mut();
    };
    let Some(media_c) = cstr_owned(media_id) else {
        return ptr::null_mut();
    };
    let mut data: *mut u8 = ptr::null_mut();
    let mut len: usize = 0;
    let mut file_name: *mut std::os::raw::c_char = ptr::null_mut();
    let mut mime: *mut std::os::raw::c_char = ptr::null_mut();
    let mut sha: *mut std::os::raw::c_char = ptr::null_mut();
    let code = lane_fetch_media(
        handle as *mut LaneSession,
        media_c.as_ptr(),
        &mut data,
        &mut len,
        &mut file_name,
        &mut mime,
        &mut sha,
    );
    if code != 0 {
        set_last_error(format!("fetch_media failed code={code}"));
        return ptr::null_mut();
    }
    let bytes = if data.is_null() || len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(data, len).to_vec() }
    };
    if !data.is_null() {
        lane_bytes_free(data, len);
    }
    let name = take_c_string(file_name).unwrap_or_default();
    let mime_s = take_c_string(mime).unwrap_or_default();
    let sha_s = take_c_string(sha).unwrap_or_default();

    let obj_class = match env.find_class("java/lang/Object") {
        Ok(c) => c,
        Err(_) => return ptr::null_mut(),
    };
    let Ok(arr) = env.new_object_array(4, &obj_class, JObject::null()) else {
        return ptr::null_mut();
    };
    let Ok(jbytes) = env.byte_array_from_slice(&bytes) else {
        return ptr::null_mut();
    };
    let Ok(jname) = env.new_string(name) else {
        return ptr::null_mut();
    };
    let Ok(jmime) = env.new_string(mime_s) else {
        return ptr::null_mut();
    };
    let Ok(jsha) = env.new_string(sha_s) else {
        return ptr::null_mut();
    };
    let _ = env.set_object_array_element(&arr, 0, jbytes);
    let _ = env.set_object_array_element(&arr, 1, jname);
    let _ = env.set_object_array_element(&arr, 2, jmime);
    let _ = env.set_object_array_element(&arr, 3, jsha);
    arr.into_raw()
}

// ---- E2EE -------------------------------------------------------------------

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneE2eeDevice_nativeGenerate(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    lane_e2ee_generate() as jlong
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneE2eeDevice_nativeFree(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
) {
    lane_e2ee_free(handle as *mut LaneE2eeDevice);
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneE2eeDevice_nativeIdentityKey(
    env: JNIEnv,
    _this: JObject,
    handle: jlong,
) -> jstring {
    let mut out: *mut std::os::raw::c_char = ptr::null_mut();
    let code = lane_e2ee_identity_key(handle as *mut LaneE2eeDevice, &mut out);
    if code != 0 || out.is_null() {
        set_last_error(format!("identity_key failed code={code}"));
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
pub extern "system" fn Java_com_lane_messenger_LaneE2eeDevice_nativeSafetyNumber(
    mut env: JNIEnv,
    _class: JClass,
    local_b64: JString,
    remote_b64: JString,
) -> jstring {
    let Ok(local) = jstring_to_string(&mut env, &local_b64) else {
        return ptr::null_mut();
    };
    let Ok(remote) = jstring_to_string(&mut env, &remote_b64) else {
        return ptr::null_mut();
    };
    let Some(local_c) = cstr_owned(local) else {
        return ptr::null_mut();
    };
    let Some(remote_c) = cstr_owned(remote) else {
        return ptr::null_mut();
    };
    let mut out: *mut std::os::raw::c_char = ptr::null_mut();
    let code = lane_e2ee_safety_number(local_c.as_ptr(), remote_c.as_ptr(), &mut out);
    if code != 0 || out.is_null() {
        set_last_error(format!("safety_number failed code={code}"));
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
pub extern "system" fn Java_com_lane_messenger_LaneE2eeDevice_nativePublish(
    mut env: JNIEnv,
    _this: JObject,
    e2ee: jlong,
    session: jlong,
    device_id: JString,
    otk_count: jint,
) -> jint {
    let Ok(device_id) = jstring_to_string(&mut env, &device_id) else {
        return -1;
    };
    let Some(dev_c) = cstr_owned(device_id) else {
        return -1;
    };
    lane_e2ee_publish(
        session as *mut LaneSession,
        e2ee as *mut LaneE2eeDevice,
        dev_c.as_ptr(),
        otk_count as u32,
    )
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneE2eeDevice_nativeSendEncryptedChat(
    mut env: JNIEnv,
    _this: JObject,
    e2ee: jlong,
    session: jlong,
    to: JString,
    message_id: JString,
    plaintext: JByteArray,
) -> jlong {
    clear_last_error();
    let Ok(to) = jstring_to_string(&mut env, &to) else {
        return -1;
    };
    let Ok(mid) = jstring_to_string(&mut env, &message_id) else {
        return -1;
    };
    let Ok(bytes) = env.convert_byte_array(&plaintext) else {
        return -1;
    };
    let Some(to_c) = cstr_owned(to) else {
        return -1;
    };
    let Some(mid_c) = cstr_owned(mid) else {
        return -1;
    };
    let mut seq: u64 = 0;
    let code = lane_send_encrypted_chat(
        session as *mut LaneSession,
        e2ee as *mut LaneE2eeDevice,
        to_c.as_ptr(),
        mid_c.as_ptr(),
        bytes.as_ptr(),
        bytes.len(),
        &mut seq,
    );
    if code != 0 {
        set_last_error(format!("send_encrypted_chat failed code={code}"));
        return -1;
    }
    seq as jlong
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneE2eeDevice_nativeDecryptChat(
    mut env: JNIEnv,
    _this: JObject,
    e2ee: jlong,
    from_user: JString,
    body: JByteArray,
) -> JByteArrayRaw {
    clear_last_error();
    let Ok(from) = jstring_to_string(&mut env, &from_user) else {
        return ptr::null_mut();
    };
    let Ok(bytes) = env.convert_byte_array(&body) else {
        return ptr::null_mut();
    };
    let Some(from_c) = cstr_owned(from) else {
        return ptr::null_mut();
    };
    let mut out: *mut u8 = ptr::null_mut();
    let mut out_len: usize = 0;
    let code = lane_decrypt_chat(
        e2ee as *mut LaneE2eeDevice,
        from_c.as_ptr(),
        bytes.as_ptr(),
        bytes.len(),
        &mut out,
        &mut out_len,
    );
    if code != 0 {
        set_last_error(format!("decrypt_chat failed code={code}"));
        return ptr::null_mut();
    }
    let plain = if out.is_null() || out_len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(out, out_len).to_vec() }
    };
    if !out.is_null() {
        lane_bytes_free(out, out_len);
    }
    env.byte_array_from_slice(&plain)
        .map(|a| a.into_raw())
        .unwrap_or(ptr::null_mut())
}

/// Local alias so we can name the JNI return type without importing the sys alias clash.
type JByteArrayRaw = jni::sys::jbyteArray;

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneE2eeDevice_nativeExportPickle(
    mut env: JNIEnv,
    _this: JObject,
    e2ee: jlong,
    passphrase: JString,
) -> JByteArrayRaw {
    let Ok(pass) = jstring_to_string(&mut env, &passphrase) else {
        return ptr::null_mut();
    };
    let Some(pass_c) = cstr_owned(pass) else {
        return ptr::null_mut();
    };
    let mut out: *mut u8 = ptr::null_mut();
    let mut out_len: usize = 0;
    let code = lane_e2ee_export_pickle(
        e2ee as *mut LaneE2eeDevice,
        pass_c.as_ptr(),
        &mut out,
        &mut out_len,
    );
    if code != 0 {
        set_last_error(format!("export_pickle failed code={code}"));
        return ptr::null_mut();
    }
    let bytes = if out.is_null() || out_len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(out, out_len).to_vec() }
    };
    if !out.is_null() {
        lane_bytes_free(out, out_len);
    }
    env.byte_array_from_slice(&bytes)
        .map(|a| a.into_raw())
        .unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneE2eeDevice_nativeImportPickle(
    mut env: JNIEnv,
    _class: JClass,
    pickle: JByteArray,
    passphrase: JString,
) -> jlong {
    let Ok(bytes) = env.convert_byte_array(&pickle) else {
        return 0;
    };
    let Ok(pass) = jstring_to_string(&mut env, &passphrase) else {
        return 0;
    };
    let Some(pass_c) = cstr_owned(pass) else {
        return 0;
    };
    let ptr = lane_e2ee_import_pickle(bytes.as_ptr(), bytes.len(), pass_c.as_ptr());
    if ptr.is_null() {
        set_last_error("import_pickle failed");
        return 0;
    }
    ptr as jlong
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneE2eeDevice_nativeCreateGroupSession(
    mut env: JNIEnv,
    _this: JObject,
    e2ee: jlong,
    group_id: JString,
) -> jstring {
    let Ok(gid) = jstring_to_string(&mut env, &group_id) else {
        return ptr::null_mut();
    };
    let Some(gid_c) = cstr_owned(gid) else {
        return ptr::null_mut();
    };
    let mut out: *mut std::os::raw::c_char = ptr::null_mut();
    let code =
        lane_e2ee_create_group_session(e2ee as *mut LaneE2eeDevice, gid_c.as_ptr(), &mut out);
    if code != 0 || out.is_null() {
        set_last_error(format!("create_group_session failed code={code}"));
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
pub extern "system" fn Java_com_lane_messenger_LaneE2eeDevice_nativeDistributeGroupKey(
    mut env: JNIEnv,
    _this: JObject,
    e2ee: jlong,
    session: jlong,
    group_id: JString,
    members_csv: JString,
) -> jint {
    let Ok(gid) = jstring_to_string(&mut env, &group_id) else {
        return -1;
    };
    let Ok(members) = jstring_to_string(&mut env, &members_csv) else {
        return -1;
    };
    let Some(gid_c) = cstr_owned(gid) else {
        return -1;
    };
    let Some(mem_c) = cstr_owned(members) else {
        return -1;
    };
    lane_e2ee_distribute_group_key(
        session as *mut LaneSession,
        e2ee as *mut LaneE2eeDevice,
        gid_c.as_ptr(),
        mem_c.as_ptr(),
    )
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneE2eeDevice_nativeSendEncryptedGroup(
    mut env: JNIEnv,
    _this: JObject,
    e2ee: jlong,
    session: jlong,
    group_id: JString,
    message_id: JString,
    plaintext: JByteArray,
) -> jint {
    let Ok(gid) = jstring_to_string(&mut env, &group_id) else {
        return -1;
    };
    let Ok(mid) = jstring_to_string(&mut env, &message_id) else {
        return -1;
    };
    let Ok(bytes) = env.convert_byte_array(&plaintext) else {
        return -1;
    };
    let Some(gid_c) = cstr_owned(gid) else {
        return -1;
    };
    let Some(mid_c) = cstr_owned(mid) else {
        return -1;
    };
    lane_send_encrypted_group(
        session as *mut LaneSession,
        e2ee as *mut LaneE2eeDevice,
        gid_c.as_ptr(),
        mid_c.as_ptr(),
        bytes.as_ptr(),
        bytes.len(),
    )
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneE2eeDevice_nativeDecryptGroup(
    mut env: JNIEnv,
    _this: JObject,
    e2ee: jlong,
    group_id: JString,
    body: JByteArray,
) -> JByteArrayRaw {
    let Ok(gid) = jstring_to_string(&mut env, &group_id) else {
        return ptr::null_mut();
    };
    let Ok(bytes) = env.convert_byte_array(&body) else {
        return ptr::null_mut();
    };
    let Some(gid_c) = cstr_owned(gid) else {
        return ptr::null_mut();
    };
    let mut out: *mut u8 = ptr::null_mut();
    let mut out_len: usize = 0;
    let code = lane_decrypt_group(
        e2ee as *mut LaneE2eeDevice,
        gid_c.as_ptr(),
        bytes.as_ptr(),
        bytes.len(),
        &mut out,
        &mut out_len,
    );
    if code != 0 {
        set_last_error(format!("decrypt_group failed code={code}"));
        return ptr::null_mut();
    }
    let plain = if out.is_null() || out_len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(out, out_len).to_vec() }
    };
    if !out.is_null() {
        lane_bytes_free(out, out_len);
    }
    env.byte_array_from_slice(&plain)
        .map(|a| a.into_raw())
        .unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "system" fn Java_com_lane_messenger_LaneE2eeDevice_nativeTryImportGroupKey(
    mut env: JNIEnv,
    _this: JObject,
    e2ee: jlong,
    from_user: JString,
    body: JByteArray,
) -> jint {
    let Ok(from) = jstring_to_string(&mut env, &from_user) else {
        return -1;
    };
    let Ok(bytes) = env.convert_byte_array(&body) else {
        return -1;
    };
    let Some(from_c) = cstr_owned(from) else {
        return -1;
    };
    let mut imported: i32 = 0;
    let code = lane_e2ee_try_import_group_key(
        e2ee as *mut LaneE2eeDevice,
        from_c.as_ptr(),
        bytes.as_ptr(),
        bytes.len(),
        &mut imported,
    );
    if code != 0 {
        return -1;
    }
    imported
}

