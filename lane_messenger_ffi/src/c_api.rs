//! C ABI for Flutter / JNI / custom hosts (`feature = "c-api"`).
//!
//! Ownership: pointers returned by `lane_*_connect` / `lane_e2ee_generate` must
//! be freed with the matching `*_free`. Strings from out-params use
//! `lane_string_free`.

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::ptr;
use std::slice;

use crate::error::FfiErrorCode;
use crate::events::LaneEvent;
use crate::runtime;
use crate::session::{ConnectOptions, SessionHandle};
use crate::E2eeHandle;

/// Opaque session.
pub struct LaneSession {
    inner: SessionHandle,
}

/// Opaque E2EE device.
pub struct LaneE2eeDevice {
    inner: E2eeHandle,
}

fn set_err(out: *mut *mut c_char, msg: &str) {
    if out.is_null() {
        return;
    }
    unsafe {
        *out = match CString::new(msg) {
            Ok(s) => s.into_raw(),
            Err(_) => ptr::null_mut(),
        };
    }
}

fn catch_code(f: impl FnOnce() -> c_int) -> c_int {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(code) => code,
        Err(_) => FfiErrorCode::Internal.as_i32(),
    }
}

fn cstr<'a>(p: *const c_char) -> Result<&'a str, FfiErrorCode> {
    if p.is_null() {
        return Err(FfiErrorCode::InvalidArgument);
    }
    unsafe { CStr::from_ptr(p) }
        .to_str()
        .map_err(|_| FfiErrorCode::InvalidArgument)
}

fn bytes_from(ptr: *const u8, len: usize) -> Result<&'static [u8], FfiErrorCode> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err(FfiErrorCode::InvalidArgument);
    }
    Ok(unsafe { slice::from_raw_parts(ptr, len) })
}

/// Library semver (static C string).
#[no_mangle]
pub extern "C" fn lane_version() -> *const c_char {
    static VER: once_cell::sync::Lazy<CString> =
        once_cell::sync::Lazy::new(|| CString::new(crate::VERSION).unwrap());
    VER.as_ptr()
}

#[no_mangle]
pub extern "C" fn lane_protocol_version() -> u8 {
    crate::PROTOCOL_VERSION
}

#[no_mangle]
pub extern "C" fn lane_string_free(s: *mut c_char) {
    if s.is_null() {
        return;
    }
    unsafe {
        drop(CString::from_raw(s));
    }
}

#[no_mangle]
pub extern "C" fn lane_bytes_free(p: *mut u8, len: usize) {
    if p.is_null() || len == 0 {
        return;
    }
    unsafe {
        drop(Vec::from_raw_parts(p, len, len));
    }
}

/// Connect + login. On success returns non-null session; on failure returns null
/// and sets `err_out` (caller frees with `lane_string_free`).
#[no_mangle]
pub extern "C" fn lane_session_connect(
    host: *const c_char,
    port: u16,
    use_tls: c_int,
    user_id: *const c_char,
    device_id: *const c_char,
    auth_token: *const c_char,
    client_version: *const c_char,
    resume_after_seq: u64,
    ping_interval_secs: u64,
    err_out: *mut *mut c_char,
) -> *mut LaneSession {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let run = || -> Result<*mut LaneSession, String> {
            let host = cstr(host).map_err(|_| "host".to_string())?;
            let user_id = cstr(user_id).map_err(|_| "user_id".to_string())?;
            let device_id = cstr(device_id).map_err(|_| "device_id".to_string())?;
            let auth_token = cstr(auth_token).map_err(|_| "auth_token".to_string())?;
            let client_version = if client_version.is_null() {
                format!("ffi-{}", crate::VERSION)
            } else {
                cstr(client_version)
                    .map_err(|_| "client_version".to_string())?
                    .to_string()
            };
            let opts = ConnectOptions {
                host: host.into(),
                port,
                use_tls: use_tls != 0,
                user_id: user_id.into(),
                device_id: device_id.into(),
                auth_token: auth_token.into(),
                client_version,
                resume_after_seq,
                ping_interval_secs,
                ..Default::default()
            };
            let session = SessionHandle::connect(opts).map_err(|e| e.detail())?;
            Ok(Box::into_raw(Box::new(LaneSession { inner: session })))
        };
        match run() {
            Ok(p) => p,
            Err(e) => {
                set_err(err_out, &e);
                ptr::null_mut()
            }
        }
    })) {
        Ok(p) => p,
        Err(_) => {
            set_err(err_out, "internal panic");
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn lane_session_free(session: *mut LaneSession) {
    if session.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(session));
    }
}

#[no_mangle]
pub extern "C" fn lane_session_close(session: *mut LaneSession) -> c_int {
    let Some(s) = (unsafe { session.as_ref() }) else {
        return FfiErrorCode::InvalidArgument.as_i32();
    };
    match s.inner.close() {
        Ok(()) => FfiErrorCode::Ok.as_i32(),
        Err(e) => e.code().as_i32(),
    }
}

#[no_mangle]
pub extern "C" fn lane_session_ping(session: *mut LaneSession) -> c_int {
    catch_code(|| {
        let Some(s) = (unsafe { session.as_ref() }) else {
            return FfiErrorCode::InvalidArgument.as_i32();
        };
        match s.inner.ping() {
            Ok(()) => FfiErrorCode::Ok.as_i32(),
            Err(e) => e.code().as_i32(),
        }
    })
}

/// Poll one event as a JSON-ish UTF-8 string. Returns 1 if an event was written
/// to `out_json` (caller frees), 0 if timeout/empty, negative on error.
#[no_mangle]
pub extern "C" fn lane_session_poll_event(
    session: *mut LaneSession,
    timeout_ms: u64,
    out_json: *mut *mut c_char,
) -> c_int {
    let Some(s) = (unsafe { session.as_ref() }) else {
        return FfiErrorCode::InvalidArgument.as_i32();
    };
    if out_json.is_null() {
        return FfiErrorCode::InvalidArgument.as_i32();
    }
    match s.inner.poll_event(timeout_ms) {
        Some(ev) => {
            let json = event_to_json(&ev);
            set_err(out_json, &json);
            1
        }
        None => 0,
    }
}

#[no_mangle]
pub extern "C" fn lane_session_set_resume_seq(session: *mut LaneSession, seq: u64) -> c_int {
    let Some(s) = (unsafe { session.as_ref() }) else {
        return FfiErrorCode::InvalidArgument.as_i32();
    };
    match s.inner.set_resume_seq(seq) {
        Ok(()) => FfiErrorCode::Ok.as_i32(),
        Err(e) => e.code().as_i32(),
    }
}

#[no_mangle]
pub extern "C" fn lane_send_chat(
    session: *mut LaneSession,
    to_user: *const c_char,
    message_id: *const c_char,
    body: *const u8,
    body_len: usize,
    out_seq: *mut u64,
) -> c_int {
    let Some(s) = (unsafe { session.as_ref() }) else {
        return FfiErrorCode::InvalidArgument.as_i32();
    };
    let to = match cstr(to_user) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    let mid = match cstr(message_id) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    let body = match bytes_from(body, body_len) {
        Ok(b) => b,
        Err(c) => return c.as_i32(),
    };
    match s.inner.send_chat(to, mid, body) {
        Ok(seq) => {
            if !out_seq.is_null() {
                unsafe { *out_seq = seq };
            }
            FfiErrorCode::Ok.as_i32()
        }
        Err(e) => e.code().as_i32(),
    }
}

#[no_mangle]
pub extern "C" fn lane_send_chat_with_media(
    session: *mut LaneSession,
    to_user: *const c_char,
    message_id: *const c_char,
    body: *const u8,
    body_len: usize,
    media_id: *const c_char,
    out_seq: *mut u64,
) -> c_int {
    let Some(s) = (unsafe { session.as_ref() }) else {
        return FfiErrorCode::InvalidArgument.as_i32();
    };
    let to = match cstr(to_user) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    let mid = match cstr(message_id) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    let media = match cstr(media_id) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    let body = match bytes_from(body, body_len) {
        Ok(b) => b,
        Err(c) => return c.as_i32(),
    };
    match s.inner.send_chat_with_media(to, mid, body, media) {
        Ok(seq) => {
            if !out_seq.is_null() {
                unsafe { *out_seq = seq };
            }
            FfiErrorCode::Ok.as_i32()
        }
        Err(e) => e.code().as_i32(),
    }
}

#[no_mangle]
pub extern "C" fn lane_send_chat_retry(
    session: *mut LaneSession,
    to_user: *const c_char,
    message_id: *const c_char,
    body: *const u8,
    body_len: usize,
    max_attempts: u32,
    out_seq: *mut u64,
) -> c_int {
    let Some(s) = (unsafe { session.as_ref() }) else {
        return FfiErrorCode::InvalidArgument.as_i32();
    };
    let to = match cstr(to_user) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    let mid = match cstr(message_id) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    let body = match bytes_from(body, body_len) {
        Ok(b) => b,
        Err(c) => return c.as_i32(),
    };
    match s.inner.send_chat_with_retry(to, mid, body, max_attempts) {
        Ok(seq) => {
            if !out_seq.is_null() {
                unsafe { *out_seq = seq };
            }
            FfiErrorCode::Ok.as_i32()
        }
        Err(e) => e.code().as_i32(),
    }
}

#[no_mangle]
pub extern "C" fn lane_ack_delivered(session: *mut LaneSession, message_id: *const c_char) -> c_int {
    let Some(s) = (unsafe { session.as_ref() }) else {
        return FfiErrorCode::InvalidArgument.as_i32();
    };
    let mid = match cstr(message_id) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    match s.inner.ack_delivered(mid) {
        Ok(()) => FfiErrorCode::Ok.as_i32(),
        Err(e) => e.code().as_i32(),
    }
}

#[no_mangle]
pub extern "C" fn lane_ack_read(session: *mut LaneSession, message_id: *const c_char) -> c_int {
    let Some(s) = (unsafe { session.as_ref() }) else {
        return FfiErrorCode::InvalidArgument.as_i32();
    };
    let mid = match cstr(message_id) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    match s.inner.ack_read(mid) {
        Ok(()) => FfiErrorCode::Ok.as_i32(),
        Err(e) => e.code().as_i32(),
    }
}

#[no_mangle]
pub extern "C" fn lane_subscribe_presence(
    session: *mut LaneSession,
    contact_ids_csv: *const c_char,
) -> c_int {
    let Some(s) = (unsafe { session.as_ref() }) else {
        return FfiErrorCode::InvalidArgument.as_i32();
    };
    let csv = match cstr(contact_ids_csv) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    let ids: Vec<String> = if csv.is_empty() {
        Vec::new()
    } else {
        csv.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()
    };
    match s.inner.subscribe_presence(&ids) {
        Ok(()) => FfiErrorCode::Ok.as_i32(),
        Err(e) => e.code().as_i32(),
    }
}

#[no_mangle]
pub extern "C" fn lane_send_presence(session: *mut LaneSession, kind: c_int) -> c_int {
    let Some(s) = (unsafe { session.as_ref() }) else {
        return FfiErrorCode::InvalidArgument.as_i32();
    };
    match s.inner.send_presence(kind) {
        Ok(()) => FfiErrorCode::Ok.as_i32(),
        Err(e) => e.code().as_i32(),
    }
}

#[no_mangle]
pub extern "C" fn lane_create_group(
    session: *mut LaneSession,
    group_id: *const c_char,
    out_version: *mut u64,
) -> c_int {
    let Some(s) = (unsafe { session.as_ref() }) else {
        return FfiErrorCode::InvalidArgument.as_i32();
    };
    let gid = match cstr(group_id) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    match s.inner.create_group(gid) {
        Ok(v) => {
            if !out_version.is_null() {
                unsafe { *out_version = v };
            }
            FfiErrorCode::Ok.as_i32()
        }
        Err(e) => e.code().as_i32(),
    }
}

#[no_mangle]
pub extern "C" fn lane_add_member(
    session: *mut LaneSession,
    group_id: *const c_char,
    user: *const c_char,
    out_version: *mut u64,
) -> c_int {
    let Some(s) = (unsafe { session.as_ref() }) else {
        return FfiErrorCode::InvalidArgument.as_i32();
    };
    let gid = match cstr(group_id) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    let user = match cstr(user) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    match s.inner.add_member(gid, user) {
        Ok(v) => {
            if !out_version.is_null() {
                unsafe { *out_version = v };
            }
            FfiErrorCode::Ok.as_i32()
        }
        Err(e) => e.code().as_i32(),
    }
}

#[no_mangle]
pub extern "C" fn lane_remove_member(
    session: *mut LaneSession,
    group_id: *const c_char,
    user: *const c_char,
    out_version: *mut u64,
) -> c_int {
    let Some(s) = (unsafe { session.as_ref() }) else {
        return FfiErrorCode::InvalidArgument.as_i32();
    };
    let gid = match cstr(group_id) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    let user = match cstr(user) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    match s.inner.remove_member(gid, user) {
        Ok(v) => {
            if !out_version.is_null() {
                unsafe { *out_version = v };
            }
            FfiErrorCode::Ok.as_i32()
        }
        Err(e) => e.code().as_i32(),
    }
}

#[no_mangle]
pub extern "C" fn lane_leave_group(
    session: *mut LaneSession,
    group_id: *const c_char,
    out_version: *mut u64,
) -> c_int {
    let Some(s) = (unsafe { session.as_ref() }) else {
        return FfiErrorCode::InvalidArgument.as_i32();
    };
    let gid = match cstr(group_id) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    match s.inner.leave_group(gid) {
        Ok(v) => {
            if !out_version.is_null() {
                unsafe { *out_version = v };
            }
            FfiErrorCode::Ok.as_i32()
        }
        Err(e) => e.code().as_i32(),
    }
}

#[no_mangle]
pub extern "C" fn lane_send_group(
    session: *mut LaneSession,
    group_id: *const c_char,
    message_id: *const c_char,
    body: *const u8,
    body_len: usize,
) -> c_int {
    let Some(s) = (unsafe { session.as_ref() }) else {
        return FfiErrorCode::InvalidArgument.as_i32();
    };
    let gid = match cstr(group_id) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    let mid = match cstr(message_id) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    let body = match bytes_from(body, body_len) {
        Ok(b) => b,
        Err(c) => return c.as_i32(),
    };
    match s.inner.send_group(gid, mid, body) {
        Ok(()) => FfiErrorCode::Ok.as_i32(),
        Err(e) => e.code().as_i32(),
    }
}

#[no_mangle]
pub extern "C" fn lane_upload_media(
    session: *mut LaneSession,
    media_id: *const c_char,
    file_name: *const c_char,
    mime_type: *const c_char,
    data: *const u8,
    data_len: usize,
    out_bytes: *mut u64,
) -> c_int {
    let Some(s) = (unsafe { session.as_ref() }) else {
        return FfiErrorCode::InvalidArgument.as_i32();
    };
    let media_id = match cstr(media_id) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    let file_name = match cstr(file_name) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    let mime_type = match cstr(mime_type) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    let data = match bytes_from(data, data_len) {
        Ok(b) => b,
        Err(c) => return c.as_i32(),
    };
    match s.inner.upload_media(media_id, file_name, mime_type, data) {
        Ok(n) => {
            if !out_bytes.is_null() {
                unsafe { *out_bytes = n };
            }
            FfiErrorCode::Ok.as_i32()
        }
        Err(e) => e.code().as_i32(),
    }
}

#[no_mangle]
pub extern "C" fn lane_fetch_media(
    session: *mut LaneSession,
    media_id: *const c_char,
    out_data: *mut *mut u8,
    out_len: *mut usize,
    out_file_name: *mut *mut c_char,
    out_mime: *mut *mut c_char,
    out_sha: *mut *mut c_char,
) -> c_int {
    let Some(s) = (unsafe { session.as_ref() }) else {
        return FfiErrorCode::InvalidArgument.as_i32();
    };
    let media_id = match cstr(media_id) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    match s.inner.fetch_media(media_id) {
        Ok(m) => {
            if !out_file_name.is_null() {
                set_err(out_file_name, &m.file_name);
            }
            if !out_mime.is_null() {
                set_err(out_mime, &m.mime_type);
            }
            if !out_sha.is_null() {
                set_err(out_sha, &m.sha256);
            }
            if !out_data.is_null() && !out_len.is_null() {
                let mut buf = m.data;
                let len = buf.len();
                let ptr = buf.as_mut_ptr();
                std::mem::forget(buf);
                unsafe {
                    *out_data = ptr;
                    *out_len = len;
                }
            }
            FfiErrorCode::Ok.as_i32()
        }
        Err(e) => e.code().as_i32(),
    }
}

#[no_mangle]
pub extern "C" fn lane_e2ee_generate() -> *mut LaneE2eeDevice {
    Box::into_raw(Box::new(LaneE2eeDevice {
        inner: E2eeHandle::generate(),
    }))
}

#[no_mangle]
pub extern "C" fn lane_e2ee_free(device: *mut LaneE2eeDevice) {
    if device.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(device));
    }
}

#[no_mangle]
pub extern "C" fn lane_e2ee_identity_key(
    device: *mut LaneE2eeDevice,
    out_key: *mut *mut c_char,
) -> c_int {
    let Some(d) = (unsafe { device.as_ref() }) else {
        return FfiErrorCode::InvalidArgument.as_i32();
    };
    if out_key.is_null() {
        return FfiErrorCode::InvalidArgument.as_i32();
    }
    set_err(out_key, &d.inner.identity_key());
    FfiErrorCode::Ok.as_i32()
}

#[no_mangle]
pub extern "C" fn lane_e2ee_safety_number(
    local_b64: *const c_char,
    remote_b64: *const c_char,
    out: *mut *mut c_char,
) -> c_int {
    let local = match cstr(local_b64) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    let remote = match cstr(remote_b64) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    if out.is_null() {
        return FfiErrorCode::InvalidArgument.as_i32();
    }
    set_err(out, &E2eeHandle::safety_number(local, remote));
    FfiErrorCode::Ok.as_i32()
}

#[no_mangle]
pub extern "C" fn lane_e2ee_publish(
    session: *mut LaneSession,
    device: *mut LaneE2eeDevice,
    device_id: *const c_char,
    otk_count: u32,
) -> c_int {
    let Some(s) = (unsafe { session.as_ref() }) else {
        return FfiErrorCode::InvalidArgument.as_i32();
    };
    let Some(d) = (unsafe { device.as_ref() }) else {
        return FfiErrorCode::InvalidArgument.as_i32();
    };
    let device_id = match cstr(device_id) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    match d.inner.publish(&s.inner, device_id, otk_count) {
        Ok(()) => FfiErrorCode::Ok.as_i32(),
        Err(e) => e.code().as_i32(),
    }
}

#[no_mangle]
pub extern "C" fn lane_send_encrypted_chat(
    session: *mut LaneSession,
    device: *mut LaneE2eeDevice,
    to_user: *const c_char,
    message_id: *const c_char,
    plaintext: *const u8,
    plaintext_len: usize,
    out_seq: *mut u64,
) -> c_int {
    let Some(s) = (unsafe { session.as_ref() }) else {
        return FfiErrorCode::InvalidArgument.as_i32();
    };
    let Some(d) = (unsafe { device.as_ref() }) else {
        return FfiErrorCode::InvalidArgument.as_i32();
    };
    let to = match cstr(to_user) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    let mid = match cstr(message_id) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    let plain = match bytes_from(plaintext, plaintext_len) {
        Ok(b) => b,
        Err(c) => return c.as_i32(),
    };
    match d.inner.send_encrypted_chat(&s.inner, to, mid, plain) {
        Ok(seq) => {
            if !out_seq.is_null() {
                unsafe { *out_seq = seq };
            }
            FfiErrorCode::Ok.as_i32()
        }
        Err(e) => e.code().as_i32(),
    }
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

fn b64(data: &[u8]) -> String {
    // Minimal base64 without extra dep — hex is fine for FFI event bodies.
    data.iter().map(|b| format!("{b:02x}")).collect()
}

fn event_to_json(ev: &LaneEvent) -> String {
    match ev {
        LaneEvent::LoginAck(a) => format!(
            r#"{{"type":"LoginAck","session_id":"{}","pending_messages":{},"ok":{},"error":"{}"}}"#,
            json_escape(&a.session_id),
            a.pending_messages,
            a.ok,
            json_escape(&a.error)
        ),
        LaneEvent::SyncComplete(s) => format!(
            r#"{{"type":"SyncComplete","delivered":{},"latest_seq":{}}}"#,
            s.delivered, s.latest_seq
        ),
        LaneEvent::SyncMessage(inner) => {
            format!(r#"{{"type":"SyncMessage","inner":{}}}"#, event_to_json(inner))
        }
        LaneEvent::ChatMessage(m) => format!(
            r#"{{"type":"ChatMessage","message_id":"{}","from_user":"{}","to_user":"{}","body_hex":"{}","sent_at":{},"seq":{},"media_id":"{}"}}"#,
            json_escape(&m.message_id),
            json_escape(&m.from_user),
            json_escape(&m.to_user),
            b64(&m.body),
            m.sent_at,
            m.seq,
            json_escape(&m.media_id)
        ),
        LaneEvent::ServerAck(a) => format!(
            r#"{{"type":"ServerAck","message_id":"{}","seq":{}}}"#,
            json_escape(&a.message_id),
            a.seq
        ),
        LaneEvent::DeliveredAck(a) => format!(
            r#"{{"type":"DeliveredAck","message_id":"{}","from_user":"{}"}}"#,
            json_escape(&a.message_id),
            json_escape(&a.from_user)
        ),
        LaneEvent::ReadAck(a) => format!(
            r#"{{"type":"ReadAck","message_id":"{}","from_user":"{}"}}"#,
            json_escape(&a.message_id),
            json_escape(&a.from_user)
        ),
        LaneEvent::GroupAckSummary(s) => format!(
            r#"{{"type":"GroupAckSummary","message_id":"{}","group_id":"{}","member_count":{}}}"#,
            json_escape(&s.message_id),
            json_escape(&s.group_id),
            s.member_count
        ),
        LaneEvent::Presence(p) => format!(
            r#"{{"type":"Presence","user_id":"{}","kind":{},"last_seen":{}}}"#,
            json_escape(&p.user_id),
            p.kind,
            p.last_seen
        ),
        LaneEvent::GroupMessage(m) => format!(
            r#"{{"type":"GroupMessage","message_id":"{}","from_user":"{}","group_id":"{}","body_hex":"{}"}}"#,
            json_escape(&m.message_id),
            json_escape(&m.from_user),
            json_escape(&m.group_id),
            b64(&m.body)
        ),
        LaneEvent::GroupEvent(e) => format!(
            r#"{{"type":"GroupEvent","group_id":"{}","op":{},"version":{}}}"#,
            json_escape(&e.group_id),
            e.op,
            e.version
        ),
        LaneEvent::MediaStart(m) => format!(
            r#"{{"type":"MediaStart","media_id":"{}","file_name":"{}","total_size":{}}}"#,
            json_escape(&m.media_id),
            json_escape(&m.file_name),
            m.total_size
        ),
        LaneEvent::MediaChunk(c) => format!(
            r#"{{"type":"MediaChunk","media_id":"{}","offset":{},"last":{}}}"#,
            json_escape(&c.media_id),
            c.offset,
            c.last
        ),
        LaneEvent::MediaAck(a) => format!(
            r#"{{"type":"MediaAck","media_id":"{}","ok":{},"complete":{},"received_bytes":{}}}"#,
            json_escape(&a.media_id),
            a.ok,
            a.complete,
            a.received_bytes
        ),
        LaneEvent::KeyBundle(k) => format!(
            r#"{{"type":"KeyBundle","user_id":"{}","device_id":"{}","found":{}}}"#,
            json_escape(&k.user_id),
            json_escape(&k.device_id),
            k.found
        ),
        LaneEvent::ProtocolError(e) => format!(
            r#"{{"type":"ProtocolError","code":{},"detail":"{}"}}"#,
            e.code,
            json_escape(&e.detail)
        ),
        LaneEvent::Disconnected { reason } => {
            format!(r#"{{"type":"Disconnected","reason":"{}"}}"#, json_escape(reason))
        }
        LaneEvent::ReplacedByNewSession => r#"{"type":"ReplacedByNewSession"}"#.into(),
        LaneEvent::Pong { seq } => format!(r#"{{"type":"Pong","seq":{seq}}}"#),
    }
}

#[no_mangle]
pub extern "C" fn lane_decrypt_chat(
    device: *mut LaneE2eeDevice,
    from_user: *const c_char,
    body: *const u8,
    body_len: usize,
    out_plain: *mut *mut u8,
    out_len: *mut usize,
) -> c_int {
    let Some(d) = (unsafe { device.as_ref() }) else {
        return FfiErrorCode::InvalidArgument.as_i32();
    };
    let from = match cstr(from_user) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    let body = match bytes_from(body, body_len) {
        Ok(b) => b,
        Err(c) => return c.as_i32(),
    };
    match d.inner.decrypt_chat(from, body) {
        Ok(mut plain) => {
            if !out_plain.is_null() && !out_len.is_null() {
                let len = plain.len();
                let ptr = plain.as_mut_ptr();
                std::mem::forget(plain);
                unsafe {
                    *out_plain = ptr;
                    *out_len = len;
                }
            }
            FfiErrorCode::Ok.as_i32()
        }
        Err(e) => e.code().as_i32(),
    }
}

#[no_mangle]
pub extern "C" fn lane_e2ee_export_pickle(
    device: *mut LaneE2eeDevice,
    passphrase: *const c_char,
    out_bytes: *mut *mut u8,
    out_len: *mut usize,
) -> c_int {
    let Some(d) = (unsafe { device.as_ref() }) else {
        return FfiErrorCode::InvalidArgument.as_i32();
    };
    let pass = match cstr(passphrase) {
        Ok(t) => t,
        Err(c) => return c.as_i32(),
    };
    let mut bytes = d.inner.export_pickle(pass);
    if !out_bytes.is_null() && !out_len.is_null() {
        let len = bytes.len();
        let ptr = bytes.as_mut_ptr();
        std::mem::forget(bytes);
        unsafe {
            *out_bytes = ptr;
            *out_len = len;
        }
    }
    FfiErrorCode::Ok.as_i32()
}

#[no_mangle]
pub extern "C" fn lane_e2ee_import_pickle(
    bytes: *const u8,
    len: usize,
    passphrase: *const c_char,
) -> *mut LaneE2eeDevice {
    let pass = match cstr(passphrase) {
        Ok(t) => t,
        Err(_) => return ptr::null_mut(),
    };
    let data = match bytes_from(bytes, len) {
        Ok(b) => b,
        Err(_) => return ptr::null_mut(),
    };
    match E2eeHandle::import_pickle(data, pass) {
        Ok(inner) => Box::into_raw(Box::new(LaneE2eeDevice { inner })),
        Err(_) => ptr::null_mut(),
    }
}

/// Ensure the Tokio runtime is started (optional; first connect also starts it).
#[no_mangle]
pub extern "C" fn lane_runtime_init() {
    let _ = runtime::runtime();
}
