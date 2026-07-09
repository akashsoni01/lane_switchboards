#ifndef LANE_MESSENGER_FFI_H
#define LANE_MESSENGER_FFI_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct LaneSession LaneSession;
typedef struct LaneE2eeDevice LaneE2eeDevice;

const char *lane_version(void);
uint8_t lane_protocol_version(void);
void lane_string_free(char *s);
void lane_bytes_free(uint8_t *p, size_t len);
void lane_runtime_init(void);

LaneSession *lane_session_connect(
    const char *host,
    uint16_t port,
    int use_tls,
    const char *user_id,
    const char *device_id,
    const char *auth_token,
    const char *client_version,
    uint64_t resume_after_seq,
    uint64_t ping_interval_secs,
    char **err_out);

void lane_session_free(LaneSession *session);
int32_t lane_session_close(LaneSession *session);
int32_t lane_session_ping(LaneSession *session);
int32_t lane_session_poll_event(LaneSession *session, uint64_t timeout_ms, char **out_json);

int32_t lane_send_chat(
    LaneSession *session,
    const char *to_user,
    const char *message_id,
    const uint8_t *body,
    size_t body_len,
    uint64_t *out_seq);

int32_t lane_ack_delivered(LaneSession *session, const char *message_id);
int32_t lane_ack_read(LaneSession *session, const char *message_id);
int32_t lane_subscribe_presence(LaneSession *session, const char *contact_ids_csv);
int32_t lane_send_presence(LaneSession *session, int32_t kind);

int32_t lane_create_group(LaneSession *session, const char *group_id, uint64_t *out_version);
int32_t lane_add_member(
    LaneSession *session,
    const char *group_id,
    const char *user,
    uint64_t *out_version);
int32_t lane_send_group(
    LaneSession *session,
    const char *group_id,
    const char *message_id,
    const uint8_t *body,
    size_t body_len);

int32_t lane_upload_media(
    LaneSession *session,
    const char *media_id,
    const char *file_name,
    const char *mime_type,
    const uint8_t *data,
    size_t data_len,
    uint64_t *out_bytes);

LaneE2eeDevice *lane_e2ee_generate(void);
void lane_e2ee_free(LaneE2eeDevice *device);
int32_t lane_e2ee_identity_key(LaneE2eeDevice *device, char **out_key);
int32_t lane_e2ee_safety_number(const char *local_b64, const char *remote_b64, char **out);
int32_t lane_e2ee_publish(
    LaneSession *session,
    LaneE2eeDevice *device,
    const char *device_id,
    uint32_t otk_count);
int32_t lane_send_encrypted_chat(
    LaneSession *session,
    LaneE2eeDevice *device,
    const char *to_user,
    const char *message_id,
    const uint8_t *plaintext,
    size_t plaintext_len,
    uint64_t *out_seq);

#ifdef __cplusplus
}
#endif

#endif /* LANE_MESSENGER_FFI_H */
