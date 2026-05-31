//! ONDC-style signing demo: `testecom` NP signs JSON → dummy receiver verifies `Authorization`.
//!
//! ```bash
//! cargo run --example ondc_demo
//! ```

#[path = "gateway/ondc.rs"]
mod ondc;

use actix_web::{post, web, App, HttpRequest, HttpResponse, HttpServer};
use ondc::{
    build_authorization_header, sample_search_json, testecom_registry, unix_now,
    verify_authorization, OndcKeyId, TESTECOM_SIGNING_PRIVATE_KEY_B64, TESTECOM_SUBSCRIBER_ID,
    TESTECOM_SIGNING_PUBLIC_KEY_B64, TESTECOM_UNIQUE_KEY_ID,
};
use tracing::info;

struct ServerState {
    registry: std::collections::HashMap<(String, String), String>,
}

#[post("/bap/search")]
async fn bap_search(
    req: HttpRequest,
    body: web::Bytes,
    state: web::Data<ServerState>,
) -> HttpResponse {
    let auth = match req.headers().get("Authorization").and_then(|v| v.to_str().ok()) {
        Some(v) => v,
        None => {
            return HttpResponse::Unauthorized().json(serde_json::json!({
                "error": "missing Authorization header"
            }));
        }
    };

    match verify_authorization(&body, auth, &state.registry, unix_now()) {
        Ok(parsed) => HttpResponse::Ok().json(serde_json::json!({
            "status": "verified",
            "subscriber": parsed.key_id.subscriber_id,
            "unique_key_id": parsed.key_id.unique_key_id,
            "created": parsed.created,
            "expires": parsed.expires,
            "body_len": body.len(),
        })),
        Err(e) => HttpResponse::Unauthorized().json(serde_json::json!({
            "error": e.to_string(),
        })),
    }
}

async fn send_signed_request() {
    let body = sample_search_json();
    let created = unix_now();
    let expires = created + 3600;

    let key_id = OndcKeyId::new(TESTECOM_SUBSCRIBER_ID, TESTECOM_UNIQUE_KEY_ID);
    let authorization = match build_authorization_header(
        body.as_bytes(),
        &key_id,
        TESTECOM_SIGNING_PRIVATE_KEY_B64,
        created,
        expires,
    ) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = %e, "failed to build Authorization");
            return;
        }
    };

    info!(%authorization, "built Authorization header (testecom)");

    let client = reqwest::Client::new();
    match client
        .post("http://127.0.0.1:8090/bap/search")
        .header("Authorization", authorization)
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .send()
        .await
    {
        Ok(resp) => {
            let status = resp.status();
            match resp.text().await {
                Ok(text) => info!(%status, body = %text, "dummy server response"),
                Err(e) => tracing::error!(error = %e, "read body failed"),
            }
        }
        Err(e) => tracing::error!(error = %e, "POST failed"),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info,ondc_demo=debug")
        .try_init()
        .ok();

    info!(
        "testecom registry public key ({}|{}): {}",
        TESTECOM_SUBSCRIBER_ID,
        TESTECOM_UNIQUE_KEY_ID,
        TESTECOM_SIGNING_PUBLIC_KEY_B64
    );

    tokio::spawn(async {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        send_signed_request().await;
    });

    let state = web::Data::new(ServerState {
        registry: testecom_registry(),
    });

    info!("dummy NP2 (receiver) on http://127.0.0.1:8090/bap/search");

    HttpServer::new(move || App::new().app_data(state.clone()).service(bap_search))
        .bind(("127.0.0.1", 8090))?
        .run()
        .await
}
