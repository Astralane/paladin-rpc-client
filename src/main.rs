mod palidator_tracker;

use axum::{
    extract::Json,
    http::StatusCode,
    response::IntoResponse,
    routing::post,
    Router,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::info;
use base64::engine::general_purpose::STANDARD as base64_engine;
use base64::Engine;

#[derive(Debug, Deserialize)]
struct SendPaladinParams {
    #[serde(rename = "revertProtection")]
    revert_protection: bool,
    #[serde(rename = "enableFallback")]
    enable_fallback: bool,
}

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    jsonrpc: String,
    method: String,
    params: (String, SendPaladinParams),
    id: u64,
}

#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    result: String,
    id: u64,
}

async fn handle_json_rpc(Json(req): Json<JsonRpcRequest>) -> impl IntoResponse {
    if req.method != "sendPaladin" {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Unsupported method", "id": req.id })),
        );
    }

    let (base64_tx, options) = req.params;

    let decoded_tx = match base64_engine.decode(&base64_tx) {
        Ok(tx) => tx,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid base64", "id": req.id })),
            );
        }
    };

    info!("Received sendPaladin tx ({} bytes)", decoded_tx.len());
    info!(
        "Options: revert_protection={}, enable_fallback={}",
        options.revert_protection, options.enable_fallback
    );

    let response = JsonRpcResponse {
        jsonrpc: "2.0".to_string(),
        result: "ok".to_string(),
        id: req.id,
    };

    let response_json = serde_json::to_value(response).unwrap();
    (StatusCode::OK, Json(response_json))
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let app = Router::new().route("/", post(handle_json_rpc));
    let addr: std::net::SocketAddr = "0.0.0.0:8080".parse().unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();

    println!("🟢 Listening on http://{}", addr);
    axum::serve(listener, app).await.unwrap();
}
