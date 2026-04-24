mod models;
mod executor;
mod handlers;

use axum::{
    extract::DefaultBodyLimit,
    routing::{get, post},
    Router,
};
use std::env;
use std::net::SocketAddr;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    let filter = EnvFilter::try_from_env("FLOCI_DUCK_LOG")
        .unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .init();

    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .route("/execute", post(handlers::handle_execute))
        .layer(DefaultBodyLimit::max(1024 * 1024));

    let port = env::var("FLOCI_DUCK_PORT").unwrap_or_else(|_| "3000".to_string());
    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse().expect("Invalid address");
    
    info!("Starting floci-duck sidecar on {}", addr);
    
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
