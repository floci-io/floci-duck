use axum::{http::{HeaderMap, StatusCode}, Json};
use tracing::{error, info, info_span, Instrument};
use uuid::Uuid;
use crate::models::{ExecuteRequest, ExecuteResponse};
use crate::executor::execute_query;

pub async fn handle_execute(
    headers: HeaderMap,
    Json(req): Json<ExecuteRequest>,
) -> (StatusCode, Json<ExecuteResponse>) {
    let correlation_id = headers
        .get("x-correlation-id")
        .and_then(|v| v.to_str().ok())
        .map(String::from)
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    let span = info_span!("execute", correlation_id = %correlation_id);

    async move {
        info!("Received execute request");

        let result = tokio::task::spawn_blocking(move || execute_query(req)).await;

        match result {
            Ok(Ok(_)) => {
                info!("Query executed successfully");
                (StatusCode::OK, Json(ExecuteResponse {
                    status: "success".to_string(),
                    message: None,
                }))
            }
            Ok(Err(e)) => {
                error!("Query execution failed: {:?}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, Json(ExecuteResponse {
                    status: "error".to_string(),
                    message: Some(e.to_string()),
                }))
            }
            Err(e) => {
                error!("Query task panicked: {:?}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, Json(ExecuteResponse {
                    status: "error".to_string(),
                    message: Some("Internal error".to_string()),
                }))
            }
        }
    }
    .instrument(span)
    .await
}
