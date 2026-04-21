use axum::{http::StatusCode, Json};
use tracing::{error, info};
use crate::models::{ExecuteRequest, ExecuteResponse};
use crate::executor::execute_query;

pub async fn handle_execute(Json(req): Json<ExecuteRequest>) -> (StatusCode, Json<ExecuteResponse>) {
    info!("Received execute request");
    
    match execute_query(&req) {
        Ok(_) => {
            info!("Query executed successfully");
            (StatusCode::OK, Json(ExecuteResponse {
                status: "success".to_string(),
                message: None,
            }))
        },
        Err(e) => {
            error!("Query execution failed: {:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ExecuteResponse {
                status: "error".to_string(),
                message: Some(e.to_string()),
            }))
        }
    }
}
