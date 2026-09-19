use crate::executor::{execute_query, execute_query_returning};
use crate::models::{ExecuteRequest, ExecuteResponse, QueryRequest, QueryResponse};
use axum::{
    http::{HeaderMap, StatusCode},
    Json,
};
use tracing::{error, info, info_span, Instrument, Span};
use uuid::Uuid;

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

        let span = Span::current();
        let result = tokio::task::spawn_blocking(move || {
            let _guard = span.enter();
            execute_query(req)
        })
        .await;

        match result {
            Ok(Ok(_)) => {
                info!("Query executed successfully");
                (
                    StatusCode::OK,
                    Json(ExecuteResponse {
                        status: "success".to_string(),
                        message: None,
                    }),
                )
            }
            Ok(Err(e)) => {
                error!("Query execution failed: {:?}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ExecuteResponse {
                        status: "error".to_string(),
                        message: Some(e.to_string()),
                    }),
                )
            }
            Err(e) => {
                error!("Query task panicked: {:?}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ExecuteResponse {
                        status: "error".to_string(),
                        message: Some("Internal error".to_string()),
                    }),
                )
            }
        }
    }
    .instrument(span)
    .await
}

pub async fn handle_query(
    headers: HeaderMap,
    Json(req): Json<QueryRequest>,
) -> (StatusCode, Json<QueryResponse>) {
    let correlation_id = headers
        .get("x-correlation-id")
        .and_then(|v| v.to_str().ok())
        .map(String::from)
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    let span = info_span!("query", correlation_id = %correlation_id);

    async move {
        info!("Received query request");

        let span = Span::current();
        let result = tokio::task::spawn_blocking(move || {
            let _guard = span.enter();
            execute_query_returning(req)
        })
        .await;

        match result {
            Ok(Ok(output)) => {
                info!("Query returned {} rows", output.rows.len());
                (
                    StatusCode::OK,
                    Json(QueryResponse {
                        status: "success".to_string(),
                        columns: Some(output.columns),
                        rows: Some(output.rows),
                        followup: output.followup,
                        arrow: output.arrow,
                        message: None,
                    }),
                )
            }
            Ok(Err(e)) => {
                error!("Query execution failed: {:?}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(QueryResponse {
                        status: "error".to_string(),
                        columns: None,
                        rows: None,
                        followup: None,
                        arrow: None,
                        message: Some(e.to_string()),
                    }),
                )
            }
            Err(e) => {
                error!("Query task panicked: {:?}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(QueryResponse {
                        status: "error".to_string(),
                        columns: None,
                        rows: None,
                        followup: None,
                        arrow: None,
                        message: Some("Internal error".to_string()),
                    }),
                )
            }
        }
    }
    .instrument(span)
    .await
}
