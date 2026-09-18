use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct ExecuteRequest {
    pub sql: String,
    pub s3_endpoint: String,
    pub s3_region: Option<String>,
    pub s3_access_key: Option<String>,
    pub s3_secret_key: Option<String>,
    pub s3_use_ssl: Option<bool>,
    pub s3_url_style: Option<String>,
    pub output_s3_path: Option<String>,
    pub setup_sql: Option<String>,
    #[allow(dead_code)]
    pub variables: Option<HashMap<String, String>>,
}

#[derive(Serialize)]
pub struct ExecuteResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Request for /query — runs SQL and returns rows as JSON.
/// S3 fields mirror ExecuteRequest so callers can share the same config.
#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub sql: String,
    pub s3_endpoint: String,
    pub s3_region: Option<String>,
    pub s3_access_key: Option<String>,
    pub s3_secret_key: Option<String>,
    pub s3_use_ssl: Option<bool>,
    pub s3_url_style: Option<String>,
    pub setup_sql: Option<String>,
    /// When true, row values are encoded losslessly: exact decimals, ISO-8601 temporals,
    /// nested lists/structs as JSON, base64 blobs. Off by default so existing callers see
    /// unchanged output.
    #[serde(default)]
    pub typed_values: bool,
    /// Optional statement run after `sql` in the same connection; its result is returned
    /// as `followup`. Lets a caller read state a DML statement just changed.
    pub followup_sql: Option<String>,
}

/// Columns and rows of one statement's result.
#[derive(Serialize)]
pub struct QueryResult {
    pub columns: Vec<Column>,
    pub rows: Vec<serde_json::Map<String, serde_json::Value>>,
}

/// A result column: its name and DuckDB SQL type, as `DESCRIBE` would print it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
}

#[derive(Serialize)]
pub struct QueryResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<Column>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<Vec<serde_json::Map<String, serde_json::Value>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub followup: Option<QueryResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}
