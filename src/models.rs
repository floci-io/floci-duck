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
    #[allow(dead_code)]
    pub variables: Option<HashMap<String, String>>,
}

#[derive(Serialize)]
pub struct ExecuteResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}
