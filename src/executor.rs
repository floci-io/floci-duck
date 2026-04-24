use crate::models::ExecuteRequest;
use duckdb::Connection;
use std::env;
use tracing::info;

pub fn execute_query(req: &ExecuteRequest) -> anyhow::Result<()> {
    // Fresh in-memory connection per request
    let conn = Connection::open_in_memory()?;

    // Set extension directory if provided
    if let Ok(ext_dir) = env::var("FLOCI_DUCK_EXT_DIR") {
        info!("Setting extension directory to: {}", ext_dir);
        conn.execute_batch(&format!("SET extension_directory = '{}';", ext_dir))?;
    }

    // Load httpfs for S3 support
    info!("Loading httpfs extension...");
    if let Err(e) = conn.execute_batch("LOAD httpfs;") {
        info!("LOAD httpfs failed, attempting INSTALL/LOAD: {:?}", e);
        conn.execute_batch("INSTALL httpfs; LOAD httpfs;")?;
    }

    // S3 configuration
    // Clean endpoint to ensure it doesn't have trailing slash or redundant protocol if possible
    let endpoint = req.s3_endpoint.trim_end_matches('/');
    
    let region = req.s3_region.clone()
        .or_else(|| env::var("FLOCI_DUCK_S3_REGION").ok())
        .unwrap_or_else(|| "us-east-1".to_string());
    
    info!("Configuring S3: endpoint={}, region={}", endpoint, region);
    
    let access_key = req.s3_access_key.clone()
        .or_else(|| env::var("FLOCI_DUCK_S3_ACCESS_KEY").ok())
        .unwrap_or_else(|| "flociadmin".to_string());
    
    let secret_key = req.s3_secret_key.clone()
        .or_else(|| env::var("FLOCI_DUCK_S3_SECRET_KEY").ok())
        .unwrap_or_else(|| "flociadmin".to_string());

    let use_ssl = req.s3_use_ssl
        .or_else(|| env::var("FLOCI_DUCK_S3_USE_SSL").ok().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| req.s3_endpoint.starts_with("https://"));

    let url_style = req.s3_url_style.clone()
        .or_else(|| env::var("FLOCI_DUCK_S3_URL_STYLE").ok())
        .unwrap_or_else(|| "path".to_string());

    conn.execute_batch(&format!(
        "SET s3_endpoint = '{}';
         SET s3_region = '{}';
         SET s3_access_key_id = '{}';
         SET s3_secret_access_key = '{}';
         SET s3_use_ssl = {};
         SET s3_url_style = '{}';",
        endpoint.replace("http://", "").replace("https://", ""),
        region,
        access_key,
        secret_key,
        use_ssl,
        url_style
    ))?;

    // Run setup DDL (e.g. CREATE OR REPLACE VIEW for Glue tables) before the main query
    if let Some(setup) = &req.setup_sql {
        if !setup.trim().is_empty() {
            info!("Executing setup SQL");
            conn.execute_batch(setup)?;
        }
    }

    // Athena mode: wrap only the user SELECT in COPY; setup DDL ran above
    let final_sql = if let Some(output_path) = &req.output_s3_path {
        info!("Athena mode detected. Output path: {}", output_path);
        format!("COPY ({}) TO '{}' (FORMAT CSV, HEADER);", req.sql, output_path)
    } else {
        info!("Firehose mode detected. Running raw SQL.");
        req.sql.clone()
    };

    info!("Executing final SQL: {}", final_sql);
    conn.execute_batch(&final_sql)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_athena_mode_sql_wrap() {
        let req = ExecuteRequest {
            sql: "SELECT * FROM table".to_string(),
            s3_endpoint: "http://floci:9000".to_string(),
            s3_region: Some("us-east-1".to_string()),
            s3_access_key: None,
            s3_secret_key: None,
            s3_use_ssl: None,
            s3_url_style: None,
            output_s3_path: Some("s3://bucket/output.csv".to_string()),
            variables: None,
        };
        
        let output_path = req.output_s3_path.as_ref().unwrap();
        let final_sql = format!("COPY ({}) TO '{}' (FORMAT CSV, HEADER);", req.sql, output_path);
        assert_eq!(final_sql, "COPY (SELECT * FROM table) TO 's3://bucket/output.csv' (FORMAT CSV, HEADER);");
    }
}
