use crate::models::ExecuteRequest;
use duckdb::Connection;
use std::env;
use tracing::info;

fn escape_sql(s: &str) -> String {
    s.replace('\'', "''")
}

fn apply_variables(sql: &str, variables: &std::collections::HashMap<String, String>) -> String {
    let mut result = sql.to_string();
    for (key, value) in variables {
        result = result.replace(&format!("{{{{{}}}}}", key), value);
    }
    result
}

pub fn execute_query(req: ExecuteRequest) -> anyhow::Result<()> {
    let conn = Connection::open_in_memory()?;

    if let Ok(ext_dir) = env::var("FLOCI_DUCK_EXT_DIR") {
        info!("Setting extension directory to: {}", ext_dir);
        conn.execute_batch(&format!("SET extension_directory = '{}';", escape_sql(&ext_dir)))?;
    }

    info!("Loading httpfs extension...");
    if let Err(e) = conn.execute_batch("LOAD httpfs;") {
        info!("LOAD httpfs failed, attempting INSTALL/LOAD: {:?}", e);
        conn.execute_batch("INSTALL httpfs; LOAD httpfs;")?;
    }

    let endpoint = req.s3_endpoint.trim_end_matches('/');

    let region = req.s3_region
        .or_else(|| env::var("FLOCI_DUCK_S3_REGION").ok())
        .unwrap_or_else(|| "us-east-1".to_string());

    info!("Configuring S3: endpoint={}, region={}", endpoint, region);

    let access_key = req.s3_access_key
        .or_else(|| env::var("FLOCI_DUCK_S3_ACCESS_KEY").ok())
        .unwrap_or_else(|| "flociadmin".to_string());

    let secret_key = req.s3_secret_key
        .or_else(|| env::var("FLOCI_DUCK_S3_SECRET_KEY").ok())
        .unwrap_or_else(|| "flociadmin".to_string());

    let use_ssl = req.s3_use_ssl
        .or_else(|| env::var("FLOCI_DUCK_S3_USE_SSL").ok().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| req.s3_endpoint.starts_with("https://"));

    let url_style = req.s3_url_style
        .or_else(|| env::var("FLOCI_DUCK_S3_URL_STYLE").ok())
        .unwrap_or_else(|| "path".to_string());

    conn.execute_batch(&format!(
        "SET s3_endpoint = '{}';
         SET s3_region = '{}';
         SET s3_access_key_id = '{}';
         SET s3_secret_access_key = '{}';
         SET s3_use_ssl = {};
         SET s3_url_style = '{}';",
        escape_sql(&endpoint.replace("http://", "").replace("https://", "")),
        escape_sql(&region),
        escape_sql(&access_key),
        escape_sql(&secret_key),
        use_ssl,
        escape_sql(&url_style),
    ))?;

    let variables = req.variables.unwrap_or_default();

    if let Some(setup) = &req.setup_sql {
        if !setup.trim().is_empty() {
            info!("Executing setup SQL");
            let setup_sql = apply_variables(setup, &variables);
            conn.execute_batch(&setup_sql)?;
        }
    }

    let sql = apply_variables(&req.sql, &variables);

    let final_sql = if let Some(output_path) = &req.output_s3_path {
        info!("Athena mode detected. Output path: {}", output_path);
        format!("COPY ({}) TO '{}' (FORMAT CSV, HEADER);", sql, output_path)
    } else {
        info!("Firehose mode detected. Running raw SQL.");
        sql
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
            setup_sql: None,
            variables: None,
        };

        let variables = req.variables.clone().unwrap_or_default();
        let sql = apply_variables(&req.sql, &variables);
        let output_path = req.output_s3_path.as_ref().unwrap();
        let final_sql = format!("COPY ({}) TO '{}' (FORMAT CSV, HEADER);", sql, output_path);
        assert_eq!(final_sql, "COPY (SELECT * FROM table) TO 's3://bucket/output.csv' (FORMAT CSV, HEADER);");
    }

    #[test]
    fn test_variable_substitution() {
        let mut vars = std::collections::HashMap::new();
        vars.insert("table".to_string(), "my_table".to_string());
        vars.insert("limit".to_string(), "100".to_string());
        let sql = apply_variables("SELECT * FROM {{table}} LIMIT {{limit}}", &vars);
        assert_eq!(sql, "SELECT * FROM my_table LIMIT 100");
    }

    #[test]
    fn test_escape_sql() {
        assert_eq!(escape_sql("O'Brien"), "O''Brien");
        assert_eq!(escape_sql("normal"), "normal");
    }
}
