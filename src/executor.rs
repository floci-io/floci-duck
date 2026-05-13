use crate::models::{ExecuteRequest, QueryRequest};
use duckdb::arrow::array::{
    Array as ArrowArray, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, LargeStringArray, StringArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
};
use duckdb::arrow::datatypes::DataType;
use duckdb::arrow::record_batch::RecordBatch;
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

fn open_conn() -> anyhow::Result<Connection> {
    let conn = Connection::open_in_memory()?;
    if let Ok(ext_dir) = env::var("FLOCI_DUCK_EXT_DIR") {
        info!("Setting extension directory to: {}", ext_dir);
        conn.execute_batch(&format!("SET extension_directory = '{}';", escape_sql(&ext_dir)))?;
    }
    Ok(conn)
}

/// Called once at startup to ensure httpfs is installed on disk.
/// Subsequent per-request `LOAD httpfs` calls will find it locally and skip the network download.
pub fn preflight() -> anyhow::Result<()> {
    info!("Preflight: installing httpfs extension...");
    let conn = open_conn()?;
    conn.execute_batch("INSTALL httpfs;")?;
    info!("Preflight: httpfs installed successfully");
    Ok(())
}

/// Loads httpfs and configures S3 credentials on an open connection.
fn setup_s3(
    conn: &Connection,
    s3_endpoint: &str,
    s3_region: &str,
    access_key: &str,
    secret_key: &str,
    use_ssl: bool,
    url_style: &str,
) -> anyhow::Result<()> {
    conn.execute_batch("LOAD httpfs;")?;

    let endpoint = s3_endpoint
        .trim_end_matches('/')
        .trim_start_matches("https://")
        .trim_start_matches("http://");

    info!("Configuring S3: endpoint={}, region={}", endpoint, s3_region);
    conn.execute_batch(&format!(
        "SET s3_endpoint = '{}';
         SET s3_region = '{}';
         SET s3_access_key_id = '{}';
         SET s3_secret_access_key = '{}';
         SET s3_use_ssl = {};
         SET s3_url_style = '{}';",
        escape_sql(endpoint),
        escape_sql(s3_region),
        escape_sql(access_key),
        escape_sql(secret_key),
        use_ssl,
        escape_sql(url_style),
    ))?;

    Ok(())
}

fn resolve_s3_params(
    s3_endpoint: &str,
    s3_region: Option<&str>,
    s3_access_key: Option<&str>,
    s3_secret_key: Option<&str>,
    s3_use_ssl: Option<bool>,
    s3_url_style: Option<&str>,
) -> (String, String, String, bool, String) {
    let region = s3_region
        .map(String::from)
        .or_else(|| env::var("FLOCI_DUCK_S3_REGION").ok())
        .unwrap_or_else(|| "us-east-1".to_string());

    let access_key = s3_access_key
        .map(String::from)
        .or_else(|| env::var("FLOCI_DUCK_S3_ACCESS_KEY").ok())
        .unwrap_or_else(|| "flociadmin".to_string());

    let secret_key = s3_secret_key
        .map(String::from)
        .or_else(|| env::var("FLOCI_DUCK_S3_SECRET_KEY").ok())
        .unwrap_or_else(|| "flociadmin".to_string());

    let use_ssl = s3_use_ssl
        .or_else(|| {
            env::var("FLOCI_DUCK_S3_USE_SSL")
                .ok()
                .and_then(|s| s.parse().ok())
        })
        .unwrap_or_else(|| s3_endpoint.starts_with("https://"));

    let url_style = s3_url_style
        .map(String::from)
        .or_else(|| env::var("FLOCI_DUCK_S3_URL_STYLE").ok())
        .unwrap_or_else(|| "path".to_string());

    (region, access_key, secret_key, use_ssl, url_style)
}

/// Fire-and-forget executor used by Athena/Firehose.
pub fn execute_query(req: ExecuteRequest) -> anyhow::Result<()> {
    let conn = open_conn()?;

    let (region, access_key, secret_key, use_ssl, url_style) = resolve_s3_params(
        &req.s3_endpoint,
        req.s3_region.as_deref(),
        req.s3_access_key.as_deref(),
        req.s3_secret_key.as_deref(),
        req.s3_use_ssl,
        req.s3_url_style.as_deref(),
    );

    setup_s3(
        &conn,
        &req.s3_endpoint,
        &region,
        &access_key,
        &secret_key,
        use_ssl,
        &url_style,
    )?;

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

/// Query executor that returns rows as JSON maps — used by S3 Select.
///
/// Uses `query_arrow()` so schema (column names) and data are available together
/// without the borrow conflict that arises when calling `column_names()` on a
/// `Rows`-borrowed statement.
pub fn execute_query_returning(
    req: QueryRequest,
) -> anyhow::Result<Vec<serde_json::Map<String, serde_json::Value>>> {
    let conn = open_conn()?;

    let (region, access_key, secret_key, use_ssl, url_style) = resolve_s3_params(
        &req.s3_endpoint,
        req.s3_region.as_deref(),
        req.s3_access_key.as_deref(),
        req.s3_secret_key.as_deref(),
        req.s3_use_ssl,
        req.s3_url_style.as_deref(),
    );

    setup_s3(
        &conn,
        &req.s3_endpoint,
        &region,
        &access_key,
        &secret_key,
        use_ssl,
        &url_style,
    )?;

    if let Some(setup) = &req.setup_sql {
        if !setup.trim().is_empty() {
            info!("Executing setup SQL");
            conn.execute_batch(setup)?;
        }
    }

    info!("Executing query SQL: {}", req.sql);
    let mut stmt = conn.prepare(&req.sql)?;
    let batches: Vec<RecordBatch> = stmt.query_arrow([])?.collect();

    let mut result = Vec::new();
    for batch in &batches {
        let field_names: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        for row_idx in 0..batch.num_rows() {
            let mut map = serde_json::Map::new();
            for (col_idx, name) in field_names.iter().enumerate() {
                let col = batch.column(col_idx);
                map.insert(name.clone(), arrow_value_to_json(col.as_ref(), row_idx));
            }
            result.push(map);
        }
    }

    info!("Query returned {} rows", result.len());
    Ok(result)
}

fn arrow_value_to_json(array: &dyn ArrowArray, idx: usize) -> serde_json::Value {
    if array.is_null(idx) {
        return serde_json::Value::Null;
    }
    match array.data_type() {
        DataType::Boolean => {
            let v = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(idx);
            serde_json::Value::Bool(v)
        }
        DataType::Int8 => serde_json::Value::Number(
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(idx)
                .into(),
        ),
        DataType::Int16 => serde_json::Value::Number(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(idx)
                .into(),
        ),
        DataType::Int32 => serde_json::Value::Number(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(idx)
                .into(),
        ),
        DataType::Int64 => serde_json::Value::Number(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(idx)
                .into(),
        ),
        DataType::UInt8 => serde_json::Value::Number(
            array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .value(idx)
                .into(),
        ),
        DataType::UInt16 => serde_json::Value::Number(
            array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap()
                .value(idx)
                .into(),
        ),
        DataType::UInt32 => serde_json::Value::Number(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(idx)
                .into(),
        ),
        DataType::UInt64 => serde_json::Value::Number(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(idx)
                .into(),
        ),
        DataType::Float32 => {
            let v = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(idx);
            serde_json::Number::from_f64(v as f64)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        DataType::Float64 => {
            let v = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(idx);
            serde_json::Number::from_f64(v)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        DataType::Decimal128(_, scale) => {
            let raw = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(idx);
            let divisor = 10_f64.powi(*scale as i32);
            serde_json::Number::from_f64(raw as f64 / divisor)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        DataType::Utf8 => serde_json::Value::String(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(idx)
                .to_string(),
        ),
        DataType::LargeUtf8 => serde_json::Value::String(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(idx)
                .to_string(),
        ),
        // Dates, timestamps, intervals, structs, lists — stringify for now
        other => serde_json::Value::String(format!("[{:?}]", other)),
    }
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
        assert_eq!(
            final_sql,
            "COPY (SELECT * FROM table) TO 's3://bucket/output.csv' (FORMAT CSV, HEADER);"
        );
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

    #[test]
    fn test_query_returning_in_memory() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = conn
            .prepare("SELECT 42 AS answer, 'hello' AS greeting")
            .unwrap();
        let batches: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        let schema = batch.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["answer", "greeting"]);
        assert_eq!(batch.num_rows(), 1);

        let answer = arrow_value_to_json(batch.column(0).as_ref(), 0);
        let greeting = arrow_value_to_json(batch.column(1).as_ref(), 0);
        assert_eq!(answer, serde_json::Value::Number(42.into()));
        assert_eq!(
            greeting,
            serde_json::Value::String("hello".to_string())
        );
    }
}
