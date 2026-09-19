use crate::models::{ArrowIpc, ArrowIpcBatch, Column, ExecuteRequest, QueryRequest, QueryResult};
use duckdb::arrow::array::{
    Array as ArrowArray, BinaryArray, BooleanArray, Decimal128Array, FixedSizeBinaryArray,
    FixedSizeListArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, MapArray, StringArray,
    StructArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use duckdb::arrow::datatypes::DataType;
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::arrow::util::display::{ArrayFormatter, FormatOptions};
use duckdb::core::LogicalTypeId;
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
        conn.execute_batch(&format!(
            "SET extension_directory = '{}';",
            escape_sql(&ext_dir)
        ))?;
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

    info!(
        "Configuring S3: endpoint={}, region={}",
        endpoint, s3_region
    );
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

/// Result of `/query`: the column names and DuckDB types, plus the rows, and the
/// result of the optional follow-up statement.
pub struct QueryOutput {
    pub columns: Vec<Column>,
    pub rows: Vec<serde_json::Map<String, serde_json::Value>>,
    pub followup: Option<QueryResult>,
    pub arrow: Option<ArrowIpc>,
}

/// Query executor that returns rows as JSON maps, used by S3 Select and BigQuery.
///
/// Uses `query_arrow()` so schema (column names) and data are available together
/// without the borrow conflict that arises when calling `column_names()` on a
/// `Rows`-borrowed statement.
pub fn execute_query_returning(req: QueryRequest) -> anyhow::Result<QueryOutput> {
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
    let (columns, rows, arrow) = if req.arrow_ipc {
        let (columns, arrow) = run_statement_arrow(&conn, &req.sql)?;
        (columns, Vec::new(), Some(arrow))
    } else {
        let (columns, rows) = run_statement(&conn, &req.sql, req.typed_values)?;
        (columns, rows, None)
    };
    info!("Query returned {} rows", rows.len());

    let followup = match &req.followup_sql {
        Some(sql) if !sql.trim().is_empty() => {
            info!("Executing follow-up SQL: {}", sql);
            let (columns, rows) = run_statement(&conn, sql, req.typed_values)?;
            Some(QueryResult { columns, rows })
        }
        _ => None,
    };

    Ok(QueryOutput {
        columns,
        rows,
        followup,
        arrow,
    })
}

/// Runs one statement and returns its result as Arrow IPC messages instead of JSON rows.
fn run_statement_arrow(conn: &Connection, sql: &str) -> anyhow::Result<(Vec<Column>, ArrowIpc)> {
    use arrow_ipc::writer::{
        write_message, CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions,
    };

    let mut stmt = conn.prepare(sql)?;
    let batches: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
    let schema = stmt.schema();
    let columns = with_described_types(conn, sql, result_columns(&stmt));

    let generator = IpcDataGenerator::default();
    let options = IpcWriteOptions::default();
    let mut tracker = DictionaryTracker::new(false);
    let mut compression = CompressionContext::default();

    let mut schema_bytes = Vec::new();
    let encoded_schema =
        generator.schema_to_bytes_with_dictionary_tracker(&schema, &mut tracker, &options);
    write_message(&mut schema_bytes, encoded_schema, &options)?;

    let mut encoded_batches = Vec::with_capacity(batches.len());
    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let (dictionaries, encoded) =
            generator.encode(batch, &mut tracker, &options, &mut compression)?;
        let mut data = Vec::new();
        for dictionary in dictionaries {
            write_message(&mut data, dictionary, &options)?;
        }
        write_message(&mut data, encoded, &options)?;
        encoded_batches.push(ArrowIpcBatch {
            data: base64_string(&data),
            row_count: batch.num_rows(),
        });
    }
    Ok((
        columns,
        ArrowIpc {
            schema: base64_string(&schema_bytes),
            batches: encoded_batches,
        },
    ))
}

type Rows = Vec<serde_json::Map<String, serde_json::Value>>;

/// Runs one statement and returns its result columns and rows.
fn run_statement(
    conn: &Connection,
    sql: &str,
    typed_values: bool,
) -> anyhow::Result<(Vec<Column>, Rows)> {
    let mut stmt = conn.prepare(sql)?;
    let batches: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
    let columns = with_described_types(conn, sql, result_columns(&stmt));

    let mut rows = Vec::new();
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
                let value = if typed_values {
                    arrow_value_to_typed_json(col.as_ref(), row_idx)
                } else {
                    arrow_value_to_json(col.as_ref(), row_idx)
                };
                map.insert(name.clone(), value);
            }
            rows.push(map);
        }
    }
    Ok((columns, rows))
}

/**
 * Replaces the Arrow-derived type names with the exact ones `DESCRIBE` reports (Arrow folds
 * aliases such as `JSON` into `VARCHAR`). `DESCRIBE` only binds the query, so this costs no
 * execution; statements it cannot describe (DML, DDL) keep the Arrow-derived names.
 */
fn with_described_types(conn: &Connection, sql: &str, columns: Vec<Column>) -> Vec<Column> {
    let described: Option<Vec<(String, String)>> = (|| {
        let mut stmt = conn.prepare(&format!("DESCRIBE {}", sql)).ok()?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .ok()?;
        rows.collect::<Result<Vec<_>, _>>().ok()
    })();
    match described {
        Some(described) if described.len() == columns.len() => columns
            .into_iter()
            .zip(described)
            .map(|(column, (_, type_name))| Column {
                name: column.name,
                type_name,
            })
            .collect(),
        _ => columns,
    }
}

fn result_columns(stmt: &duckdb::Statement<'_>) -> Vec<Column> {
    let schema = stmt.schema();
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| Column {
            name: field.name().clone(),
            type_name: column_type_sql(field.data_type(), stmt.column_logical_type(idx).id()),
        })
        .collect()
}

/// Renders a result column's type as DuckDB SQL, the way `DESCRIBE` prints it
/// (e.g. `DECIMAL(38,9)`, `TIMESTAMP WITH TIME ZONE`, `STRUCT("a" INTEGER[])`).
///
/// The structure comes from the Arrow schema; the column's DuckDB logical type id only
/// disambiguates types Arrow folds into another (HUGEINT exports as DECIMAL(38,0), UUID
/// and ENUM as strings).
pub fn column_type_sql(arrow: &DataType, logical: LogicalTypeId) -> String {
    match logical {
        LogicalTypeId::Hugeint => "HUGEINT".into(),
        LogicalTypeId::UHugeint => "UHUGEINT".into(),
        LogicalTypeId::Uuid => "UUID".into(),
        LogicalTypeId::Enum => "ENUM".into(),
        LogicalTypeId::TimeTZ => "TIME WITH TIME ZONE".into(),
        LogicalTypeId::Bit => "BIT".into(),
        _ => arrow_type_sql(arrow),
    }
}

fn arrow_type_sql(t: &DataType) -> String {
    match t {
        DataType::Null => "\"NULL\"".into(),
        DataType::Boolean => "BOOLEAN".into(),
        DataType::Int8 => "TINYINT".into(),
        DataType::Int16 => "SMALLINT".into(),
        DataType::Int32 => "INTEGER".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::UInt8 => "UTINYINT".into(),
        DataType::UInt16 => "USMALLINT".into(),
        DataType::UInt32 => "UINTEGER".into(),
        DataType::UInt64 => "UBIGINT".into(),
        DataType::Float16 | DataType::Float32 => "FLOAT".into(),
        DataType::Float64 => "DOUBLE".into(),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => format!("DECIMAL({},{})", p, s),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "VARCHAR".into(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "BLOB".into(),
        DataType::Date32 | DataType::Date64 => "DATE".into(),
        DataType::Time32(_) | DataType::Time64(_) => "TIME".into(),
        DataType::Timestamp(_, Some(_)) => "TIMESTAMP WITH TIME ZONE".into(),
        DataType::Timestamp(unit, None) => match unit {
            duckdb::arrow::datatypes::TimeUnit::Second => "TIMESTAMP_S".into(),
            duckdb::arrow::datatypes::TimeUnit::Millisecond => "TIMESTAMP_MS".into(),
            duckdb::arrow::datatypes::TimeUnit::Nanosecond => "TIMESTAMP_NS".into(),
            duckdb::arrow::datatypes::TimeUnit::Microsecond => "TIMESTAMP".into(),
        },
        DataType::Interval(_) | DataType::Duration(_) => "INTERVAL".into(),
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            format!("{}[]", arrow_type_sql(f.data_type()))
        }
        DataType::Struct(fields) => {
            let parts: Vec<String> = fields
                .iter()
                .map(|f| {
                    format!(
                        "\"{}\" {}",
                        f.name().replace('"', "\"\""),
                        arrow_type_sql(f.data_type())
                    )
                })
                .collect();
            format!("STRUCT({})", parts.join(", "))
        }
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(kv) if kv.len() == 2 => format!(
                "MAP({}, {})",
                arrow_type_sql(kv[0].data_type()),
                arrow_type_sql(kv[1].data_type())
            ),
            _ => "MAP".into(),
        },
        DataType::Dictionary(_, value) => arrow_type_sql(value),
        other => format!("{:?}", other).to_uppercase(),
    }
}

/// Lossless JSON encoding of one Arrow value, used when a `/query` request sets
/// `typed_values`. Scalars that JSON can hold exactly stay numbers/booleans; decimals,
/// temporals and intervals are strings in Arrow's canonical (ISO-8601) text form; blobs
/// are base64; lists and structs nest.
pub fn arrow_value_to_typed_json(array: &dyn ArrowArray, idx: usize) -> serde_json::Value {
    if array.is_null(idx) {
        return serde_json::Value::Null;
    }
    match array.data_type() {
        DataType::Float32 | DataType::Float64 => {
            let v = match array.data_type() {
                DataType::Float32 => array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .value(idx) as f64,
                _ => array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(idx),
            };
            if v.is_nan() {
                serde_json::Value::String("NaN".into())
            } else if v.is_infinite() {
                serde_json::Value::String(if v > 0.0 { "Infinity" } else { "-Infinity" }.into())
            } else {
                arrow_value_to_json(array, idx)
            }
        }
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Utf8
        | DataType::LargeUtf8 => arrow_value_to_json(array, idx),
        DataType::Binary => base64_value(
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(idx),
        ),
        DataType::LargeBinary => base64_value(
            array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .value(idx),
        ),
        DataType::FixedSizeBinary(_) => base64_value(
            array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(idx),
        ),
        DataType::List(_) => nested_list(
            array
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(idx)
                .as_ref(),
        ),
        DataType::LargeList(_) => nested_list(
            array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .unwrap()
                .value(idx)
                .as_ref(),
        ),
        DataType::FixedSizeList(_, _) => nested_list(
            array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap()
                .value(idx)
                .as_ref(),
        ),
        DataType::Struct(_) => {
            let s = array.as_any().downcast_ref::<StructArray>().unwrap();
            let mut map = serde_json::Map::new();
            for (i, name) in s.column_names().iter().enumerate() {
                map.insert(
                    name.to_string(),
                    arrow_value_to_typed_json(s.column(i).as_ref(), idx),
                );
            }
            serde_json::Value::Object(map)
        }
        DataType::Map(_, _) => {
            let entries = array
                .as_any()
                .downcast_ref::<MapArray>()
                .unwrap()
                .value(idx);
            let pairs = (0..entries.len())
                .map(|i| {
                    let mut pair = serde_json::Map::new();
                    pair.insert(
                        "key".into(),
                        arrow_value_to_typed_json(entries.column(0).as_ref(), i),
                    );
                    pair.insert(
                        "value".into(),
                        arrow_value_to_typed_json(entries.column(1).as_ref(), i),
                    );
                    serde_json::Value::Object(pair)
                })
                .collect();
            serde_json::Value::Array(pairs)
        }
        DataType::Timestamp(unit, tz) => {
            use duckdb::arrow::array::{
                TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
                TimestampSecondArray,
            };
            use duckdb::arrow::datatypes::TimeUnit;
            use duckdb::arrow::temporal_conversions::{
                timestamp_ms_to_datetime, timestamp_ns_to_datetime, timestamp_s_to_datetime,
                timestamp_us_to_datetime,
            };
            let any = array.as_any();
            let datetime = match unit {
                TimeUnit::Second => timestamp_s_to_datetime(
                    any.downcast_ref::<TimestampSecondArray>()
                        .unwrap()
                        .value(idx),
                ),
                TimeUnit::Millisecond => timestamp_ms_to_datetime(
                    any.downcast_ref::<TimestampMillisecondArray>()
                        .unwrap()
                        .value(idx),
                ),
                TimeUnit::Microsecond => timestamp_us_to_datetime(
                    any.downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap()
                        .value(idx),
                ),
                TimeUnit::Nanosecond => timestamp_ns_to_datetime(
                    any.downcast_ref::<TimestampNanosecondArray>()
                        .unwrap()
                        .value(idx),
                ),
            };
            match datetime {
                // DuckDB stores TIMESTAMPTZ as a UTC instant; the zone only affects display.
                Some(dt) => serde_json::Value::String(format!(
                    "{}{}",
                    dt.format("%Y-%m-%dT%H:%M:%S%.f"),
                    if tz.is_some() { "Z" } else { "" }
                )),
                None => serde_json::Value::Null,
            }
        }
        // Decimals, dates, times, intervals, dictionaries (ENUM): Arrow's formatter
        // renders these exactly.
        _ => match ArrayFormatter::try_new(array, &FormatOptions::default()) {
            Ok(formatter) => serde_json::Value::String(formatter.value(idx).to_string()),
            Err(_) => serde_json::Value::String(format!("[{:?}]", array.data_type())),
        },
    }
}

fn nested_list(values: &dyn ArrowArray) -> serde_json::Value {
    serde_json::Value::Array(
        (0..values.len())
            .map(|i| arrow_value_to_typed_json(values, i))
            .collect(),
    )
}

fn base64_string(bytes: &[u8]) -> String {
    match base64_value(bytes) {
        serde_json::Value::String(s) => s,
        _ => unreachable!(),
    }
}

fn base64_value(bytes: &[u8]) -> serde_json::Value {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        out.push(ALPHABET[(n >> 18) as usize & 63] as char);
        out.push(ALPHABET[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[n as usize & 63] as char
        } else {
            '='
        });
    }
    serde_json::Value::String(out)
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
        assert_eq!(greeting, serde_json::Value::String("hello".to_string()));
    }

    fn run(sql: &str, typed_values: bool) -> QueryOutput {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("SET TimeZone = 'UTC';").unwrap();
        let mut stmt = conn.prepare(sql).unwrap();
        let batches: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
        let columns = result_columns(&stmt);
        let mut rows = Vec::new();
        for batch in &batches {
            for row in 0..batch.num_rows() {
                let mut map = serde_json::Map::new();
                for (col, field) in batch.schema().fields().iter().enumerate() {
                    let array = batch.column(col);
                    let value = if typed_values {
                        arrow_value_to_typed_json(array.as_ref(), row)
                    } else {
                        arrow_value_to_json(array.as_ref(), row)
                    };
                    map.insert(field.name().clone(), value);
                }
                rows.push(map);
            }
        }
        QueryOutput {
            columns,
            rows,
            followup: None,
            arrow: None,
        }
    }

    const TYPED_SQL: &str = "SELECT 1.25::DECIMAL(38,9) AS num, DATE '2024-01-02' AS d, \
        TIMESTAMPTZ '2024-01-02 03:04:05.123456+00' AS ts, TIMESTAMP '2024-01-02 03:04:05' AS dt, \
        [1, 2] AS arr, {'x': 1, 'y': 'z'} AS st, 'hi'::BLOB AS b, count_if(true) AS c, \
        'NaN'::DOUBLE AS nan";

    #[test]
    fn test_columns_report_duckdb_types() {
        let out = run(TYPED_SQL, false);
        let types: Vec<(&str, &str)> = out
            .columns
            .iter()
            .map(|c| (c.name.as_str(), c.type_name.as_str()))
            .collect();
        assert_eq!(
            types,
            vec![
                ("num", "DECIMAL(38,9)"),
                ("d", "DATE"),
                ("ts", "TIMESTAMP WITH TIME ZONE"),
                ("dt", "TIMESTAMP"),
                ("arr", "INTEGER[]"),
                ("st", "STRUCT(\"x\" INTEGER, \"y\" VARCHAR)"),
                ("b", "BLOB"),
                ("c", "HUGEINT"),
                ("nan", "DOUBLE"),
            ]
        );
    }

    #[test]
    fn test_described_types_keep_type_aliases() {
        let conn = Connection::open_in_memory().unwrap();
        let sql = "SELECT '{\"a\": 1}'::JSON AS j, 1::HUGEINT AS h, 'x' AS s";
        let mut stmt = conn.prepare(sql).unwrap();
        let _: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
        let columns = with_described_types(&conn, sql, result_columns(&stmt));
        let types: Vec<&str> = columns.iter().map(|c| c.type_name.as_str()).collect();
        assert_eq!(types, vec!["JSON", "HUGEINT", "VARCHAR"]);

        // DML cannot be described; the Arrow-derived names remain.
        conn.execute_batch("CREATE TABLE t (x INTEGER);").unwrap();
        let mut insert = conn.prepare("INSERT INTO t VALUES (1)").unwrap();
        let _: Vec<RecordBatch> = insert.query_arrow([]).unwrap().collect();
        let columns =
            with_described_types(&conn, "INSERT INTO t VALUES (1)", result_columns(&insert));
        assert_eq!(columns[0].type_name, "BIGINT");
    }

    #[test]
    fn test_columns_present_for_empty_results() {
        let out = run("SELECT 1 AS a, 'x' AS b WHERE false", false);
        assert!(out.rows.is_empty());
        assert_eq!(out.columns.len(), 2);
        assert_eq!(out.columns[0].type_name, "INTEGER");
    }

    #[test]
    fn test_typed_values_are_lossless() {
        let row = &run(TYPED_SQL, true).rows[0];
        assert_eq!(row["num"], serde_json::json!("1.250000000"));
        assert_eq!(row["d"], serde_json::json!("2024-01-02"));
        assert_eq!(row["ts"], serde_json::json!("2024-01-02T03:04:05.123456Z"));
        assert_eq!(row["dt"], serde_json::json!("2024-01-02T03:04:05"));
        assert_eq!(row["arr"], serde_json::json!([1, 2]));
        assert_eq!(row["st"], serde_json::json!({"x": 1, "y": "z"}));
        assert_eq!(row["b"], serde_json::json!("aGk="));
        assert_eq!(row["c"], serde_json::json!("1"));
        assert_eq!(row["nan"], serde_json::json!("NaN"));
    }

    #[test]
    fn test_untyped_values_keep_the_legacy_encoding() {
        // Existing /query callers (floci S3 Select) must see exactly the old output.
        let row = &run(TYPED_SQL, false).rows[0];
        assert_eq!(row["num"], serde_json::json!(1.25));
        assert_eq!(row["d"], serde_json::json!("[Date32]"));
        assert!(row["arr"].as_str().unwrap().starts_with("[List("));
        assert_eq!(row["nan"], serde_json::Value::Null);
    }

    #[test]
    fn test_followup_sees_the_changes_of_the_first_statement() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE t AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) v(id, name);",
        )
        .unwrap();
        let (columns, rows) =
            run_statement(&conn, "UPDATE t SET name = 'x' WHERE id = 2", true).unwrap();
        assert_eq!(columns[0].name, "Count");
        assert_eq!(rows[0]["Count"], serde_json::json!(1));

        let (columns, rows) = run_statement(&conn, "SELECT * FROM t ORDER BY id", true).unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(rows[1]["name"], serde_json::json!("x"));
    }

    #[test]
    fn test_arrow_ipc_messages_round_trip() {
        use duckdb::arrow::datatypes::DataType as DT;
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("SET TimeZone = 'UTC';").unwrap();
        let (columns, arrow) = run_statement_arrow(
            &conn,
            "SELECT * FROM (VALUES (1::BIGINT, 'a', TIMESTAMPTZ '2024-01-02 03:04:05+00'), \
             (2, 'b', NULL)) v(id, name, ts)",
        )
        .unwrap();
        assert_eq!(columns.len(), 3);
        assert_eq!(arrow.batches.iter().map(|b| b.row_count).sum::<usize>(), 2);
        // Every message is an encapsulated IPC message: continuation marker, then metadata length.
        let schema = decode_base64(&arrow.schema);
        assert_eq!(&schema[0..4], &[0xFF, 0xFF, 0xFF, 0xFF]);
        let parsed = arrow_ipc::convert::try_schema_from_ipc_buffer(&schema).unwrap();
        assert_eq!(parsed.field(0).data_type(), &DT::Int64);
        assert_eq!(
            parsed.field(2).data_type(),
            &DT::Timestamp(
                duckdb::arrow::datatypes::TimeUnit::Microsecond,
                Some("UTC".into())
            )
        );
        let batch = decode_base64(&arrow.batches[0].data);
        assert_eq!(&batch[0..4], &[0xFF, 0xFF, 0xFF, 0xFF]);
    }

    fn decode_base64(text: &str) -> Vec<u8> {
        const ALPHABET: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = Vec::new();
        let mut buffer = 0u32;
        let mut bits = 0;
        for byte in text.bytes().filter(|b| *b != b'=') {
            let value = ALPHABET.iter().position(|c| *c == byte).unwrap() as u32;
            buffer = (buffer << 6) | value;
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                out.push((buffer >> bits) as u8);
                buffer &= (1 << bits) - 1;
            }
        }
        out
    }

    #[test]
    fn test_base64() {
        assert_eq!(base64_value(b""), serde_json::json!(""));
        assert_eq!(base64_value(b"f"), serde_json::json!("Zg=="));
        assert_eq!(base64_value(b"fo"), serde_json::json!("Zm8="));
        assert_eq!(base64_value(b"foo"), serde_json::json!("Zm9v"));
    }
}
