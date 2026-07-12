use std::fmt::Write;

use crate::azure_auth::PyAzureCredential;
use crate::parameter_conversion::{
    FastParameter, TypedNull, convert_parameters_to_fast, params_as_sql_refs,
    python_to_fast_parameter,
};
use crate::pool_config::PyPoolConfig;
use crate::pool_manager::{ConnectionPool, ensure_pool_initialized_with_auth};
use crate::types::{create_connection_error, create_sql_error};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3_async_runtimes::tokio::future_into_py;
use smallvec::SmallVec;
use std::sync::Arc;
use tiberius::Config;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio_util::compat::TokioAsyncWriteCompatExt;

/// Parses batch items (SQL queries with parameters) from a Python list.
pub fn parse_batch_items<'p>(
    items: &Bound<'p, PyList>,
    py: Python<'p>,
) -> PyResult<Vec<(String, SmallVec<[FastParameter; 16]>)>> {
    let mut batch_items = Vec::with_capacity(items.len());

    for (batch_index, item) in items.iter().enumerate() {
        let tuple = item.cast::<pyo3::types::PyTuple>().map_err(|_| {
            PyValueError::new_err("Each batch item must be a tuple of (sql, parameters)")
        })?;

        if tuple.len() != 2 {
            return Err(PyValueError::new_err(
                "Tuple must contain exactly 2 elements",
            ));
        }

        let sql: String = tuple.get_item(0)?.extract()?;
        let params_py = tuple.get_item(1)?;

        let fast_params = if params_py.is_none() {
            SmallVec::new()
        } else {
            convert_parameters_to_fast(Some(&params_py), py).map_err(|e| {
                PyValueError::new_err(format!(
                    "Batch item {} parameter validation failed: {}",
                    batch_index, e
                ))
            })?
        };

        if fast_params.len() > 2100 {
            return Err(PyValueError::new_err(format!(
                "Batch item {} exceeds SQL Server parameter limit: {} parameters provided, maximum is 2,100",
                batch_index,
                fast_params.len()
            )));
        }

        batch_items.push((sql, fast_params));
    }

    Ok(batch_items)
}

/// Internal helper: Execute batch commands on an existing connection without transaction management.
/// Used by both Connection (with automatic transaction) and Transaction (with manual control).
pub async fn execute_batch_on_connection(
    conn: &mut tiberius::Client<tokio_util::compat::Compat<tokio::net::TcpStream>>,
    batch_commands: Vec<(String, SmallVec<[FastParameter; 16]>)>,
) -> PyResult<Vec<u64>> {
    let mut all_results = Vec::with_capacity(batch_commands.len());

    for (sql, parameters) in batch_commands {
        // Fast path: skip SmallVec construction entirely for parameter-free statements
        // (common for DDL like CREATE TABLE inside a batch).
        let result = if parameters.is_empty() {
            conn.execute(sql, &[])
                .await
                .map_err(|e| create_sql_error(e, "Batch item failed"))?
        } else {
            let tiberius_params = params_as_sql_refs(&parameters);
            conn.execute(sql, &tiberius_params)
                .await
                .map_err(|e| create_sql_error(e, "Batch item failed"))?
        };

        let affected: u64 = result.rows_affected().iter().sum();
        all_results.push(affected);
    }

    Ok(all_results)
}

/// Internal helper: Execute batch queries on an existing connection.
/// Used by both Connection and Transaction classes.
pub async fn query_batch_on_connection(
    conn: &mut tiberius::Client<tokio_util::compat::Compat<tokio::net::TcpStream>>,
    batch_queries: Vec<(String, SmallVec<[FastParameter; 16]>)>,
) -> PyResult<Vec<Vec<tiberius::Row>>> {
    let mut all_results = Vec::with_capacity(batch_queries.len());

    for (query, parameters) in batch_queries {
        // Fast path: skip SmallVec construction entirely for parameter-free queries.
        let stream = if parameters.is_empty() {
            conn.query(&query, &[])
                .await
                .map_err(|e| create_sql_error(e, "Batch query execution failed"))?
        } else {
            let tiberius_params = params_as_sql_refs(&parameters);
            conn.query(&query, &tiberius_params)
                .await
                .map_err(|e| create_sql_error(e, "Batch query execution failed"))?
        };

        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| create_sql_error(e, "Failed to get batch results"))?;

        all_results.push(rows);
    }

    Ok(all_results)
}

pub fn execute_batch<'p>(
    config: Arc<Config>,
    azure_credential: Option<Arc<PyAzureCredential>>,
    py: Python<'p>,
    commands: &Bound<'p, PyList>,
) -> PyResult<Bound<'p, PyAny>> {
    let batch_commands = parse_batch_items(commands, py)?;

    future_into_py(py, async move {
        // ── Safety: dedicated connection, not a pooled one ─────────────────────────
        //
        // execute_batch wraps all commands in a single BEGIN / COMMIT transaction.
        // If the caller's coroutine is cancelled (e.g. asyncio.Task.cancel()) while
        // the transaction is open, the Rust future is dropped.  With a *pooled*
        // connection the guard would silently return the connection to the pool with
        // an open BEGIN TRANSACTION, corrupting the state seen by the next caller.
        //
        // By using a *dedicated* TCP connection instead:
        //   • If the future is dropped, the TCP socket is closed by the OS.
        //   • SQL Server detects the broken connection and automatically rolls back.
        //   • The shared pool is never touched, so no poisoning is possible.
        //
        // The cost (one extra TCP + TDS handshake per batch call) is acceptable
        // because batch operations are inherently heavy and latency-tolerant.
        // ───────────────────────────────────────────────────────────────────────────

        let tcp = TcpStream::connect(config.get_addr())
            .await
            .map_err(|e| create_connection_error(format!("Failed to connect to server: {}", e)))?;

        // Disable Nagle — same rationale as pool_manager.rs and transaction.rs.
        tcp.set_nodelay(true)
            .map_err(|e| create_connection_error(format!("Failed to set TCP_NODELAY: {}", e)))?;

        // Apply Azure token (or leave config auth as-is for SQL / Windows auth).
        let mut auth_config = (*config).clone();
        if let Some(ref cred) = azure_credential {
            let auth_method = cred
                .to_auth_method()
                .await
                .map_err(|e| create_connection_error(format!("Authentication failed: {}", e)))?;
            auth_config.authentication(auth_method);
        }

        let mut conn = tiberius::Client::connect(auth_config, tcp.compat_write())
            .await
            .map_err(|e| create_sql_error(e, "Failed to connect for batch execution"))?;

        conn.simple_query("BEGIN TRANSACTION")
            .await
            .map_err(|e| create_sql_error(e, "Failed to start transaction"))?;

        let all_results = match execute_batch_on_connection(&mut conn, batch_commands).await {
            Ok(results) => results,
            Err(e) => {
                // Best-effort rollback; ignore secondary errors.
                let _ = conn.simple_query("ROLLBACK TRANSACTION").await;
                return Err(e);
            }
        };

        conn.simple_query("COMMIT TRANSACTION")
            .await
            .map_err(|e| create_sql_error(e, "Failed to commit batch transaction"))?;

        // conn drops here — TCP connection closed cleanly.
        // On future cancellation the OS closes the socket; SQL Server rolls back.

        Python::attach(|py| {
            let py_list = PyList::new(py, all_results)?;
            Ok(py_list.into_any().unbind())
        })
    })
}

pub fn query_batch<'p>(
    pool: Arc<RwLock<Option<ConnectionPool>>>,
    config: Arc<Config>,
    pool_config: PyPoolConfig,
    azure_credential: Option<Arc<PyAzureCredential>>,
    py: Python<'p>,
    queries: &Bound<'p, PyList>,
) -> PyResult<Bound<'p, PyAny>> {
    let batch_queries = parse_batch_items(queries, py)?;

    let pool = Arc::clone(&pool);
    let config = Arc::clone(&config);
    let pool_config = pool_config.clone();

    future_into_py(py, async move {
        let pool_ref =
            ensure_pool_initialized_with_auth(pool, config, &pool_config, azure_credential)
                .await?;

        let mut conn = pool_ref.get().await.map_err(|e| {
            create_connection_error(format!("Failed to get connection from pool: {}", e))
        })?;

        let all_results = query_batch_on_connection(&mut conn, batch_queries).await?;

        Python::attach(|py| -> PyResult<Py<PyAny>> {
            let mut py_results = Vec::with_capacity(all_results.len());
            for result in all_results {
                let query_stream = crate::types::PyQueryStream::from_tiberius_rows(result, py)?;
                let py_result = Py::new(py, query_stream)?;
                py_results.push(py_result.into_any());
            }
            let py_list = PyList::new(py, py_results)?;
            Ok(py_list.into_any().unbind())
        })
    })
}

/// Wraps a single SQL Server identifier part in square brackets and escapes `]` as `]]`.
///
/// Returns `Err` if `part` contains a null byte (`\x00`).  Null bytes are the only
/// character not neutralised by bracket-quoting: some driver layers and C-string APIs
/// treat `\x00` as a string terminator, which could silently truncate the identifier
/// and produce unintended SQL.  All other Unicode characters — including right-to-left
/// override codepoints (U+202E etc.) — are inert inside `[...]` and require no special
/// handling because SQL Server parses bracket-quoted names literally at the byte level.
fn quote_identifier_part(part: &str) -> PyResult<String> {
    if part.contains('\x00') {
        return Err(PyValueError::new_err(
            "Identifier contains a null byte (\\x00), which is not allowed in SQL Server identifiers",
        ));
    }
    let mut quoted = String::with_capacity(part.len() + 2);
    quoted.push('[');
    for ch in part.chars() {
        if ch == ']' {
            quoted.push(']'); // escape ] by doubling
        }
        quoted.push(ch);
    }
    quoted.push(']');
    Ok(quoted)
}

/// Quotes a (possibly multipart) SQL Server identifier, handling forms like:
///   table, schema.table, db.schema.table, db..table
///
/// Each dot-separated part is independently bracket-quoted so that:
/// - `dbo.users`     → `[dbo].[users]`
/// - `mydb..users`   → `[mydb]..[users]`  (empty middle part preserved as-is)
/// - `users`         → `[users]`
///
/// Returns `Err` (propagated from [`quote_identifier_part`]) if any identifier part
/// contains a null byte.
fn quote_identifier(name: &str) -> PyResult<String> {
    let parts: Vec<&str> = name.split('.').collect();
    let mut result = String::with_capacity(name.len() + parts.len() * 2);
    for (i, part) in parts.iter().enumerate() {
        if i > 0 {
            result.push('.');
        }
        if part.is_empty() {
            // preserve empty parts (e.g. the middle segment in db..table)
        } else {
            result.push_str(&quote_identifier_part(part)?);
        }
    }
    Ok(result)
}

/// Fix untyped (U8/tinyint) NULL placeholders in a flat row-major buffer.
///
/// When Python `None` is converted with `python_to_fast_parameter` it becomes
/// `Null(TypedNull::U8)` — a tinyint-typed NULL.  In a multi-row VALUES INSERT
/// SQL Server reconciles parameter types across the same column position in
/// every row, so a tinyint null alongside a nvarchar value causes a conversion
/// error.  This function scans each column, infers the correct type from the
/// first non-null sibling value, and patches every untyped NULL in that column.
fn fix_bulk_null_types(flat_data: &mut [FastParameter], col_count: usize) {
    if col_count == 0 || flat_data.is_empty() {
        return;
    }
    let row_count = flat_data.len() / col_count;

    for col in 0..col_count {
        // Infer the null type from the first non-null value in this column.
        let null_type = (0..row_count)
            .map(|row| &flat_data[row * col_count + col])
            .find_map(|p| match p {
                FastParameter::String(_) => Some(TypedNull::String),
                FastParameter::I64(_) => Some(TypedNull::I64),
                FastParameter::F64(_) => Some(TypedNull::F64),
                FastParameter::Bool(_) => Some(TypedNull::Bit),
                FastParameter::Bytes(_) => Some(TypedNull::Binary),
                FastParameter::Date(_) => Some(TypedNull::Date),
                FastParameter::DateTime(_) => Some(TypedNull::DateTime),
                FastParameter::Null(_) => None,
            })
            .unwrap_or(TypedNull::String); // all-null column → nvarchar null is safe

        // Patch every untyped Null in this column.
        for row in 0..row_count {
            let idx = row * col_count + col;
            if matches!(&flat_data[idx], FastParameter::Null(TypedNull::U8)) {
                flat_data[idx] = FastParameter::Null(null_type.clone());
            }
        }
    }
}

pub fn bulk_insert<'p>(
    pool: Arc<RwLock<Option<ConnectionPool>>>,
    config: Arc<Config>,
    pool_config: PyPoolConfig,
    azure_credential: Option<Arc<PyAzureCredential>>,
    py: Python<'p>,
    table_name: String,
    columns: Vec<String>,
    data_rows: &Bound<'p, PyList>,
) -> PyResult<Bound<'p, PyAny>> {
    if columns.is_empty() {
        return Err(PyValueError::new_err(
            "At least one column must be specified",
        ));
    }

    let col_count = columns.len();

    // Hard limit for SQL Server is 2100. We use 2000 to be safe.
    // Calculate rows_per_batch here (sync, GIL-held phase) so chunking drives
    // conversion rather than being applied after a full allocation.
    let rows_per_batch = (2000usize / col_count).max(1);
    let chunk_capacity = rows_per_batch * col_count;

    // Build owned chunks of at most `rows_per_batch` rows while still holding
    // the GIL.  Each chunk is a self-contained Vec<FastParameter> so the async
    // block can drop it immediately after its INSERT executes, keeping live
    // memory proportional to one chunk rather than the entire dataset.
    //
    // Previously a single flat Vec was allocated for all rows up-front and kept
    // alive until the very last await returned, doubling peak memory for large
    // inputs.
    let num_chunks = data_rows.len().div_ceil(rows_per_batch);
    let mut chunks: Vec<Vec<FastParameter>> = Vec::with_capacity(num_chunks);
    let mut current_chunk: Vec<FastParameter> = Vec::with_capacity(chunk_capacity);

    for row in data_rows.iter() {
        let row_list = row.cast::<PyList>()?;
        if row_list.len() != col_count {
            return Err(PyValueError::new_err(format!(
                "Row has {} values but {} columns specified",
                row_list.len(),
                col_count
            )));
        }
        for value in row_list.iter() {
            current_chunk.push(python_to_fast_parameter(&value)?);
        }

        // Once the chunk holds a full batch worth of rows, fix its null types
        // and move it to the chunks list, then start a fresh allocation.
        if current_chunk.len() >= chunk_capacity {
            fix_bulk_null_types(&mut current_chunk, col_count);
            chunks.push(current_chunk);
            current_chunk = Vec::with_capacity(chunk_capacity);
        }
    }

    // Flush the final (possibly partial) chunk.
    if !current_chunk.is_empty() {
        fix_bulk_null_types(&mut current_chunk, col_count);
        chunks.push(current_chunk);
    }

    future_into_py(py, async move {
        let pool_ref =
            ensure_pool_initialized_with_auth(pool, config, &pool_config, azure_credential)
                .await?;

        let mut conn = pool_ref
            .get()
            .await
            .map_err(|e| create_connection_error(format!("Pool error: {}", e)))?;

        let mut total_affected = 0u64;

        // Quote identifiers to prevent SQL injection (bracket-quote per SQL Server rules).
        // Returns Err if any name contains a null byte.
        let quoted_table = quote_identifier(&table_name)?;
        let columns_sql = columns
            .iter()
            .map(|c| quote_identifier(c))
            .collect::<PyResult<Vec<_>>>()?
            .join(", ");

        // Drain chunks via into_iter: each Vec<FastParameter> is moved out and
        // dropped at the end of its loop body, freeing memory progressively
        // instead of holding all rows alive until the final query completes.
        for chunk in chunks {
            let row_count_in_batch = chunk.len() / col_count;

            // Optimize: Use String with pre-allocated capacity instead of format!
            let mut sql = String::with_capacity(100 + row_count_in_batch * (col_count * 5));
            sql.push_str("INSERT INTO ");
            sql.push_str(&quoted_table);
            sql.push_str(" (");
            sql.push_str(&columns_sql);
            sql.push_str(") VALUES ");

            // Optimize: Build value placeholders more efficiently
            for r in 0..row_count_in_batch {
                if r > 0 {
                    sql.push(',');
                }
                sql.push('(');
                for c in 1..=col_count {
                    if c > 1 {
                        sql.push(',');
                    }
                    sql.push('@');
                    sql.push('P');
                    // Optimized: write integer directly into pre-allocated buffer
                    let param_num = (r * col_count) + c;
                    let _ = write!(sql, "{}", param_num);
                }
                sql.push(')');
            }

            // Use SmallVec to avoid heap allocation for small parameter sets
            let mut params: SmallVec<[&dyn tiberius::ToSql; 128]> =
                SmallVec::with_capacity(chunk.len());
            for p in &chunk {
                params.push(p as &dyn tiberius::ToSql);
            }

            let result = conn
                .execute(sql, &params)
                .await
                .map_err(|e| create_sql_error(e, "Batch execution failed"))?;

            total_affected += result.rows_affected().iter().sum::<u64>();
            // `chunk` is dropped here — its FastParameter memory is freed before
            // the next batch is sent.
        }

        Python::attach(|py| {
            let res = total_affected.into_pyobject(py)?;
            Ok(res.into_any().unbind())
        })
    })
}
