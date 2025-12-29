use crate::parameter_conversion::{convert_parameters_to_fast, python_to_fast_parameter, FastParameter};
use crate::pool_config::PyPoolConfig;
use crate::pool_manager::{ensure_pool_initialized, ConnectionPool};
use crate::types::PyFastExecutionResult;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3_async_runtimes::tokio::future_into_py;
use smallvec::SmallVec;
use std::sync::Arc;
use parking_lot::Mutex;
use tiberius::Config;

/// Parses batch items (SQL queries with parameters) from a Python list.
fn parse_batch_items<'p>(
    items: &Bound<'p, PyList>,
    py: Python<'p>,
) -> PyResult<Vec<(String, SmallVec<[FastParameter; 16]>)>> {
    let mut batch_items = Vec::with_capacity(items.len());

    for item in items.iter() {
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
            convert_parameters_to_fast(Some(&params_py), py)?
        };

        batch_items.push((sql, fast_params));
    }

    Ok(batch_items)
}

pub fn execute_batch<'p>(
    pool: Arc<Mutex<Option<ConnectionPool>>>,
    config: Arc<Config>,
    pool_config: PyPoolConfig,
    py: Python<'p>,
    commands: &Bound<'p, PyList>,
) -> PyResult<Bound<'p, PyAny>> {
    let batch_commands = parse_batch_items(commands, py)?;

    // PERFORMANCE CRITICAL: Move Arc values directly without intermediate clones
    // Arc::clone() is cheap, but the move statement doesn't clone - it transfers ownership
    let pool = Arc::clone(&pool);
    let config = Arc::clone(&config);
    let pool_config = pool_config.clone();

    future_into_py(py, async move {
        let pool_ref = ensure_pool_initialized(pool, config, &pool_config).await?;

        let mut conn = pool_ref
            .get()
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Pool error: {}", e)))?;

        let mut all_results = Vec::with_capacity(batch_commands.len());

        conn.simple_query("BEGIN TRANSACTION")
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to start transaction: {}", e)))?;

        for (sql, parameters) in batch_commands {
            let tiberius_params: SmallVec<[&dyn tiberius::ToSql; 16]> = parameters
                .iter()
                .map(|p| p as &dyn tiberius::ToSql)
                .collect();

            match conn.execute(sql, &tiberius_params).await {
                Ok(result) => {
                    let affected: u64 = result.rows_affected().iter().sum();
                    all_results.push(affected);
                }
                Err(e) => {
                    let _ = conn.simple_query("ROLLBACK TRANSACTION").await;
                    return Err(PyRuntimeError::new_err(format!("Batch item failed: {}", e)));
                }
            }
        }

        conn.simple_query("COMMIT TRANSACTION")
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to commit batch: {}", e)))?;

        Python::attach(|py| {
            let py_list = PyList::new(py, all_results)?;
            Ok(py_list.into_any().unbind())
        })
    })
}

pub fn query_batch<'p>(
    pool: Arc<Mutex<Option<ConnectionPool>>>,
    config: Arc<Config>,
    pool_config: PyPoolConfig,
    py: Python<'p>,
    queries: &Bound<'p, PyList>,
) -> PyResult<Bound<'p, PyAny>> {
    let batch_queries = parse_batch_items(queries, py)?;

    // PERFORMANCE CRITICAL: Move Arc values directly without intermediate clones
    // Arc::clone() is cheap, but the move statement doesn't clone - it transfers ownership
    let pool = Arc::clone(&pool);
    let config = Arc::clone(&config);
    let pool_config = pool_config.clone();

    future_into_py(py, async move {
        let pool_ref = ensure_pool_initialized(pool, config, &pool_config).await?;

        // Execute all queries in sequence on a single connection
        let mut conn = pool_ref.get().await.map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to get connection from pool: {}", e))
        })?;

        let mut all_results = Vec::with_capacity(batch_queries.len());

        for (query, parameters) in batch_queries {
            let tiberius_params: SmallVec<[&dyn tiberius::ToSql; 16]> = parameters
                .iter()
                .map(|p| p as &dyn tiberius::ToSql)
                .collect();

            let stream = conn.query(&query, &tiberius_params).await.map_err(|e| {
                PyRuntimeError::new_err(format!("Batch query execution failed: {}", e))
            })?;

            let rows = stream.into_first_result().await.map_err(|e| {
                PyRuntimeError::new_err(format!("Failed to get batch results: {}", e))
            })?;

            all_results.push(rows);
        }

        Python::attach(|py| -> PyResult<Py<PyAny>> {
            let mut py_results = Vec::with_capacity(all_results.len());
            for result in all_results {
                let fast_result = PyFastExecutionResult::with_rows(result, py)?;
                let py_result = Py::new(py, fast_result)?;
                py_results.push(py_result.into_any());
            }
            let py_list = PyList::new(py, py_results)?;
            Ok(py_list.into_any().unbind())
        })
    })
}

pub fn bulk_insert<'p>(
    pool: Arc<Mutex<Option<ConnectionPool>>>,
    config: Arc<Config>,
    pool_config: PyPoolConfig,
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

    let mut flat_data: Vec<FastParameter> = Vec::with_capacity(data_rows.len() * columns.len());
    for row in data_rows.iter() {
        let row_list = row.cast::<PyList>()?;
        if row_list.len() != columns.len() {
            return Err(PyValueError::new_err(format!(
                "Row has {} values but {} columns specified",
                row_list.len(),
                columns.len()
            )));
        }
        for value in row_list.iter() {
            flat_data.push(python_to_fast_parameter(&value)?);
        }
    }

    let col_count = columns.len();

    future_into_py(py, async move {
        let pool_ref = ensure_pool_initialized(pool, config, &pool_config).await?;

        let mut conn = pool_ref
            .get()
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Pool error: {}", e)))?;

        let mut total_affected = 0u64;

        // Hard limit for SQL Server is 2100. We use 2000 to be safe.
        let max_params_per_request = 2000;
        let rows_per_batch = max_params_per_request / col_count;

        // Ensure we handle the case where a single row has > 2000 columns (unlikely but safe)
        let rows_per_batch = if rows_per_batch == 0 {
            1
        } else {
            rows_per_batch
        };

        // Pre-build the column list once
        let columns_sql = columns.join(", ");

        for chunk in flat_data.chunks(rows_per_batch * col_count) {
            let row_count_in_batch = chunk.len() / col_count;

            // Optimize: Use String with pre-allocated capacity instead of format!
            let mut sql = String::with_capacity(100 + row_count_in_batch * (col_count * 5));
            sql.push_str("INSERT INTO ");
            sql.push_str(&table_name);
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
                    // Optimized: write integer directly without format!
                    let param_num = (r * col_count) + c;
                    for digit in param_num.to_string().chars() {
                        sql.push(digit);
                    }
                }
                sql.push(')');
            }

            let params: Vec<&dyn tiberius::ToSql> =
                chunk.iter().map(|p| p as &dyn tiberius::ToSql).collect();

            let result = conn
                .execute(sql, &params)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Batch execution failed: {}", e)))?;

            total_affected += result.rows_affected().iter().sum::<u64>();
        }

        Python::attach(|py| {
            let res = total_affected.into_pyobject(py)?;
            Ok(res.into_any().unbind())
        })
    })
}
