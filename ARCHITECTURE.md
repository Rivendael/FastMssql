# Architecture

This document explains the internal design of FastMSSQL, how the Rust/Python bridge works, and the key optimization strategies used throughout the codebase.

## Table of Contents

1. [High-Level Overview](#high-level-overview)
2. [Technology Stack](#technology-stack)
3. [Project Structure](#project-structure)
4. [Core Components](#core-components)
5. [Execution Flow](#execution-flow)
6. [Performance Optimizations](#performance-optimizations)
7. [Concurrency Model](#concurrency-model)
8. [Type System & Conversion](#type-system--conversion)
9. [Memory Management](#memory-management)
10. [Connection Pooling](#connection-pooling)

---

## High-Level Overview

FastMSSQL is a **Python extension written in Rust** that provides a high-performance, async SQL Server client. The architecture consists of:

```
Python User Code
       ↓
   [PyO3 Bindings] ← Python/Rust Bridge
       ↓
   [Rust Runtime] ← Core async execution
       ↓
  [Tiberius] ← SQL Server protocol client
       ↓
  [Tokio] ← Async I/O runtime
       ↓
  [TCP/TLS] → SQL Server (port 1433)
```

**Key Design Principle:** Minimize latency and maximize throughput by keeping Rust in the hot path while providing a clean Python API.

---

## Technology Stack

### Python-Rust Bridge
- **PyO3 0.27.2** — Python bindings for Rust
  - Handles Python ↔ Rust data marshalling
  - Manages Python reference counting and GIL interactions
  - ABI3 stable across Python 3.10-3.14

### Async Runtime
- **Tokio 1.48** — Async I/O runtime
  - Multi-threaded executor with tuned worker count
  - Handles thousands of concurrent connections
  - Built-in channel types for async communication

- **pyo3-async-runtimes** — PyO3 ↔ Tokio integration
  - Bridges Python's async/await with Tokio
  - Handles GIL release during async operations

### Database Client
- **Tiberius 0.12** — Native SQL Server TDS protocol client
  - Pure Rust implementation (no ODBC/ODBC drivers needed)
  - Features: parameterized queries, SSL/TLS, connection pooling integration
  - Uses `tokio-tls` for encrypted connections
  - Supports TCP and named pipes on Windows

### Connection Pool
- **BB8 0.9** — Async connection pool manager
  - Type-agnostic pooling (works with any async connection type)
  - Configurable min/max sizes and timeouts
  - Health checking and automatic reconnection

### Optimization Libraries
- **MiMalloc** — Microsoft's high-performance memory allocator
  - Reduces allocation latency by ~30-50% vs default allocator
  - Better multi-threaded performance
  - Enabled globally via `#[global_allocator]`

- **SmallVec 1.15** — Stack-allocated vectors for small collections
  - Parameters: up to 16 stored on stack, no heap allocation
  - Avoids allocation overhead for typical query parameter counts

- **parking_lot 0.12** — Faster mutex/RwLock than `std::sync`
  - Lower overhead locking primitive
  - Used for connection pool synchronization

- **ahash 0.8** — Fast hashing algorithm
  - ~3x faster than SipHash for short strings
  - Used internally by Tokio and collections

---

## Project Structure

```
src/
├── lib.rs                      # Entry point, module declarations, Tokio setup
├── connection.rs               # PyConnection class, query/execute methods
├── pool_manager.rs             # Connection pool initialization and management
├── batch.rs                    # Batch query operations
├── parameter_conversion.rs      # Python → Rust parameter conversion
├── parameter_utils.rs          # Helper functions for parameter parsing
├── py_parameters.rs            # Python Parameter/Parameters classes
├── type_mapping.rs             # SQL Server ↔ Python type conversions
├── ssl_config.rs               # SSL/TLS configuration
├── pool_config.rs              # Connection pool configuration
└── types.rs                    # PyFastRow, PyFastExecutionResult result types

python/
└── fastmssql/
    └── __init__.py             # Python package root

fastmssql.pyi                   # Type hints for IDE support
```

---

## Core Components

### 1. Module Initialization (`lib.rs`)

The entry point configures the entire runtime environment:

```rust
#[pymodule]
fn fastmssql(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    
    // Tune thread pool for high RPS
    builder
        .worker_threads((cpu_count / 2).max(1).min(8))
        .max_blocking_threads((cpu_count * 32).min(512))
        .thread_keep_alive(Duration::from_secs(900))
        .thread_stack_size(4 * 1024 * 1024)
        .global_queue_interval(7)
        .event_interval(13);
    
    pyo3_async_runtimes::tokio::init(builder);
    // ... register classes ...
}
```

**Design Decisions:**

- **Worker threads:** CPU count / 2 (fewer threads = less contention at high RPS)
- **Blocking threads:** CPU count × 32 (surge capacity for DB I/O)
- **Keep-alive:** 15 minutes (prevents thread thrashing)
- **Stack size:** 4 MB (allows more threads for high concurrency)
- **Queue intervals:** Tuned for low-latency, high-throughput workloads

### 2. Connection Class (`connection.rs`)

The main user-facing API:

```python
# User code
async with Connection(conn_str, ssl_config=ssl_config) as conn:
    result = await conn.query("SELECT * FROM users WHERE id = @P1", [user_id])
    rows = result.rows()
```

**Implementation Details:**

- Stores connection pool state in `Arc<Mutex<Option<ConnectionPool>>>`
  - Arc: Shared ownership across async tasks
  - Mutex: Synchronize lazy initialization
  - Option: Pool created on first use (lazy initialization)

- **GIL Handling:** Async operations release Python's Global Interpreter Lock
  ```rust
  pub fn query<'p>(&self, sql: &str, parameters: Option<Bound<'p, PyAny>>) 
    -> PyResult<Bound<'p, PyAny>> {
    // GIL is released during future_into_py
    future_into_py(py, self.query_async(...))
  }
  ```

- **Error Handling:** Rich error messages for debugging
  ```rust
  .map_err(|e| {
    PyRuntimeError::new_err(
      format!("Query execution failed: {}", e)
    )
  })
  ```

### 3. Connection Pool (`pool_manager.rs`)

Manages the BB8 connection pool with thread-safe initialization:

```rust
pub async fn ensure_pool_initialized(
    pool: Arc<Mutex<Option<ConnectionPool>>>,
    config: Arc<Config>,
    pool_config: &PyPoolConfig,
) -> PyResult<ConnectionPool> {
    // Fast path: check if already initialized (lock released before async)
    {
        let pool_guard = pool.lock();
        if let Some(ref p) = *pool_guard {
            return Ok(p.clone());
        }
    } // Lock released here
    
    // Slow path: initialize if needed
    let new_pool = establish_pool(&config, pool_config).await?;
    
    // Double-check pattern: another thread might have initialized first
    let mut pool_guard = pool.lock();
    if let Some(ref p) = *pool_guard {
        Ok(p.clone())
    } else {
        *pool_guard = Some(new_pool.clone());
        Ok(new_pool)
    }
}
```

**Key Pattern:** Double-checked locking
- Fast path: No lock contention (pool already initialized)
- Slow path: Lock released before `await` (no blocking I/O under lock)
- Thread-safe initialization without deadlock risk

**BB8 Configuration:**

```rust
Pool::builder()
    .retry_connection(true)      // Auto-reconnect on failure
    .max_size(pool_config.max_size)           // Default: 10
    .min_idle(pool_config.min_idle)           // Default: 2
    .max_lifetime(pool_config.max_lifetime)   // Conn lifetime limit
    .idle_timeout(pool_config.idle_timeout)   // Idle timeout
    .build(manager)
```

### 4. Batch Operations (`batch.rs`)

High-performance batch execution for multiple queries:

```python
# User code
result = await conn.execute_batch([
    ("INSERT INTO users (name) VALUES (@P1)", ["Alice"]),
    ("INSERT INTO users (name) VALUES (@P1)", ["Bob"]),
])
```

**Implementation:**

- Parses list of (SQL, parameters) tuples
- Converts parameters once at the start (not per query)
- Executes sequentially in a single transaction-like context
- Returns aggregate results (total affected rows, any errors)

**Optimization:** Uses `SmallVec` for parameter storage, avoiding allocations for typical batch sizes.

### 5. Parameter Conversion (`parameter_conversion.rs`)

Bridges Python and Rust type systems:

```rust
pub enum FastParameter {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
}

impl tiberius::ToSql for FastParameter {
    fn to_sql(&self) -> tiberius::ColumnData<'_> {
        match self {
            FastParameter::Null => tiberius::ColumnData::U8(None),
            FastParameter::Bool(b) => b.to_sql(),
            FastParameter::I64(i) => i.to_sql(),
            // ... other types
        }
    }
}
```

**Conversion Process:**

1. Python object → `FastParameter` enum
2. `FastParameter` implements `tiberius::ToSql`
3. Tiberius converts to TDS protocol bytes

**SmallVec Optimization:**

```rust
let mut result: SmallVec<[FastParameter; 16]> = SmallVec::with_capacity(len);
// 0-16 parameters: zero heap allocations
// 17+ parameters: single heap allocation (rare)
```

### 6. Type Mapping (`type_mapping.rs`)

Converts SQL Server column values to Python objects:

```rust
#[inline(always)]
fn handle_int4(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<i32, usize>(index) {
        Ok(Some(val)) => Ok((val as i64).into_pyobject(py)?.into_any().unbind()),
        _ => Ok(py.None())
    }
}
```

**Supported SQL Server Types:**

| SQL Server Type | Python Type | Notes |
|---|---|---|
| INT, BIGINT | int | 32/64-bit signed integers |
| FLOAT, REAL | float | IEEE 754 floating point |
| VARCHAR, NVARCHAR | str | UTF-8 strings |
| CHAR, NCHAR | str | Fixed-width strings |
| BIT | int | 0 or 1 |
| BINARY, VARBINARY | bytes | Raw bytes |
| DECIMAL, NUMERIC | str | High-precision (converted to string) |
| DATETIME, DATETIME2 | str | ISO 8601 format |
| DATE, TIME | str | Specialized formats |
| UNIQUEIDENTIFIER | str | UUID as string |
| NULL | None | Python None |

**Optimization Strategy:**

- `#[inline(always)]` on type handlers allows compiler to specialize per column type
- Minimal branching in hot paths
- Early return for NULL values
- Uses `try_get` to handle missing columns gracefully

### 7. SSL/TLS Configuration (`ssl_config.rs`)

Manages encrypted database connections:

```python
from fastmssql import SslConfig, EncryptionLevel

ssl_config = SslConfig(
    encryption_level=EncryptionLevel.Required,
    ca_certificate_path="/path/to/ca.pem"
)
```

**Encryption Levels:**

- **Required:** All traffic encrypted (recommended)
- **LoginOnly:** Only credentials encrypted
- **Off:** No encryption (development only)

**Certificate Validation:**

- Mutually exclusive: either trust server OR provide CA certificate
- File existence and readability checked at construction time
- Supported formats: `.pem`, `.crt`, `.der`

---

## Execution Flow

### 1. Single Query Execution

```
Python:
    result = await conn.query("SELECT @@VERSION", [])
                ↓
Rust (PyConnection.query):
    1. Convert parameters: Python → FastParameter
    2. Release GIL
    3. future_into_py() creates Python coroutine
                ↓
Rust (Async):
    4. ensure_pool_initialized() - lazy create pool
    5. Get connection from BB8 pool
    6. Build Tiberius parameters
    7. Execute query via tiberius::Client::query()
    8. Collect rows into Vec<Row>
                ↓
Rust (Type Conversion):
    9. For each row:
       - For each column:
         - Use type_mapping to convert to PyObject
       - Store in PyFastRow
    10. Wrap in PyFastExecutionResult
                ↓
Python:
    11. Await future, get PyFastExecutionResult
    12. Call .rows() to get list of PyFastRow dicts
```

### 2. Batch Execution

```
Python:
    result = await conn.execute_batch([
        ("INSERT ... VALUES (@P1)", ["Alice"]),
        ("INSERT ... VALUES (@P1)", ["Bob"]),
    ])
                ↓
Rust (batch.rs):
    1. Parse batch items - extract SQL and parameters
    2. Convert all parameters in one pass
    3. Release GIL
    4. future_into_py() creates Python coroutine
                ↓
Rust (Async):
    5. ensure_pool_initialized()
    6. For each batch item:
       - Get connection from pool
       - Build Tiberius parameters
       - Execute query
       - Accumulate affected row count
    7. Return aggregate result
                ↓
Python:
    8. Await future, get batch execution result
```

---

## Performance Optimizations

### 1. Zero-Copy Parameter Passing

**Problem:** Copying parameters between Python and Rust adds latency.

**Solution:** Direct conversion without intermediate allocations
```rust
// BEFORE: Multiple allocations
let params = Vec::new();
for param in py_params {
    params.push(python_to_fast_parameter(param));
}

// AFTER: Stack allocation with SmallVec
let mut params: SmallVec<[FastParameter; 16]> = SmallVec::with_capacity(len);
// Typical queries have ≤16 parameters → zero heap allocation
```

### 2. Global Memory Allocator

**Problem:** Default Rust allocator has higher latency.

**Solution:** MiMalloc allocator
```rust
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
```

**Benefits:**
- 30-50% lower allocation latency
- Better multi-threaded performance
- Uses per-thread freelists

### 3. GIL Release Strategy

**Problem:** Python GIL blocks other threads during long Rust operations.

**Solution:** Release GIL before async work
```rust
pub fn query<'p>(&self, sql: &str, parameters: Option<Bound<'p, PyAny>>) 
    -> PyResult<Bound<'p, PyAny>> {
    let py = query.py();
    // GIL is released inside future_into_py
    future_into_py(py, self.query_async(sql, parameters, py))
}
```

This allows Python to execute other threads while database I/O happens.

### 4. Lazy Initialization

**Problem:** Creating pools/runtimes on import adds startup latency.

**Solution:** Create on first use
```rust
let pool_guard = pool.lock();
if let Some(ref p) = *pool_guard {
    return Ok(p.clone());  // Already initialized
}
// Create only on first query
```

### 5. Type Handler Inlining

**Problem:** Type dispatch has branch misprediction overhead.

**Solution:** `#[inline(always)]` on handlers lets compiler specialize
```rust
#[inline(always)]
fn handle_int4(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    // Compiler generates specialized code for each call site
    // Zero runtime dispatch
}
```

### 6. Batch Parameter Optimization

**Problem:** Converting parameters per query is redundant.

**Solution:** Single conversion pass
```rust
// Convert all parameters once
let fast_params = convert_parameters_to_fast(Some(&params), py)?;
// Reuse or apply to queries
```

### 7. Connection Pool Defaults

**Problem:** Poor pool settings hurt throughput.

**Solution:** Tuned defaults based on deployment patterns
```rust
PoolConfig {
    max_size: 10,        // Few open connections
    min_idle: 2,         // Pre-warmed
    max_lifetime: 1800s, // 30-minute rotation
}
```

---

## Concurrency Model

### Tokio Runtime Configuration

FastMSSQL uses a **tuned Tokio multi-threaded runtime:**

```rust
let builder = tokio::runtime::Builder::new_multi_thread();

builder
    .worker_threads((cpu_count / 2).max(1).min(8))     // ← Few workers
    .max_blocking_threads((cpu_count * 32).min(512))   // ← Many blockers
    .thread_keep_alive(Duration::from_secs(900))       // ← Long lived
    .thread_stack_size(4 * 1024 * 1024)                // ← Smaller stacks
    .global_queue_interval(7)                           // ← Less contention
    .event_interval(13);                                // ← Better cache locality
```

**Rationale:**

- **Fewer worker threads:** Reduces work-stealing contention when handling many connections
- **Many blocking threads:** SQL queries block, so we need capacity for surge loads
- **Long keep-alive:** Amortizes thread creation cost
- **Small stacks:** More threads fit in memory, better for high concurrency
- **Global queue interval:** Balances per-thread queues with global queue (reduces contention)
- **Event interval:** Batches event polling for cache efficiency

### Task Model

Each query execution is a single async task:
```
Connection::query()
    ↓
Tokio spawns task (no blocking)
    ↓
Task waits on pool.get() (async)
    ↓
Task executes tiberius query (async I/O)
    ↓
Task collects results (CPU-bound, fast)
    ↓
Task returns to Python
```

**Key Property:** No task blocks, so thousands of queries can be in-flight simultaneously.

### Synchronization Primitives

| Primitive | Usage | Why |
|---|---|---|
| `Arc<Mutex<Option<Pool>>>` | Connection pool storage | Shared ownership, single-threaded access to pool creation |
| `parking_lot::Mutex` | Pool lock | Faster than `std::sync::Mutex` |
| `Arc<Config>` | Shared connection config | Zero-copy reference across tasks |

---

## Type System & Conversion

### Parameter Conversion Flow

```
Python Input (user code)
    ↓
PyO3 binding receives as Bound<'p, PyAny>
    ↓
python_to_fast_parameter() → FastParameter enum
    - Null:       → FastParameter::Null
    - str:        → FastParameter::String
    - int:        → FastParameter::I64
    - float:      → FastParameter::F64
    - bool:       → FastParameter::Bool
    - bytes:      → FastParameter::Bytes
    ↓
FastParameter implements tiberius::ToSql
    ↓
Tiberius converts to TDS protocol bytes
    ↓
SQL Server (port 1433)
```

### Result Conversion Flow

```
SQL Server (rows over TDS protocol)
    ↓
Tiberius parses bytes → tiberius::Row
    ↓
type_mapping::convert_row_to_pydict()
    - For each column:
      - Use ColumnType to dispatch to handler
      - Handler calls row.try_get::<T, usize>()
      - T::from converted to PyObject
    ↓
PyFastRow: ordered dict of column → value
    ↓
PyFastExecutionResult: list of PyFastRow
    ↓
Python user receives result.rows() → List[Dict[str, Any]]
```

---

## Memory Management

### Stack Allocation Strategy

**Goal:** Minimize heap allocations for typical workloads.

**SmallVec for Parameters:**
```rust
// Stack storage for 16 parameters
SmallVec<[FastParameter; 16]>
// 0-16 params: no heap allocation
// 17+ params: automatic heap allocation (rare)
```

**Typical query:**
```python
await conn.query("SELECT * FROM users WHERE id = @P1", [42])
# 1 parameter → stored entirely on stack
# 0 heap allocations
```

### Reference Counting

**Goal:** Shared ownership without garbage collection overhead.

**Arc Usage:**
```rust
pub struct PyConnection {
    pool: Arc<Mutex<Option<ConnectionPool>>>,
    config: Arc<Config>,
    // ...
}
```

**Pattern:**
- Arc is cloned when passing to async tasks (cheap, atomic operation)
- Actual data is not copied
- When last Arc is dropped, data is deallocated

### Connection Lifecycle

```
Connection created
    ↓
Pool created lazily on first query
    ↓
BB8 creates Tiberius clients (TCP connections)
    ↓
Connections reused across multiple queries
    ↓
BB8 periodically checks connection health
    ↓
Idle connections timeout and close
    ↓
Connection dropped
    ↓
Pool garbage collected when last Arc reference drops
```

---

## Connection Pooling

### BB8 Pool Details

**Type:** `Pool<ConnectionManager>` where ConnectionManager = Tiberius

**Lifecycle:**

1. **Creation** (lazy, on first query)
   ```rust
   let pool = Pool::builder()
       .max_size(10)
       .min_idle(2)
       .build(manager)
       .await?;
   ```

2. **Get Connection**
   ```rust
   let mut conn = pool.get().await?;
   // If available: instant return
   // If exhausted: waits for connection to return
   // If below min_idle: creates new connection
   ```

3. **Query Execution**
   ```rust
   let result = conn.query(sql, &params).await?;
   // Connection stays checked out during query
   // GIL is released, other threads can run
   ```

4. **Return Connection**
   - Implicit when `conn` is dropped
   - BB8 returns it to pool
   - Health checked on return

### Configuration Best Practices

```python
from fastmssql import Connection, PoolConfig

pool_config = PoolConfig(
    max_size=10,              # Typical app: 5-20
    min_idle=2,               # Pre-warm connections
    max_lifetime=1800,        # 30 min (prevents stale connections)
    idle_timeout=600,         # 10 min (close idle connections)
    connection_timeout=30     # 30 sec (timeout waiting for connection)
)

async with Connection(conn_str, pool_config=pool_config) as conn:
    result = await conn.query("SELECT 1")
```

**Tuning:**
- **High throughput (> 1000 RPS):** Increase `max_size` to 20-50
- **Limited resources:** Decrease `max_size` to 5, increase `min_idle` to 0
- **Long-running app:** Enable `max_lifetime` to rotate connections

---

## Summary: Why It's Fast

1. **Rust Core:** No garbage collection, memory-safe, optimized
2. **Tokio Runtime:** Async I/O without blocking threads
3. **Native TDS Client:** No ODBC/drivers, direct protocol implementation
4. **Tuned Memory:** MiMalloc + SmallVec eliminate allocation overhead
5. **GIL Release:** Python can use other threads during I/O
6. **Type Specialization:** Compiler generates optimized code per type
7. **Lazy Initialization:** No startup overhead
8. **Connection Pooling:** Reuse TCP connections, avoid handshake latency
9. **Batch Operations:** Single I/O round-trip for multiple queries
10. **Zero-Copy Conversions:** Direct marshalling without intermediate allocations

---

## Further Reading

- [Tiberius Documentation](https://github.com/steffengy/tiberius)
- [PyO3 Guide](https://pyo3.rs/)
- [Tokio Tutorial](https://tokio.rs/tokio/tutorial)
- [BB8 Pool](https://github.com/djc/bb8)
- [MiMalloc Paper](https://www.microsoft.com/en-us/research/publication/mimalloc-free-list-sharding-by-location/)
