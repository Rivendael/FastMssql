# FastMSSQL ⚡

FastMSSQL is an async Python library for Microsoft SQL Server (MSSQL), built in Rust.
Unlike standard libaries, it uses a native SQL Server client—no ODBC required—simplifying installation on Windows, macOS, and Linux.
Great for data ingestion, bulk inserts, and large-scale query workloads.

[![Python Versions](https://img.shields.io/pypi/pyversions/fastmssql)](https://pypi.org/project/fastmssql/)

[![License](https://img.shields.io/badge/license-MIT%20-green)](LICENSE)

[![Unit Tests](https://github.com/Rivendael/fastmssql/actions/workflows/unittests.yml/badge.svg)](https://github.com/Rivendael/fastmssql/actions/workflows/build.yml)

[![Latest Release](https://img.shields.io/github/v/release/Rivendael/fastmssql)](https://github.com/Rivendael/fastmssql/releases)

[![Platform](https://img.shields.io/badge/platform-Windows%20|%20Linux%20|%20macOS-lightgrey)](https://github.com/Rivendael/fastmssql)

[![Rust Backend](https://img.shields.io/badge/backend-rust-orange)](https://github.com/Rivendael/pymssql-rs)

## Features

- High performance: optimized for very high RPS and low overhead
- Rust core: memory‑safe and reliable, tuned Tokio runtime
- No ODBC: native SQL Server client, no external drivers needed
- Connection pooling: bb8‑based, smart defaults (default max_size=10, min_idle=2)
- Async first: clean async/await API with `async with` context managers
- Strong typing: fast conversions for common SQL Server types
- Thread‑safe: safe to use in concurrent apps
- Cross‑platform: Windows, macOS, Linux
- Batch operations: high-performance bulk inserts and batch query execution


## Key API methods

Core methods for individual operations:

- `query()` — SELECT statements that return rows
- `execute()` — INSERT/UPDATE/DELETE/DDL that return affected row count

```python
# Use query() for SELECT statements
result = await conn.query("SELECT * FROM users WHERE age > @P1", [25])
rows = result.rows()

# Use execute() for data modification
affected = await conn.execute("INSERT INTO users (name) VALUES (@P1)", ["John"])
```


## Installation

### From PyPI (recommended)

```bash
pip install fastmssql
```

### Prerequisites

- Python 3.10 to 3.14
- Microsoft SQL Server (any recent version)

## Quick start

### Basic async usage

```python
import asyncio
from fastmssql import Connection

async def main():
    conn_str = "Server=localhost;Database=master;User Id=myuser;Password=mypass"
    async with Connection(conn_str) as conn:
        # SELECT: use query() -> rows()
        result = await conn.query("SELECT @@VERSION as version")
        for row in result.rows():
            print(row['version'])

        # Pool statistics (tuple: connected, connections, idle, max_size, min_idle)
        connected, connections, idle, max_size, min_idle = await conn.pool_stats()
        print(f"Pool: connected={connected}, size={connections}/{max_size}, idle={idle}, min_idle={min_idle}")

asyncio.run(main())
```

## Explicit Connection Management

When not utilizing Python's context manager (async with), **FastMssql** uses *lazy connection initialization*:  
if you call `query()` or `execute()` on a new `Connection`, the underlying pool is created if not already present. 

For more control, you can explicitly connect and disconnect:

```python
import asyncio
from fastmssql import Connection

async def main():
    conn_str = "Server=localhost;Database=master;User Id=myuser;Password=mypass"
    conn = Connection(conn_str)

    # Explicitly connect
    await conn.connect()
    assert await conn.is_connected()

    # Run queries
    result = await conn.query("SELECT 42 as answer")
    print(result.rows()[0]["answer"])  # -> 42

    # Explicitly disconnect
    await conn.disconnect()
    assert not await conn.is_connected()
    
asyncio.run(main())
```
## Usage

### Connection options

You can connect either with a connection string or individual parameters.

1) Connection string

```python
import asyncio
from fastmssql import Connection

async def main():
    conn_str = "Server=localhost;Database=master;User Id=myuser;Password=mypass"
    async with Connection(connection_string=conn_str) as conn:
        rows = (await conn.query("SELECT DB_NAME() as db")).rows()
        print(rows[0]['db'])

asyncio.run(main())
```

2) Individual parameters

```python
import asyncio
from fastmssql import Connection

async def main():
    async with Connection(
        server="localhost",
        database="master",
        username="myuser",
        password="mypassword"
    ) as conn:
        rows = (await conn.query("SELECT SUSER_SID() as sid")).rows()
        print(rows[0]['sid'])

asyncio.run(main())
```

Note: Windows authentication (Trusted Connection) is currently not supported. Use SQL authentication (username/password).


### Working with data

```python
import asyncio
from fastmssql import Connection

async def main():
    async with Connection("Server=.;Database=MyDB;User Id=sa;Password=StrongPwd;") as conn:
        # SELECT (returns rows)
        users = (await conn.query(
            "SELECT id, name, email FROM users WHERE active = 1"
        )).rows()
        for u in users:
            print(f"User {u['id']}: {u['name']} ({u['email']})")

        # INSERT / UPDATE / DELETE (returns affected row count)
        inserted = await conn.execute(
            "INSERT INTO users (name, email) VALUES (@P1, @P2)",
            ["Jane", "jane@example.com"],
        )
        print(f"Inserted {inserted} row(s)")

        updated = await conn.execute(
            "UPDATE users SET last_login = GETDATE() WHERE id = @P1",
            [123],
        )
        print(f"Updated {updated} row(s)")

asyncio.run(main())
```

Parameters use positional placeholders: `@P1`, `@P2`, ... Provide values as a list in the same order.


### Batch operations

For high-throughput scenarios, use batch methods to reduce network round-trips:

```python
import asyncio
from fastmssql import Connection

async def main_fetching():
    # Replace with your actual connection string
    async with Connection("Server=.;Database=MyDB;User Id=sa;Password=StrongPwd;") as conn:
        
        # --- 1. Prepare Data for Demonstration ---
        columns = ["name", "email", "age"]
        data_rows = [
            ["Alice Johnson", "alice@example.com", 28],
            ["Bob Smith", "bob@example.com", 32],
            ["Carol Davis", "carol@example.com", 25],
            ["David Lee", "david@example.com", 35],
            ["Eva Green", "eva@example.com", 29]
        ]
        await conn.bulk_insert("users", columns, data_rows)

        # --- 2. Execute Query and Retrieve the Result Object ---
        print("\n--- Result Object Fetching (fetchone, fetchmany, fetchall) ---")
        
        # The Result object is returned after the awaitable query executes.
        result = await conn.query("SELECT name, age FROM users ORDER BY age DESC")
        
        # fetchone(): Retrieves the next single row synchronously.
        oldest_user = result.fetchone() 
        if oldest_user:
            print(f"1. fetchone: Oldest user is {oldest_user['name']} (Age: {oldest_user['age']})")
        
        # fetchmany(2): Retrieves the next set of rows synchronously.
        next_two_users = result.fetchmany(2)
        print(f"2. fetchmany: Retrieved {len(next_two_users)} users: {[r['name'] for r in next_two_users]}.")
        
        # fetchall(): Retrieves all remaining rows synchronously.
        remaining_users = result.fetchall()
        print(f"3. fetchall: Retrieved all {len(remaining_users)} remaining users: {[r['name'] for r in remaining_users]}.")
        
        # Exhaustion Check: Subsequent calls return None/[]
        print(f"4. Exhaustion Check (fetchone): {result.fetchone()}")
        print(f"5. Exhaustion Check (fetchmany): {result.fetchmany(1)}")

        # --- 3. Batch Commands for multiple operations ---
        print("\n--- Batch Commands (execute_batch) ---")
        commands = [
            ("UPDATE users SET last_login = GETDATE() WHERE name = @P1", ["Alice Johnson"]),
            ("INSERT INTO user_logs (action, user_name) VALUES (@P1, @P2)", ["login", "Alice Johnson"])
        ]
        
        affected_counts = await conn.execute_batch(commands)
        print(f"Updated {affected_counts[0]} users, inserted {affected_counts[1]} logs")
        
asyncio.run(main_fetching())
```

### Connection pooling

Tune the pool to fit your workload. Constructor signature:

```python
from fastmssql import PoolConfig

# PoolConfig(max_size=10, min_idle=2, max_lifetime_secs=None, idle_timeout_secs=None, connection_timeout_secs=30)
config = PoolConfig(
    max_size=20,              # max connections in pool
    min_idle=5,               # keep at least this many idle
    max_lifetime_secs=3600,   # recycle connections after 1h
    idle_timeout_secs=600,    # close idle connections after 10m
    connection_timeout_secs=30
)
```

Presets:

```python
one.  = PoolConfig.one()                     # ~ max_size=1, min_idle=1
low   = PoolConfig.low_resource()            # ~ max_size=3,   min_idle=1
dev   = PoolConfig.development()             # ~ max_size=5,   min_idle=1
high  = PoolConfig.high_throughput()         # ~ max_size=50,  min_idle=15
maxp  = PoolConfig.performance()             # ~ max_size=100, min_idle=30
```

Apply to a connection:

```python
async with Connection(conn_str, pool_config=high) as conn:
    rows = (await conn.query("SELECT 1 AS ok")).rows()
```

Default pool (if omitted): `max_size=10`, `min_idle=2`.


### SSL/TLS

For `Required` and `LoginOnly` encryption, you must specify how to validate the server certificate:

**Option 1: Trust Server Certificate** (development/self-signed certs):
```python
from fastmssql import SslConfig, EncryptionLevel, Connection

ssl = SslConfig(
    encryption_level=EncryptionLevel.Required,
    trust_server_certificate=True
)

async with Connection(conn_str, ssl_config=ssl) as conn:
    ...
```

**Option 2: Custom CA Certificate** (production):
```python
from fastmssql import SslConfig, EncryptionLevel, Connection

ssl = SslConfig(
    encryption_level=EncryptionLevel.Required,
    ca_certificate_path="/path/to/ca-cert.pem"
)

async with Connection(conn_str, ssl_config=ssl) as conn:
    ...
```

**Note**: `trust_server_certificate` and `ca_certificate_path` are mutually exclusive.

Helpers:

- `SslConfig.development()` – encrypt, trust all (dev only)
- `SslConfig.with_ca_certificate(path)` – use custom CA
- `SslConfig.login_only()` / `SslConfig.disabled()` – legacy modes
- `SslConfig.disabled()` – no encryption (not recommended)


## Performance tips

For maximum throughput in highly concurrent scenarios, use multiple `Connection` instances (each with its own pool) and batch your work:

```python
import asyncio
from fastmssql import Connection, PoolConfig

async def worker(conn_str, cfg):
    async with Connection(conn_str, pool_config=cfg) as conn:
        for _ in range(1000):
            _ = (await conn.query("SELECT 1 as v")).rows()

async def main():
    conn_str = "Server=.;Database=master;User Id=sa;Password=StrongPwd;"
    cfg = PoolConfig.high_throughput()
    await asyncio.gather(*[asyncio.create_task(worker(conn_str, cfg)) for _ in range(32)])

asyncio.run(main())
```


## Examples & benchmarks

- Examples: `examples/comprehensive_example.py`
- Benchmarks: `benchmarks/`


## Troubleshooting

- Import/build: ensure Rust toolchain and `maturin` are installed if building from source
- Connection: verify connection string; Windows auth not supported
- Timeouts: increase pool size or tune `connection_timeout_secs`
- Parameters: use `@P1, @P2, ...` and pass a list of values


## Contributing

Contributions are welcome. Please open an issue or PR.


## License

FastMSSQL is licensed under MIT:

See the [LICENSE](LICENSE) file for details.

## Third‑party attributions

Built on excellent open source projects: Tiberius, PyO3, pyo3‑asyncio, bb8, tokio, serde, pytest, maturin, and more. See `licenses/NOTICE.txt` for the full list. The full texts of Apache‑2.0 and MIT are in `licenses/`.


## Acknowledgments

Thanks to the maintainers of Tiberius, PyO3, pyo3‑asyncio, Tokio, pytest, maturin, and the broader open source community.