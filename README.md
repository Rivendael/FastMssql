# FastMSSQL ⚡

FastMSSQL is an async Python library for Microsoft SQL Server (MSSQL), built in Rust.
Unlike pyodbc or pymssql, it uses a native SQL Server client—no ODBC required—simplifying installation on Windows, macOS, and Linux.
Great for data ingestion, bulk inserts, and large-scale query workloads.

[![PyPI](https://img.shields.io/pypi/v/fastmssql)](https://pypi.org/project/fastmssql/)
[![Python Versions](https://img.shields.io/pypi/pyversions/fastmssql)](https://pypi.org/project/fastmssql/)
[![Rust Backend](https://img.shields.io/badge/backend-rust-orange)](https://github.com/Rivendael/pymssql-rs)
[![Async](https://img.shields.io/badge/async-tokio-blue)](https://github.com/Rivendael/pymssql-rs)
[![License](https://img.shields.io/badge/license-GPL--3.0%20or%20Commercial-green)](LICENSE)



## Contents

- Features
- Installation
- Quick start
- Usage
  - Connection options
  - Working with data (query vs execute)
  - Connection pooling
  - SSL/TLS
- Performance tips
- Examples & benchmarks
- Troubleshooting
- Contributing
- License


## Features

- High performance: optimized for very high RPS and low overhead
- Rust core: memory‑safe and reliable, tuned Tokio runtime
- No ODBC: native SQL Server client, no external drivers needed
- Connection pooling: bb8‑based, smart defaults (default max_size=10, min_idle=2)
- Async first: clean async/await API with `async with` context managers
- Strong typing: fast conversions for common SQL Server types
- Thread‑safe: safe to use in concurrent apps
- Cross‑platform: Windows, macOS, Linux


## Key API methods

Two focused methods cover most use cases:

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

- Python 3.8 to 3.13
- Microsoft SQL Server (any recent version)

### From source (development)

```bash
git clone <your-repo-url>
cd pymssql-rs
./setup.sh
```


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


### Connection strings

```python
# SQL Server Authentication
conn_str = "Server=localhost;Database=MyDB;User Id=sa;Password=MyPassword"

# With specific port
conn_str = "Server=localhost,1433;Database=MyDB;User Id=myuser;Password=mypass"

# Azure SQL Database (encryption recommended)
conn_str = "Server=tcp:myserver.database.windows.net,1433;Database=MyDB;User Id=myuser;Password=mypass;Encrypt=true"
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
high  = PoolConfig.high_throughput()         # ~ max_size=50,  min_idle=15
low   = PoolConfig.low_resource()            # ~ max_size=3,   min_idle=1
dev   = PoolConfig.development()             # ~ max_size=5,   min_idle=1
maxp  = PoolConfig.maximum_performance()     # ~ max_size=100, min_idle=30
ultra = PoolConfig.ultra_high_concurrency()  # ~ max_size=200, min_idle=50
```

Apply to a connection:

```python
async with Connection(conn_str, pool_config=high) as conn:
    rows = (await conn.query("SELECT 1 AS ok")).rows()
```

Default pool (if omitted): `max_size=10`, `min_idle=2`.


### SSL/TLS

```python
from fastmssql import SslConfig, EncryptionLevel, Connection

ssl = SslConfig(
    encryption_level=EncryptionLevel.REQUIRED,  # or "Required"
    trust_server_certificate=False,
)

async with Connection(conn_str, ssl_config=ssl) as conn:
    ...
```

Helpers:

- `SslConfig.development()` – encrypt, trust all (dev only)
- `SslConfig.with_ca_certificate(path)` – use custom CA
- `SslConfig.login_only()` / `SslConfig.disabled()` – legacy modes


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
- Benchmarks: `benchmarks/` (MIT licensed)


## Troubleshooting

- Import/build: ensure Rust toolchain and `maturin` are installed if building from source
- Connection: verify connection string; Windows auth not supported
- Timeouts: increase pool size or tune `connection_timeout_secs`
- Parameters: use `@P1, @P2, ...` and pass a list of values


## Contributing

Contributions are welcome. Please open an issue or PR.


## License

FastMSSQL is dual‑licensed:

- GPL‑3.0 (for open source projects)
- Commercial (for proprietary use). Contact: riverb514@gmail.com

See the [LICENSE](LICENSE) file for details.

### Examples and Benchmarks

- `examples/` and `benchmarks/` are under the MIT License. See files in `licenses/`.


## Third‑party attributions

Built on excellent open source projects: Tiberius, PyO3, pyo3‑asyncio, bb8, tokio, serde, pytest, maturin, and more. See `licenses/NOTICE.txt` for the full list. The full texts of Apache‑2.0 and MIT are in `licenses/`.


## Acknowledgments

Thanks to the maintainers of Tiberius, PyO3, pyo3‑asyncio, Tokio, pytest, maturin, and the broader open source community.