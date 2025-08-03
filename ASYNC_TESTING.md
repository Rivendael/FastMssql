# Async Testing for mssql-python-rust

This document describes the async tests that have been added to the project.

## Overview

The test suite now includes comprehensive async tests alongside the existing synchronous tests. These tests verify that all async functionality works correctly with your Rust-based SQL Server driver.

## Test Coverage

### Async Tests Added:

1. **`test_async_connection_creation()`** - Tests creating async connection objects
2. **`test_async_basic_connection()`** - Tests basic async database connectivity using context manager
3. **`test_async_simple_query()`** - Tests executing simple SELECT queries asynchronously
4. **`test_async_multiple_queries()`** - Tests executing multiple queries on the same async connection
5. **`test_async_data_types()`** - Tests various SQL Server data types with async operations
6. **`test_async_execute_non_query()`** - Tests async INSERT/UPDATE/DELETE operations
7. **`test_async_execute_scalar()`** - Tests async scalar value retrieval
8. **`test_async_convenience_functions()`** - Tests module-level async convenience functions
9. **`test_async_manual_connection_lifecycle()`** - Tests manual async connect/disconnect
10. **`test_async_error_handling()`** - Tests proper async error handling
11. **`test_async_concurrent_queries()`** - Tests executing multiple async queries concurrently

## Running Tests

### All Tests
```bash
python -m pytest tests/ -v
```

### Async Tests Only
```bash
python -m pytest tests/ -v -k "async"
```

### Integration Tests Only
```bash
python -m pytest tests/ -v -m integration
```

### Excluding Integration Tests (for cases without database)
```bash
python -m pytest tests/ -v -m "not integration"
```

## Requirements

- `pytest-asyncio` - For async test support (automatically installed with dev dependencies)
- Active SQL Server instance - For integration tests (tests will be skipped if not available)

## Test Features

### Async Context Managers
Tests verify that async context managers work correctly:
```python
async with mssql.connect_async(connection_string) as conn:
    rows = await conn.execute("SELECT 1")
```

### Concurrent Execution
The concurrent queries test demonstrates running multiple async queries simultaneously:
```python
query1 = conn.execute("SELECT 1 as value, 'query1' as name")
query2 = conn.execute("SELECT 2 as value, 'query2' as name") 
query3 = conn.execute("SELECT 3 as value, 'query3' as name")

results = await asyncio.gather(query1, query2, query3)
```

### Error Handling
Tests ensure that async operations properly handle and propagate errors:
```python
async with mssql.connect_async(TEST_CONNECTION_STRING) as conn:
    with pytest.raises(Exception):
        await conn.execute("SELECT * FROM non_existent_table")
```

## Configuration

The project is configured for async testing in `pyproject.toml`:
```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "auto"
markers = [
    "integration: marks tests as integration tests",
    "asyncio: marks tests as async tests",
]
```

This configuration enables automatic async test detection and provides proper test markers.
