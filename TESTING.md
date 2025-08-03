# Testing Guide for mssql-python-rust

This guide describes the comprehensive test suite for the mssql-python-rust library.

## Overview

The test suite is organized into multiple modules to provide thorough coverage of all SQL Server operations and edge cases:

- **test_basic.py** - Basic functionality and connection tests
- **test_data_types.py** - SQL Server data type handling
- **test_ddl.py** - Data Definition Language (CREATE, ALTER, DROP)
- **test_dml.py** - Data Manipulation Language (SELECT, INSERT, UPDATE, DELETE)
- **test_advanced_sql.py** - Stored procedures, functions, CTEs, window functions
- **test_performance.py** - Performance and stress testing
- **test_error_handling.py** - Error scenarios and edge cases

## Test Categories

### Basic Tests
Tests that don't require a database connection:
```bash
python run_tests.py                    # Basic tests only
pytest tests/test_basic.py::test_version -v
```

### Integration Tests
Tests that require a working SQL Server database:
```bash
python run_tests.py --integration      # All integration tests
pytest tests/ -m integration -v        # Using pytest directly
```

### Performance Tests
Tests that measure performance characteristics:
```bash
python run_tests.py --performance      # Performance tests
pytest tests/test_performance.py -m performance -v
```

### Stress Tests
High-load testing scenarios:
```bash
pytest tests/test_performance.py -m stress -v
```

## Running Tests

### Prerequisites

1. **Install development dependencies:**
   ```bash
   pip install pytest pytest-asyncio pytest-cov pytest-timeout
   ```

2. **Build the library:**
   ```bash
   maturin develop
   ```

3. **Configure database connection:**
   Update the `TEST_CONNECTION_STRING` in test files to point to your SQL Server instance.

### Test Execution Options

#### Using the test runner script:
```bash
# Basic functionality tests
python run_tests.py

# Integration tests (require database)
python run_tests.py --integration

# Performance tests
python run_tests.py --performance

# All tests
python run_tests.py --all

# Specific test file
python run_tests.py --file test_data_types

# Check prerequisites
python run_tests.py --check

# List available test files
python run_tests.py --list
```

#### Using pytest directly:
```bash
# All tests
pytest tests/ -v

# Only integration tests
pytest tests/ -m integration -v

# Skip integration tests (basic tests only)
pytest tests/ -m "not integration" -v

# Only async tests
pytest tests/ -k async -v

# Specific test file
pytest tests/test_data_types.py -v

# Specific test function
pytest tests/test_basic.py::test_version -v

# With coverage report
pytest tests/ --cov=mssql_python_rust --cov-report=html
```

## Test Structure

### Data Types Testing (`test_data_types.py`)
Tests all SQL Server data types:
- Numeric types (INT, BIGINT, DECIMAL, FLOAT, etc.)
- String types (VARCHAR, NVARCHAR, TEXT, etc.)
- Date/time types (DATE, TIME, DATETIME, etc.)
- Binary types (BINARY, VARBINARY, IMAGE)
- Special types (BIT, UNIQUEIDENTIFIER, XML)
- NULL value handling
- Large values and boundary conditions

### DDL Testing (`test_ddl.py`)
Tests Data Definition Language operations:
- CREATE/DROP TABLE
- ALTER TABLE (add/modify columns)
- CREATE/DROP INDEX (regular, unique, composite)
- CREATE/DROP VIEW
- CREATE/DROP PROCEDURE
- CREATE/DROP FUNCTION
- Schema operations
- Constraint management

### DML Testing (`test_dml.py`)
Tests Data Manipulation Language operations:
- INSERT (single, bulk, with defaults)
- SELECT (simple, complex, with JOINs, aggregates)
- UPDATE (single, bulk, with calculations)
- DELETE (single, bulk, with conditions)
- MERGE (UPSERT) operations
- Transaction scenarios

### Advanced SQL Testing (`test_advanced_sql.py`)
Tests advanced SQL Server features:
- Stored procedure execution
- User-defined functions (scalar and table-valued)
- Common Table Expressions (CTEs)
- Window functions (ROW_NUMBER, RANK, LAG/LEAD)
- PIVOT/UNPIVOT operations
- Temporary tables and variables
- Batch operations

### Performance Testing (`test_performance.py`)
Tests performance characteristics:
- Large result set handling
- Concurrent connections
- Bulk insert performance
- Repeated query execution
- Memory usage with large data
- Connection pooling simulation
- Long-running queries
- Mixed read/write operations

### Error Handling Testing (`test_error_handling.py`)
Tests error scenarios and edge cases:
- Invalid connection strings
- SQL syntax errors
- Constraint violations
- Data type conversion errors
- Connection interruption
- NULL and empty value handling
- Special characters in data
- Boundary value testing
- Multiple result sets

## Test Configuration

### Connection String
Update the test connection string in each test file:
```python
TEST_CONNECTION_STRING = "Server=YOUR_SERVER;Database=YOUR_DB;Integrated Security=true;TrustServerCertificate=yes"
```

### Test Markers
Tests are marked with pytest markers:
- `@pytest.mark.integration` - Requires database connection
- `@pytest.mark.performance` - Performance testing
- `@pytest.mark.stress` - Stress testing
- `@pytest.mark.asyncio` - Async operations

### Test Fixtures
Some tests use fixtures for setup/teardown:
- Database table creation and cleanup
- Test data setup
- Stored procedure management

## Database Requirements

### Minimum Requirements
- SQL Server 2016 or later (including SQL Server Express)
- A test database with full permissions
- Network connectivity to the SQL Server instance

### Recommended Setup
- Dedicated test database (e.g., `pymssql_test`)
- SQL Server Express LocalDB for local development
- Separate test instance to avoid affecting production data

### Test Database Setup
```sql
-- Create test database
CREATE DATABASE pymssql_test;

-- Grant permissions (if needed)
USE pymssql_test;
-- Add appropriate permissions for your test user
```

## Continuous Integration

### GitHub Actions Example
```yaml
name: Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: windows-latest
    services:
      mssql:
        image: mcr.microsoft.com/mssql/server:2019-latest
        env:
          SA_PASSWORD: YourStrong@Passw0rd
          ACCEPT_EULA: Y
        options: >-
          --health-cmd "/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P YourStrong@Passw0rd -Q 'SELECT 1'"
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
    
    steps:
    - uses: actions/checkout@v3
    - uses: actions/setup-python@v4
      with:
        python-version: '3.11'
    - name: Install Rust
      uses: actions-rs/toolchain@v1
      with:
        toolchain: stable
    - name: Install dependencies
      run: |
        pip install maturin pytest pytest-asyncio
        maturin develop
    - name: Run tests
      run: python run_tests.py --integration
```

## Troubleshooting

### Common Issues

1. **Connection failures:**
   - Verify SQL Server is running
   - Check connection string format
   - Ensure network connectivity
   - Verify authentication method

2. **Permission errors:**
   - Ensure test user has necessary database permissions
   - Check if database exists
   - Verify CREATE/DROP permissions for DDL tests

3. **Timeout issues:**
   - Increase test timeout in pytest configuration
   - Check server performance
   - Consider reducing test data size

4. **Library not found:**
   - Run `maturin develop` to build the library
   - Check Python path configuration
   - Verify virtual environment activation

### Test Data Cleanup
Tests are designed to clean up after themselves, but manual cleanup may be needed:
```sql
-- Clean up test tables (if needed)
DROP TABLE IF EXISTS test_dml_employees;
DROP TABLE IF EXISTS test_ddl_table;
-- Add other test tables as needed
```

## Contributing

When adding new tests:

1. **Follow naming conventions:**
   - Test files: `test_*.py`
   - Test functions: `test_*`
   - Use descriptive names

2. **Add appropriate markers:**
   ```python
   @pytest.mark.integration
   def test_database_operation():
       pass
   ```

3. **Include error handling:**
   ```python
   try:
       # Test code
       pass
   except Exception as e:
       pytest.skip(f"Database not available: {e}")
   ```

4. **Clean up resources:**
   - Drop temporary tables
   - Close connections
   - Use fixtures for complex setup/teardown

5. **Document test purpose:**
   - Add docstrings to test functions
   - Include comments for complex test logic
   - Update this guide for new test categories
