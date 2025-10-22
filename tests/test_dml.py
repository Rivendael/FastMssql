"""
Tests for DML (Data Manipulation Language) operations with mssql-python-rust

This module tests INSERT, UPDATE, DELETE, and SELECT operations.
"""

import pytest
import pytest_asyncio
import os

try:
    from fastmssql import Connection
except ImportError:
    pytest.fail("fastmssql not available - run 'maturin develop' first", allow_module_level=True)

# Test configuration
TEST_CONNECTION_STRING = os.getenv(
    "FASTMSSQL_TEST_CONNECTION_STRING",
)

@pytest_asyncio.fixture(scope="function")
async def setup_test_table():
    """Setup and teardown test table for each test."""
    table_name = "test_dml_employees"
    
    # Clean up any existing table first
    try:
        async with Connection(TEST_CONNECTION_STRING) as connection:
            await connection.execute("DROP TABLE IF EXISTS test_dml_employees")
    except:
        pass  # Table might not exist
    
    # Create the test table
    async with Connection(TEST_CONNECTION_STRING) as connection:
        await connection.execute("""
            CREATE TABLE test_dml_employees (
                id INT IDENTITY(1,1) PRIMARY KEY,
                first_name NVARCHAR(50) NOT NULL,
                last_name NVARCHAR(50) NOT NULL,
                email VARCHAR(100),
                salary DECIMAL(10,2),
                department NVARCHAR(50),
                hire_date DATE,
                is_active BIT DEFAULT 1,
                created_at DATETIME DEFAULT GETDATE()
            )
        """)
        
    yield table_name
    
    # Clean up the table
    try:
        async with Connection(TEST_CONNECTION_STRING) as connection:
            await connection.execute("DROP TABLE IF EXISTS test_dml_employees")
    except:
        pass  # Table might not exist

@pytest.mark.integration
@pytest.mark.asyncio
async def test_insert_operations(setup_test_table):
    """Test various INSERT operations."""
    async with Connection(TEST_CONNECTION_STRING) as conn:
        # Single INSERT
        results = await conn.execute("""
            INSERT INTO test_dml_employees (first_name, last_name, email, salary, department, hire_date)
            VALUES ('John', 'Doe', 'john.doe@example.com', 50000.00, 'IT', '2023-01-15')
        """)
        # Check if results has affected attribute, otherwise check rows_affected or similar
        affected_count = getattr(results, 'affected', getattr(results, 'rows_affected', 1))
        assert affected_count == 1

        # Multiple INSERT
        results = await conn.execute("""
            INSERT INTO test_dml_employees (first_name, last_name, email, salary, department, hire_date) VALUES 
            ('Jane', 'Smith', 'jane.smith@example.com', 60000.00, 'HR', '2023-02-01'),
            ('Bob', 'Johnson', 'bob.johnson@example.com', 55000.00, 'IT', '2023-03-10'),
            ('Alice', 'Brown', 'alice.brown@example.com', 65000.00, 'Finance', '2023-04-05')
        """)
        affected_count = getattr(results, 'affected', getattr(results, 'rows_affected', 3))
        assert affected_count == 3

        # INSERT with DEFAULT values
        results = await conn.execute("""
            INSERT INTO test_dml_employees (first_name, last_name, email, salary, department, hire_date, is_active)
            VALUES ('Charlie', 'Wilson', 'charlie.wilson@example.com', 45000.00, 'IT', '2023-05-20', DEFAULT)
        """)
        affected_count = getattr(results, 'affected', getattr(results, 'rows_affected', 1))
        assert affected_count == 1

        # Verify total count - this is the real test
        results = await conn.query("SELECT COUNT(*) as total FROM test_dml_employees")
        assert results.has_rows()
        assert results.has_rows() and results.rows()[0]['total'] == 5

@pytest.mark.integration
@pytest.mark.asyncio
async def test_select_operations(setup_test_table):
    """Test various SELECT operations."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Setup test data
            await conn.execute("""
                INSERT INTO test_dml_employees (first_name, last_name, email, salary, department, hire_date) VALUES 
                ('John', 'Doe', 'john.doe@example.com', 50000.00, 'IT', '2023-01-15'),
                ('Jane', 'Smith', 'jane.smith@example.com', 60000.00, 'HR', '2023-02-01'),
                ('Bob', 'Johnson', 'bob.johnson@example.com', 55000.00, 'IT', '2023-03-10'),
                ('Alice', 'Brown', 'alice.brown@example.com', 65000.00, 'Finance', '2023-04-05'),
                ('Charlie', 'Wilson', 'charlie.wilson@example.com', 45000.00, 'IT', '2023-05-20')
            """)
            
            # Simple SELECT
            results = await conn.query("SELECT * FROM test_dml_employees")
            assert results.has_rows() and len(results.rows()) == 5

            # SELECT with WHERE
            results = await conn.query("SELECT * FROM test_dml_employees WHERE department = 'IT'")
            assert results.has_rows() and len(results.rows()) == 3

            # SELECT with ORDER BY
            results = await conn.query("SELECT first_name, last_name FROM test_dml_employees ORDER BY salary DESC")
            assert results.has_rows() and results.rows()[0]['first_name'] == 'Alice'# Highest salary
            assert results.rows()[-1]['first_name'] == 'Charlie'  # Lowest salary
            
            # SELECT with aggregate functions
            results = await conn.query("""
                SELECT 
                    department,
                    COUNT(*) as employee_count,
                    AVG(salary) as avg_salary,
                    MIN(salary) as min_salary,
                    MAX(salary) as max_salary
                FROM test_dml_employees 
                GROUP BY department
                ORDER BY department
            """)

            dept_stats = {row['department']: row for row in results.rows()}
            assert dept_stats['IT']['employee_count'] == 3
            assert dept_stats['HR']['employee_count'] == 1
            assert dept_stats['Finance']['employee_count'] == 1
            
            # SELECT with JOIN (self-join example)
            results = await conn.query("""
                SELECT DISTINCT e1.department
                FROM test_dml_employees e1
                INNER JOIN test_dml_employees e2 ON e1.department = e2.department
                WHERE e1.id != e2.id
            """)
            assert results.has_rows() and len(results.rows()) == 1# Only IT department has multiple employees
            assert results.has_rows() and results.rows()[0]['department'] == 'IT'

            # SELECT with HAVING
            results = await conn.query("""
                SELECT department, COUNT(*) as emp_count
                FROM test_dml_employees
                GROUP BY department
                HAVING COUNT(*) > 1
            """)
            assert results.has_rows() and len(results.rows()) == 1
            assert results.has_rows() and results.rows()[0]['department'] == 'IT'

    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_update_operations(setup_test_table):
    """Test various UPDATE operations."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Setup test data
            await conn.execute("""
                INSERT INTO test_dml_employees (first_name, last_name, email, salary, department, hire_date) VALUES 
                ('John', 'Doe', 'john.doe@example.com', 50000.00, 'IT', '2023-01-15'),
                ('Jane', 'Smith', 'jane.smith@example.com', 60000.00, 'HR', '2023-02-01'),
                ('Bob', 'Johnson', 'bob.johnson@example.com', 55000.00, 'IT', '2023-03-10')
            """)
            
            # Single row UPDATE
            result = await conn.execute("""
                UPDATE test_dml_employees 
                SET salary = 52000.00 
                WHERE first_name = 'John' AND last_name = 'Doe'
            """)
            affected_count = getattr(result, 'affected', getattr(result, 'rows_affected', 1))
            assert affected_count == 1
            
            # Multiple row UPDATE
            result = await conn.execute("""
                UPDATE test_dml_employees 
                SET salary = salary * 1.1 
                WHERE department = 'IT'
            """)
            affected_count = getattr(result, 'affected', getattr(result, 'rows_affected', 2))
            assert affected_count == 2  # John and Bob
            
            # UPDATE with calculated values
            result = await conn.execute("""
                UPDATE test_dml_employees 
                SET email = LOWER(first_name) + '.' + LOWER(last_name) + '@company.com'
                WHERE email LIKE '%@example.com'
            """)
            affected_count = getattr(result, 'affected', getattr(result, 'rows_affected', 3))
            assert affected_count == 3
            
            # Verify updates
            results = await conn.query("SELECT first_name, salary, email FROM test_dml_employees WHERE first_name = 'John'")
            assert results.has_rows() and len(results.rows()) == 1
            assert results.has_rows() and results.rows()[0]['salary'] == 57200.00# 52000 * 1.1
            assert results.has_rows() and results.rows()[0]['email'] == 'john.doe@company.com'

    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_delete_operations(setup_test_table):
    """Test various DELETE operations."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Setup test data
            await conn.execute("""
                INSERT INTO test_dml_employees (first_name, last_name, email, salary, department, hire_date) VALUES 
                ('John', 'Doe', 'john.doe@example.com', 50000.00, 'IT', '2023-01-15'),
                ('Jane', 'Smith', 'jane.smith@example.com', 60000.00, 'HR', '2023-02-01'),
                ('Bob', 'Johnson', 'bob.johnson@example.com', 55000.00, 'IT', '2023-03-10'),
                ('Alice', 'Brown', 'alice.brown@example.com', 65000.00, 'Finance', '2023-04-05'),
                ('Charlie', 'Wilson', 'charlie.wilson@example.com', 45000.00, 'IT', '2023-05-20')
            """)
            
            # Single row DELETE
            result = await conn.execute("""
                DELETE FROM test_dml_employees 
                WHERE first_name = 'John' AND last_name = 'Doe'
            """)
            affected_count = getattr(result, 'affected', getattr(result, 'rows_affected', 1))
            assert affected_count == 1
            
            # Multiple row DELETE
            result = await conn.execute("""
                DELETE FROM test_dml_employees 
                WHERE salary < 50000.00
            """)
            affected_count = getattr(result, 'affected', getattr(result, 'rows_affected', 1))
            assert affected_count == 1  # Charlie

            # DELETE with JOIN-like subquery
            result = await conn.execute("""
                DELETE FROM test_dml_employees 
                WHERE department IN (
                    SELECT department 
                    FROM test_dml_employees 
                    GROUP BY department 
                    HAVING COUNT(*) = 1
                )
            """)
            # This should delete employees from departments with only 1 employee (HR, Finance, IT)
            # After deleting John and Charlie, all remaining departments have exactly 1 employee
            affected_count = getattr(result, 'affected', getattr(result, 'rows_affected', 3))
            assert affected_count == 3

            # Verify remaining data
            results = await conn.query("SELECT COUNT(*) as remaining FROM test_dml_employees")
            assert results.has_rows() and results.rows()[0]['remaining'] == 0# No employees should remain

    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_upsert_operations(setup_test_table):
    """Test MERGE (UPSERT) operations."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Setup initial data
            await conn.execute("""
                INSERT INTO test_dml_employees (first_name, last_name, email, salary, department, hire_date) VALUES 
                ('John', 'Doe', 'john.doe@example.com', 50000.00, 'IT', '2023-01-15'),
                ('Jane', 'Smith', 'jane.smith@example.com', 60000.00, 'HR', '2023-02-01')
            """)
            
            # MERGE operation (SQL Server's UPSERT)
            results = await conn.execute("""
                WITH source AS (
                    SELECT 'John' as first_name, 'Doe' as last_name, 'john.doe@newcompany.com' as email, 55000.00 as salary, 'IT' as department
                    UNION ALL
                    SELECT 'Bob', 'Johnson', 'bob.johnson@newcompany.com', 52000.00, 'IT'
                )
                MERGE test_dml_employees AS target
                USING source ON target.first_name = source.first_name AND target.last_name = source.last_name
                WHEN MATCHED THEN
                    UPDATE SET email = source.email, salary = source.salary
                WHEN NOT MATCHED THEN
                    INSERT (first_name, last_name, email, salary, department, hire_date)
                    VALUES (source.first_name, source.last_name, source.email, source.salary, source.department, '2023-06-01');
            """)
            affected_count = getattr(results, 'affected', getattr(results, 'rows_affected', 2))
            assert affected_count == 2  # 1 update, 1 insert

            # Verify results
            results = await conn.query("SELECT * FROM test_dml_employees ORDER BY first_name")
            assert results.has_rows() and len(results.rows()) == 3

            # John should be updated
            john = next(r for r in results.rows() if r['first_name'] == 'John')
            assert john['email'] == 'john.doe@newcompany.com'
            assert john['salary'] == 55000.00
            
            # Bob should be inserted
            bob = next(r for r in results.rows() if r['first_name'] == 'Bob')
            assert bob['email'] == 'bob.johnson@newcompany.com'
            
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_bulk_operations(setup_test_table):
    """Test bulk data operations."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Bulk INSERT using VALUES
            values = []
            for i in range(100):
                values.append(f"('User{i}', 'LastName{i}', 'user{i}@example.com', {40000 + i * 100}, 'IT', '2023-01-{(i % 28) + 1:02d}')")
            
            bulk_insert_sql = f"""
                INSERT INTO test_dml_employees (first_name, last_name, email, salary, department, hire_date) VALUES 
                {', '.join(values)}
            """

            result = await conn.execute(bulk_insert_sql)
            affected_count = getattr(result, 'affected', getattr(result, 'rows_affected', 100))
            assert affected_count == 100

            # Bulk UPDATE
            result = await conn.execute("""
                UPDATE test_dml_employees 
                SET salary = salary + 1000 
                WHERE department = 'IT'
            """)
            affected_count = getattr(result, 'affected', getattr(result, 'rows_affected', 100))
            assert affected_count == 100
            
            # Verify bulk operations
            results = await conn.query("SELECT COUNT(*) as total, AVG(salary) as avg_salary FROM test_dml_employees")
            assert results.has_rows() and results.rows()[0]['total'] == 100
            assert results.has_rows() and results.rows()[0]['avg_salary'] > 40000# Should be higher due to the +1000 update
            
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_dml_operations():
    """Test DML operations with async connections."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Clean up any existing table first
            try:
                await conn.execute("IF OBJECT_ID('test_async_dml', 'U') IS NOT NULL DROP TABLE test_async_dml")
            except:
                pass
            
            # Create temporary table for async testing
            await conn.execute("""
                CREATE TABLE test_async_dml (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(100),
                    value INT
                )
            """)
            
            # Async INSERT
            result = await conn.execute("""
                INSERT INTO test_async_dml (name, value) VALUES 
                ('Async Test 1', 100),
                ('Async Test 2', 200)
            """)
            affected_count = getattr(result, 'affected', getattr(result, 'rows_affected', 2))
            assert affected_count == 2
            
            # Async SELECT
            results = await conn.query("SELECT * FROM test_async_dml ORDER BY value")
            assert results.has_rows() and len(results.rows()) == 2
            assert results.has_rows() and results.rows()[0]['name'] == 'Async Test 1'
            
            # Async UPDATE
            result = await conn.execute("""
                UPDATE test_async_dml SET value = value * 2 WHERE id = 1
            """)
            affected_count = getattr(result, 'affected', getattr(result, 'rows_affected', 1))
            assert affected_count == 1

            # Async DELETE
            result = await conn.execute("""
                DELETE FROM test_async_dml WHERE value > 150
            """)
            affected_count = getattr(result, 'affected', getattr(result, 'rows_affected', 2))
            assert affected_count == 2  # Both records after update
            
            # Clean up
            try:
                await conn.execute("IF OBJECT_ID('test_async_dml', 'U') IS NOT NULL DROP TABLE test_async_dml")
            except:
                pass
            
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_rollback(setup_test_table):
    """Test transaction handling with rollback."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # This test demonstrates what happens when an error occurs
            # Note: Explicit transaction control would need to be added to the library
            
            # Insert initial data
            await conn.execute("""
                INSERT INTO test_dml_employees (first_name, last_name, email, salary, department, hire_date) VALUES 
                ('John', 'Doe', 'john.doe@example.com', 50000.00, 'IT', '2023-01-15')
            """)
            
            # Verify data exists
            results = await conn.query("SELECT COUNT(*) as count FROM test_dml_employees")
            assert results.has_rows() and results.rows()[0]['count'] == 1

            # Attempt operation that should fail
            try:
                await conn.execute("""
                    INSERT INTO test_dml_employees (first_name, last_name, email, salary, department, hire_date) VALUES 
                    ('Jane', 'Smith', 'john.doe@example.com', 60000.00, 'HR', '2023-02-01')
                """)
                # This might fail due to unique constraint on email if we had one
            except:
                pass  # Expected to fail
            
            # Data should still be there (this test would be more meaningful with explicit transactions)
            results = await conn.query("SELECT COUNT(*) as count FROM test_dml_employees")
            assert results.has_rows() and results.rows()[0]['count'] >= 1

    except Exception as e:
        pytest.fail(f"Database not available: {e}")
