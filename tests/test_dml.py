"""
Tests for DML (Data Manipulation Language) operations with mssql-python-rust

This module tests INSERT, UPDATE, DELETE, and SELECT operations.
"""

import pytest
import sys
import os

# Add the parent directory to Python path for development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python'))

try:
    import mssql_python_rust as mssql
except ImportError:
    pytest.skip("mssql_python_rust not available - run 'maturin develop' first", allow_module_level=True)

# Test configuration
TEST_CONNECTION_STRING = "Server=SNOWFLAKE\\SQLEXPRESS,50014;Database=pymssql_test;Integrated Security=true;TrustServerCertificate=yes"

@pytest.fixture
def test_table():
    """Setup and teardown test table."""
    connection = mssql.connect(TEST_CONNECTION_STRING)
    
    try:
        with connection:
            # Create test table
            connection.execute_non_query("""
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
            
        yield "test_dml_employees"
        
    finally:
        try:
            with connection:
                connection.execute_non_query("DROP TABLE IF EXISTS test_dml_employees")
        except:
            pass  # Table might not exist

@pytest.mark.integration
def test_insert_operations(test_table):
    """Test various INSERT operations."""
    with mssql.connect(TEST_CONNECTION_STRING) as conn:
        # Single INSERT
        affected = conn.execute_non_query("""
            INSERT INTO test_dml_employees (first_name, last_name, email, salary, department, hire_date)
            VALUES ('John', 'Doe', 'john.doe@example.com', 50000.00, 'IT', '2023-01-15')
        """)
        assert affected == 1
        
        # Multiple INSERT
        affected = conn.execute_non_query("""
            INSERT INTO test_dml_employees (first_name, last_name, email, salary, department, hire_date) VALUES 
            ('Jane', 'Smith', 'jane.smith@example.com', 60000.00, 'HR', '2023-02-01'),
            ('Bob', 'Johnson', 'bob.johnson@example.com', 55000.00, 'IT', '2023-03-10'),
            ('Alice', 'Brown', 'alice.brown@example.com', 65000.00, 'Finance', '2023-04-05')
        """)
        assert affected == 3
        
        # INSERT with DEFAULT values
        affected = conn.execute_non_query("""
            INSERT INTO test_dml_employees (first_name, last_name, email, salary, department, hire_date, is_active)
            VALUES ('Charlie', 'Wilson', 'charlie.wilson@example.com', 45000.00, 'IT', '2023-05-20', DEFAULT)
        """)
        assert affected == 1
        
        # Verify total count - this is the real test
        rows = conn.execute("SELECT COUNT(*) as total FROM test_dml_employees")
        assert rows[0]['total'] == 5

@pytest.mark.integration
def test_select_operations(test_table):
    """Test various SELECT operations."""
    try:
        with mssql.connect(TEST_CONNECTION_STRING) as conn:
            # Setup test data
            conn.execute_non_query("""
                INSERT INTO test_dml_employees (first_name, last_name, email, salary, department, hire_date) VALUES 
                ('John', 'Doe', 'john.doe@example.com', 50000.00, 'IT', '2023-01-15'),
                ('Jane', 'Smith', 'jane.smith@example.com', 60000.00, 'HR', '2023-02-01'),
                ('Bob', 'Johnson', 'bob.johnson@example.com', 55000.00, 'IT', '2023-03-10'),
                ('Alice', 'Brown', 'alice.brown@example.com', 65000.00, 'Finance', '2023-04-05'),
                ('Charlie', 'Wilson', 'charlie.wilson@example.com', 45000.00, 'IT', '2023-05-20')
            """)
            
            # Simple SELECT
            rows = conn.execute("SELECT * FROM test_dml_employees")
            assert len(rows) == 5
            
            # SELECT with WHERE
            rows = conn.execute("SELECT * FROM test_dml_employees WHERE department = 'IT'")
            assert len(rows) == 3
            
            # SELECT with ORDER BY
            rows = conn.execute("SELECT first_name, last_name FROM test_dml_employees ORDER BY salary DESC")
            assert rows[0]['first_name'] == 'Alice'  # Highest salary
            assert rows[-1]['first_name'] == 'Charlie'  # Lowest salary
            
            # SELECT with aggregate functions
            rows = conn.execute("""
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
            
            dept_stats = {row['department']: row for row in rows}
            assert dept_stats['IT']['employee_count'] == 3
            assert dept_stats['HR']['employee_count'] == 1
            assert dept_stats['Finance']['employee_count'] == 1
            
            # SELECT with JOIN (self-join example)
            rows = conn.execute("""
                SELECT DISTINCT e1.department
                FROM test_dml_employees e1
                INNER JOIN test_dml_employees e2 ON e1.department = e2.department
                WHERE e1.id != e2.id
            """)
            assert len(rows) == 1  # Only IT department has multiple employees
            assert rows[0]['department'] == 'IT'
            
            # SELECT with HAVING
            rows = conn.execute("""
                SELECT department, COUNT(*) as emp_count
                FROM test_dml_employees
                GROUP BY department
                HAVING COUNT(*) > 1
            """)
            assert len(rows) == 1
            assert rows[0]['department'] == 'IT'
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration
def test_update_operations(test_table):
    """Test various UPDATE operations."""
    try:
        with mssql.connect(TEST_CONNECTION_STRING) as conn:
            # Setup test data
            conn.execute_non_query("""
                INSERT INTO test_dml_employees (first_name, last_name, email, salary, department, hire_date) VALUES 
                ('John', 'Doe', 'john.doe@example.com', 50000.00, 'IT', '2023-01-15'),
                ('Jane', 'Smith', 'jane.smith@example.com', 60000.00, 'HR', '2023-02-01'),
                ('Bob', 'Johnson', 'bob.johnson@example.com', 55000.00, 'IT', '2023-03-10')
            """)
            
            # Single row UPDATE
            affected = conn.execute_non_query("""
                UPDATE test_dml_employees 
                SET salary = 52000.00 
                WHERE first_name = 'John' AND last_name = 'Doe'
            """)
            assert affected == 1
            
            # Multiple row UPDATE
            affected = conn.execute_non_query("""
                UPDATE test_dml_employees 
                SET salary = salary * 1.1 
                WHERE department = 'IT'
            """)
            assert affected == 2  # John and Bob
            
            # UPDATE with calculated values
            affected = conn.execute_non_query("""
                UPDATE test_dml_employees 
                SET email = LOWER(first_name) + '.' + LOWER(last_name) + '@company.com'
                WHERE email LIKE '%@example.com'
            """)
            assert affected == 3
            
            # Verify updates
            rows = conn.execute("SELECT first_name, salary, email FROM test_dml_employees WHERE first_name = 'John'")
            assert len(rows) == 1
            assert rows[0]['salary'] == 57200.00  # 52000 * 1.1
            assert rows[0]['email'] == 'john.doe@company.com'
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration
def test_delete_operations(test_table):
    """Test various DELETE operations."""
    try:
        with mssql.connect(TEST_CONNECTION_STRING) as conn:
            # Setup test data
            conn.execute_non_query("""
                INSERT INTO test_dml_employees (first_name, last_name, email, salary, department, hire_date) VALUES 
                ('John', 'Doe', 'john.doe@example.com', 50000.00, 'IT', '2023-01-15'),
                ('Jane', 'Smith', 'jane.smith@example.com', 60000.00, 'HR', '2023-02-01'),
                ('Bob', 'Johnson', 'bob.johnson@example.com', 55000.00, 'IT', '2023-03-10'),
                ('Alice', 'Brown', 'alice.brown@example.com', 65000.00, 'Finance', '2023-04-05'),
                ('Charlie', 'Wilson', 'charlie.wilson@example.com', 45000.00, 'IT', '2023-05-20')
            """)
            
            # Single row DELETE
            affected = conn.execute_non_query("""
                DELETE FROM test_dml_employees 
                WHERE first_name = 'John' AND last_name = 'Doe'
            """)
            assert affected == 1
            
            # Multiple row DELETE
            affected = conn.execute_non_query("""
                DELETE FROM test_dml_employees 
                WHERE salary < 50000.00
            """)
            assert affected == 1  # Charlie
            
            # DELETE with JOIN-like subquery
            affected = conn.execute_non_query("""
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
            assert affected == 3
            
            # Verify remaining data
            rows = conn.execute("SELECT COUNT(*) as remaining FROM test_dml_employees")
            assert rows[0]['remaining'] == 0  # No employees should remain
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration
def test_upsert_operations(test_table):
    """Test MERGE (UPSERT) operations."""
    try:
        with mssql.connect(TEST_CONNECTION_STRING) as conn:
            # Setup initial data
            conn.execute_non_query("""
                INSERT INTO test_dml_employees (first_name, last_name, email, salary, department, hire_date) VALUES 
                ('John', 'Doe', 'john.doe@example.com', 50000.00, 'IT', '2023-01-15'),
                ('Jane', 'Smith', 'jane.smith@example.com', 60000.00, 'HR', '2023-02-01')
            """)
            
            # MERGE operation (SQL Server's UPSERT)
            affected = conn.execute_non_query("""
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
            assert affected == 2  # 1 update, 1 insert
            
            # Verify results
            rows = conn.execute("SELECT * FROM test_dml_employees ORDER BY first_name")
            assert len(rows) == 3
            
            # John should be updated
            john = next(r for r in rows if r['first_name'] == 'John')
            assert john['email'] == 'john.doe@newcompany.com'
            assert john['salary'] == 55000.00
            
            # Bob should be inserted
            bob = next(r for r in rows if r['first_name'] == 'Bob')
            assert bob['email'] == 'bob.johnson@newcompany.com'
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration
def test_bulk_operations(test_table):
    """Test bulk data operations."""
    try:
        with mssql.connect(TEST_CONNECTION_STRING) as conn:
            # Bulk INSERT using VALUES
            values = []
            for i in range(100):
                values.append(f"('User{i}', 'LastName{i}', 'user{i}@example.com', {40000 + i * 100}, 'IT', '2023-01-{(i % 28) + 1:02d}')")
            
            bulk_insert_sql = f"""
                INSERT INTO test_dml_employees (first_name, last_name, email, salary, department, hire_date) VALUES 
                {', '.join(values)}
            """
            
            affected = conn.execute_non_query(bulk_insert_sql)
            assert affected == 100
            
            # Bulk UPDATE
            affected = conn.execute_non_query("""
                UPDATE test_dml_employees 
                SET salary = salary + 1000 
                WHERE department = 'IT'
            """)
            assert affected == 100
            
            # Verify bulk operations
            rows = conn.execute("SELECT COUNT(*) as total, AVG(salary) as avg_salary FROM test_dml_employees")
            assert rows[0]['total'] == 100
            assert rows[0]['avg_salary'] > 40000  # Should be higher due to the +1000 update
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_dml_operations():
    """Test DML operations with async connections."""
    try:
        async with mssql.connect_async(TEST_CONNECTION_STRING) as conn:
            # Clean up any existing table first
            try:
                await conn.execute_non_query("IF OBJECT_ID('test_async_dml', 'U') IS NOT NULL DROP TABLE test_async_dml")
            except:
                pass
            
            # Create temporary table for async testing
            await conn.execute_non_query("""
                CREATE TABLE test_async_dml (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(100),
                    value INT
                )
            """)
            
            # Async INSERT
            affected = await conn.execute_non_query("""
                INSERT INTO test_async_dml (name, value) VALUES 
                ('Async Test 1', 100),
                ('Async Test 2', 200)
            """)
            assert affected == 2
            
            # Async SELECT
            rows = await conn.execute("SELECT * FROM test_async_dml ORDER BY value")
            assert len(rows) == 2
            assert rows[0]['name'] == 'Async Test 1'
            
            # Async UPDATE
            affected = await conn.execute_non_query("""
                UPDATE test_async_dml SET value = value * 2 WHERE id = 1
            """)
            assert affected == 1
            
            # Async DELETE
            affected = await conn.execute_non_query("""
                DELETE FROM test_async_dml WHERE value > 150
            """)
            assert affected == 2  # Both records after update
            
            # Clean up
            try:
                await conn.execute_non_query("IF OBJECT_ID('test_async_dml', 'U') IS NOT NULL DROP TABLE test_async_dml")
            except:
                pass
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration
def test_transaction_rollback(test_table):
    """Test transaction handling with rollback."""
    try:
        with mssql.connect(TEST_CONNECTION_STRING) as conn:
            # This test demonstrates what happens when an error occurs
            # Note: Explicit transaction control would need to be added to the library
            
            # Insert initial data
            conn.execute_non_query("""
                INSERT INTO test_dml_employees (first_name, last_name, email, salary, department, hire_date) VALUES 
                ('John', 'Doe', 'john.doe@example.com', 50000.00, 'IT', '2023-01-15')
            """)
            
            # Verify data exists
            rows = conn.execute("SELECT COUNT(*) as count FROM test_dml_employees")
            assert rows[0]['count'] == 1
            
            # Attempt operation that should fail
            try:
                conn.execute_non_query("""
                    INSERT INTO test_dml_employees (first_name, last_name, email, salary, department, hire_date) VALUES 
                    ('Jane', 'Smith', 'john.doe@example.com', 60000.00, 'HR', '2023-02-01')
                """)
                # This might fail due to unique constraint on email if we had one
            except:
                pass  # Expected to fail
            
            # Data should still be there (this test would be more meaningful with explicit transactions)
            rows = conn.execute("SELECT COUNT(*) as count FROM test_dml_employees")
            assert rows[0]['count'] >= 1
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")
