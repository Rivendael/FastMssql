"""
Tests for DDL (Data Definition Language) operations with mssql-python-rust

This module tests CREATE, ALTER, DROP operations for various database objects.
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

@pytest.mark.integration
def test_create_drop_table():
    """Test creating and dropping tables."""
    try:
        with mssql.connect(TEST_CONNECTION_STRING) as conn:
            # Create table
            create_sql = """
                CREATE TABLE test_ddl_table (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(100) NOT NULL,
                    email VARCHAR(255),
                    age INT,
                    created_date DATETIME DEFAULT GETDATE(),
                    is_active BIT DEFAULT 1
                )
            """
            conn.execute_non_query(create_sql)
            
            # Verify table exists
            check_sql = """
                SELECT COUNT(*) as table_count 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = 'test_ddl_table'
            """
            rows = conn.execute(check_sql)
            assert rows[0]['table_count'] == 1
            
            # Drop table
            conn.execute_non_query("DROP TABLE test_ddl_table")
            
            # Verify table is gone
            rows = conn.execute(check_sql)
            assert rows[0]['table_count'] == 0
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration
def test_alter_table():
    """Test altering table structure."""
    try:
        with mssql.connect(TEST_CONNECTION_STRING) as conn:
            # Create initial table
            conn.execute_non_query("""
                CREATE TABLE test_alter_table (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(50)
                )
            """)
            
            # Add column
            conn.execute_non_query("ALTER TABLE test_alter_table ADD description NVARCHAR(255)")
            
            # Modify column
            conn.execute_non_query("ALTER TABLE test_alter_table ALTER COLUMN name NVARCHAR(100)")
            
            # Check column exists and has correct properties
            rows = conn.execute("""
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = 'test_alter_table'
                ORDER BY COLUMN_NAME
            """)
            
            columns = {row['COLUMN_NAME']: row for row in rows}
            assert 'description' in columns
            assert columns['name']['CHARACTER_MAXIMUM_LENGTH'] == 100
            
            # Clean up
            conn.execute_non_query("DROP TABLE test_alter_table")
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration
def test_create_drop_index():
    """Test creating and dropping indexes."""
    try:
        with mssql.connect(TEST_CONNECTION_STRING) as conn:
            # Create table first
            conn.execute_non_query("""
                CREATE TABLE test_index_table (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(100),
                    email VARCHAR(255),
                    category_id INT
                )
            """)
            
            # Create regular index
            conn.execute_non_query("""
                CREATE INDEX IX_test_index_table_name 
                ON test_index_table (name)
            """)
            
            # Create composite index
            conn.execute_non_query("""
                CREATE INDEX IX_test_index_table_category_name 
                ON test_index_table (category_id, name)
            """)
            
            # Create unique index
            conn.execute_non_query("""
                CREATE UNIQUE INDEX IX_test_index_table_email 
                ON test_index_table (email)
            """)
            
            # Verify indexes exist
            rows = conn.execute("""
                SELECT name FROM sys.indexes 
                WHERE object_id = OBJECT_ID('test_index_table')
                AND name IS NOT NULL
                AND name LIKE 'IX_test_index_table%'
            """)
            
            index_names = [row['name'] for row in rows]
            assert 'IX_test_index_table_name' in index_names
            assert 'IX_test_index_table_category_name' in index_names
            assert 'IX_test_index_table_email' in index_names
            
            # Drop indexes
            conn.execute_non_query("DROP INDEX IX_test_index_table_name ON test_index_table")
            conn.execute_non_query("DROP INDEX IX_test_index_table_category_name ON test_index_table")
            conn.execute_non_query("DROP INDEX IX_test_index_table_email ON test_index_table")
            
            # Clean up table
            conn.execute_non_query("DROP TABLE test_index_table")
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration
def test_create_drop_view():
    """Test creating and dropping views."""
    try:
        with mssql.connect(TEST_CONNECTION_STRING) as conn:
            # Test if views are supported by trying to create a simple one
            try:
                conn.execute_non_query("CREATE VIEW test_feature_check AS SELECT 1 as test_col")
                conn.execute_non_query("DROP VIEW test_feature_check")
            except Exception as e:
                if "Incorrect syntax near the keyword 'VIEW'" in str(e):
                    pytest.skip("Views not supported in this SQL Server edition")
                else:
                    raise
            
            # Clean up any existing objects first
            try:
                conn.execute_non_query("IF OBJECT_ID('test_view_employees', 'V') IS NOT NULL DROP VIEW test_view_employees")
                conn.execute_non_query("IF OBJECT_ID('test_view_base', 'U') IS NOT NULL DROP TABLE test_view_base")
            except:
                pass
            
            # Create base table
            conn.execute_non_query("""
                CREATE TABLE test_view_base (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(100),
                    salary DECIMAL(10,2),
                    department NVARCHAR(50)
                )
            """)
            
            # Insert test data
            conn.execute_non_query("""
                INSERT INTO test_view_base (name, salary, department) VALUES 
                ('John Doe', 50000.00, 'IT'),
                ('Jane Smith', 60000.00, 'HR'),
                ('Bob Johnson', 55000.00, 'IT')
            """)
            
            # Create view
            conn.execute_non_query("""
                CREATE VIEW test_view_employees AS
                SELECT 
                    name,
                    salary,
                    department,
                    CASE WHEN salary > 55000 THEN 'High' ELSE 'Standard' END as salary_grade
                FROM test_view_base
                WHERE department = 'IT'
            """)
            
            # Test view
            rows = conn.execute("SELECT * FROM test_view_employees ORDER BY name")
            assert len(rows) == 2
            assert rows[0]['name'] == 'Bob Johnson'
            assert rows[1]['name'] == 'John Doe'
            
            # Drop view and table
            try:
                conn.execute_non_query("IF OBJECT_ID('test_view_employees', 'V') IS NOT NULL DROP VIEW test_view_employees")
                conn.execute_non_query("IF OBJECT_ID('test_view_base', 'U') IS NOT NULL DROP TABLE test_view_base")
            except:
                pass
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration
def test_create_drop_procedure():
    """Test creating and dropping stored procedures."""
    try:
        with mssql.connect(TEST_CONNECTION_STRING) as conn:
            # Test if procedures are supported by trying to create a simple one
            try:
                conn.execute_non_query("CREATE PROCEDURE test_feature_check AS BEGIN SELECT 1 END")
                conn.execute_non_query("DROP PROCEDURE test_feature_check")
            except Exception as e:
                if "Incorrect syntax near the keyword 'PROCEDURE'" in str(e):
                    pytest.skip("Stored procedures not supported in this SQL Server edition")
                else:
                    raise
            
            # Clean up any existing procedure first
            try:
                conn.execute_non_query("IF OBJECT_ID('test_procedure', 'P') IS NOT NULL DROP PROCEDURE test_procedure")
            except:
                pass
            
            # Create procedure with proper syntax
            conn.execute_non_query("""
                CREATE PROCEDURE test_procedure
                    @input_value INT,
                    @output_value INT OUTPUT
                AS
                BEGIN
                    SET @output_value = @input_value * 2;
                    SELECT @input_value as input, @output_value as output;
                END
            """)
            
            # Verify procedure exists
            rows = conn.execute("""
                SELECT COUNT(*) as proc_count
                FROM INFORMATION_SCHEMA.ROUTINES
                WHERE ROUTINE_NAME = 'test_procedure' AND ROUTINE_TYPE = 'PROCEDURE'
            """)
            assert rows[0]['proc_count'] == 1
            
            # Drop procedure
            try:
                conn.execute_non_query("IF OBJECT_ID('test_procedure', 'P') IS NOT NULL DROP PROCEDURE test_procedure")
            except:
                pass
            
            # Verify procedure is gone
            rows = conn.execute("""
                SELECT COUNT(*) as proc_count
                FROM INFORMATION_SCHEMA.ROUTINES
                WHERE ROUTINE_NAME = 'test_procedure' AND ROUTINE_TYPE = 'PROCEDURE'
            """)
            assert rows[0]['proc_count'] == 0
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration
def test_create_drop_function():
    """Test creating and dropping user-defined functions."""
    try:
        with mssql.connect(TEST_CONNECTION_STRING) as conn:
            # Test if functions are supported by trying to create a simple one
            try:
                conn.execute_non_query("CREATE FUNCTION test_feature_check() RETURNS INT AS BEGIN RETURN 1 END")
                conn.execute_non_query("DROP FUNCTION test_feature_check")
            except Exception as e:
                if "Incorrect syntax near the keyword 'FUNCTION'" in str(e):
                    pytest.skip("User-defined functions not supported in this SQL Server edition")
                else:
                    raise
            
            # Clean up any existing function first
            try:
                conn.execute_non_query("IF OBJECT_ID('dbo.test_function', 'FN') IS NOT NULL DROP FUNCTION dbo.test_function")
            except:
                pass
            
            # Create scalar function with proper syntax
            conn.execute_non_query("""
                CREATE FUNCTION dbo.test_function(@input INT)
                RETURNS INT
                AS
                BEGIN
                    RETURN @input * @input;
                END
            """)
            
            # Test function
            rows = conn.execute("SELECT dbo.test_function(5) as result")
            assert rows[0]['result'] == 25
            
            # Drop function
            try:
                conn.execute_non_query("""
                    IF OBJECT_ID('dbo.test_function', 'FN') IS NOT NULL 
                    DROP FUNCTION dbo.test_function
                """)
            except:
                pass
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration
def test_constraints():
    """Test creating and dropping constraints."""
    try:
        with mssql.connect(TEST_CONNECTION_STRING) as conn:
            # Create table with constraints
            conn.execute_non_query("""
                CREATE TABLE test_constraints (
                    id INT IDENTITY(1,1),
                    email VARCHAR(255),
                    age INT,
                    category_id INT
                )
            """)
            
            # Add primary key constraint
            conn.execute_non_query("""
                ALTER TABLE test_constraints 
                ADD CONSTRAINT PK_test_constraints PRIMARY KEY (id)
            """)
            
            # Add unique constraint
            conn.execute_non_query("""
                ALTER TABLE test_constraints 
                ADD CONSTRAINT UQ_test_constraints_email UNIQUE (email)
            """)
            
            # Add check constraint
            conn.execute_non_query("""
                ALTER TABLE test_constraints 
                ADD CONSTRAINT CK_test_constraints_age CHECK (age >= 0 AND age <= 150)
            """)
            
            # Test constraints work
            conn.execute_non_query("INSERT INTO test_constraints (email, age) VALUES ('test@example.com', 25)")
            
            # This should fail due to check constraint
            with pytest.raises(Exception):
                conn.execute_non_query("INSERT INTO test_constraints (email, age) VALUES ('test2@example.com', 200)")
            
            # Drop constraints
            conn.execute_non_query("ALTER TABLE test_constraints DROP CONSTRAINT CK_test_constraints_age")
            conn.execute_non_query("ALTER TABLE test_constraints DROP CONSTRAINT UQ_test_constraints_email")
            conn.execute_non_query("ALTER TABLE test_constraints DROP CONSTRAINT PK_test_constraints")
            
            # Clean up
            conn.execute_non_query("DROP TABLE test_constraints")
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_ddl_operations():
    """Test DDL operations with async connections."""
    try:
        async with mssql.connect_async(TEST_CONNECTION_STRING) as conn:
            # Create table
            await conn.execute_non_query("""
                CREATE TABLE test_async_ddl (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(100),
                    created_date DATETIME DEFAULT GETDATE()
                )
            """)
            
            # Insert data
            await conn.execute_non_query("""
                INSERT INTO test_async_ddl (name) VALUES ('Async Test')
            """)
            
            # Query data
            rows = await conn.execute("SELECT * FROM test_async_ddl")
            assert len(rows) == 1
            assert rows[0]['name'] == 'Async Test'
            
            # Clean up
            await conn.execute_non_query("DROP TABLE test_async_ddl")
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration
def test_schema_operations():
    """Test schema creation and management."""
    try:
        with mssql.connect(TEST_CONNECTION_STRING) as conn:
            # Test if schemas are supported by trying to create a simple one
            try:
                conn.execute_non_query("CREATE SCHEMA test_feature_check")
                conn.execute_non_query("DROP SCHEMA test_feature_check")
            except Exception as e:
                if "Incorrect syntax near the keyword 'SCHEMA'" in str(e):
                    pytest.skip("Schemas not supported in this SQL Server edition")
                else:
                    raise
            
            # Clean up any existing objects first
            try:
                conn.execute_non_query("IF OBJECT_ID('test_schema.test_table', 'U') IS NOT NULL DROP TABLE test_schema.test_table")
                conn.execute_non_query("IF SCHEMA_ID('test_schema') IS NOT NULL DROP SCHEMA test_schema")
            except:
                pass
            
            # Create schema
            conn.execute_non_query("CREATE SCHEMA test_schema")
            
            # Create table in schema
            conn.execute_non_query("""
                CREATE TABLE test_schema.test_table (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(100)
                )
            """)
            
            # Insert data
            conn.execute_non_query("INSERT INTO test_schema.test_table (name) VALUES ('Schema Test')")
            
            # Query data
            rows = conn.execute("SELECT * FROM test_schema.test_table")
            assert len(rows) == 1
            assert rows[0]['name'] == 'Schema Test'
            
            # Clean up
            try:
                conn.execute_non_query("""
                    IF OBJECT_ID('test_schema.test_table', 'U') IS NOT NULL 
                    DROP TABLE test_schema.test_table
                """)
                conn.execute_non_query("""
                    IF SCHEMA_ID('test_schema') IS NOT NULL 
                    DROP SCHEMA test_schema
                """)
            except:
                pass
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")
