"""
Tests for DDL (Data Definition Language) operations with mssql-python-rust

This module tests CREATE, ALTER, DROP operations for various database objects.
"""

import pytest
import os

try:
    from fastmssql import Connection
except ImportError:
    pytest.fail("fastmssql not available - run 'maturin develop' first", allow_module_level=True)

# Test configuration
TEST_CONNECTION_STRING = os.getenv(
    "FASTMSSQL_TEST_CONNECTION_STRING",
)
@pytest.mark.integration
@pytest.mark.asyncio
async def test_create_drop_table():
    """Test creating and dropping tables."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            await conn.execute("DROP TABLE IF EXISTS test_ddl_table")
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
            await conn.execute(create_sql)
            
            # Verify table exists
            check_sql = """
                SELECT COUNT(*) as table_count 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = 'test_ddl_table'
            """
            result = await conn.query(check_sql)
            assert result.has_rows()
            rows = result.rows() if result.has_rows() else []
            assert rows[0]['table_count'] == 1
            
            # Drop table
            await conn.execute("DROP TABLE test_ddl_table")

            # Verify table is gone
            result = await conn.query(check_sql)
            assert result.has_rows()
            rows = result.rows() if result.has_rows() else []
            assert rows[0]['table_count'] == 0
            
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_alter_table():
    """Test altering table structure."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            await conn.execute("DROP TABLE IF EXISTS test_alter_table")
            # Create initial table
            await conn.execute("""
                CREATE TABLE test_alter_table (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(50)
                )
            """)
            
            # Add column
            await conn.execute("ALTER TABLE test_alter_table ADD description NVARCHAR(255)")

            # Modify column
            await conn.execute("ALTER TABLE test_alter_table ALTER COLUMN name NVARCHAR(100)")

            # Check column exists and has correct properties
            result = await conn.query("""
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = 'test_alter_table'
                ORDER BY COLUMN_NAME
            """)
            assert result.has_rows()
            rows = result.rows() if result.has_rows() else []
            columns = {row.get('COLUMN_NAME'): row for row in rows}
            assert 'description' in columns
            assert columns['name']['CHARACTER_MAXIMUM_LENGTH'] == 100
            
            # Clean up
            await conn.execute("DROP TABLE test_alter_table")
            
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_create_drop_index():
    """Test creating and dropping indexes."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Clean up any existing table first
            try:
                await conn.execute("DROP TABLE IF EXISTS test_index_table")
            except:
                pass
                
            # Create table first
            await conn.execute("""
                CREATE TABLE test_index_table (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(100),
                    email VARCHAR(255),
                    category_id INT
                )
            """)
            
            # Create regular index
            await conn.execute("""
                CREATE INDEX IX_test_index_table_name 
                ON test_index_table (name)
            """)
            
            # Create composite index
            await conn.execute("""
                CREATE INDEX IX_test_index_table_category_name 
                ON test_index_table (category_id, name)
            """)
            
            # Create unique index
            await conn.execute("""
                CREATE UNIQUE INDEX IX_test_index_table_email 
                ON test_index_table (email)
            """)
            
            # Verify indexes exist
            result = await conn.query("""
                SELECT name FROM sys.indexes 
                WHERE object_id = OBJECT_ID('test_index_table')
                AND name IS NOT NULL
                AND name LIKE 'IX_test_index_table%'
            """)
            
            index_names = [row['name'] for row in result.rows()]
            assert 'IX_test_index_table_name' in index_names
            assert 'IX_test_index_table_category_name' in index_names
            assert 'IX_test_index_table_email' in index_names
            
            # Drop indexes
            await conn.execute("DROP INDEX IX_test_index_table_name ON test_index_table")
            await conn.execute("DROP INDEX IX_test_index_table_category_name ON test_index_table")
            await conn.execute("DROP INDEX IX_test_index_table_email ON test_index_table")

            # Clean up table
            await conn.execute("DROP TABLE test_index_table")

    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_create_drop_view():
    """Test creating and dropping views."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Test if views are supported by trying to create a simple one
            try:
                await conn.execute("CREATE VIEW test_feature_check AS SELECT 1 as test_col")
                await conn.execute("DROP VIEW test_feature_check")
            except Exception as e:
                if "Incorrect syntax near the keyword 'VIEW'" in str(e):
                    pytest.skip("Views not supported in this SQL Server edition")
                else:
                    raise
            
            # Clean up any existing objects first
            try:
                await conn.execute("IF OBJECT_ID('test_view_employees', 'V') IS NOT NULL DROP VIEW test_view_employees")
                await conn.execute("IF OBJECT_ID('test_view_base', 'U') IS NOT NULL DROP TABLE test_view_base")
            except:
                pass
            
            # Create base table
            await conn.execute("""
                CREATE TABLE test_view_base (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(100),
                    salary DECIMAL(10,2),
                    department NVARCHAR(50)
                )
            """)
            
            # Insert test data
            await conn.execute("""
                INSERT INTO test_view_base (name, salary, department) VALUES 
                ('John Doe', 50000.00, 'IT'),
                ('Jane Smith', 60000.00, 'HR'),
                ('Bob Johnson', 55000.00, 'IT')
            """)
            
            # Create view
            await conn.execute("""
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
            result = await conn.query("SELECT * FROM test_view_employees ORDER BY name")
            assert result.has_rows() and len(result.rows()) == 2
            assert result.rows()[0]['name'] == 'Bob Johnson'
            assert result.rows()[1]['name'] == 'John Doe'

            # Drop view and table
            try:
                await conn.execute("IF OBJECT_ID('test_view_employees', 'V') IS NOT NULL DROP VIEW test_view_employees")
                await conn.execute("IF OBJECT_ID('test_view_base', 'U') IS NOT NULL DROP TABLE test_view_base")
            except:
                pass
            
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_create_drop_procedure():
    """Test creating and dropping stored procedures."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Test if procedures are supported by trying to create a simple one
            try:
                await conn.execute("CREATE PROCEDURE test_feature_check AS BEGIN SELECT 1 END")
                await conn.execute("DROP PROCEDURE test_feature_check")
            except Exception as e:
                if "Incorrect syntax near the keyword 'PROCEDURE'" in str(e):
                    pytest.skip("Stored procedures not supported in this SQL Server edition")
                else:
                    raise
            
            # Clean up any existing procedure first
            try:
                await conn.execute("IF OBJECT_ID('test_procedure', 'P') IS NOT NULL DROP PROCEDURE test_procedure")
            except:
                pass
            
            # Create procedure with proper syntax
            await conn.execute("""
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
            result = await conn.query("""
                SELECT COUNT(*) as proc_count
                FROM INFORMATION_SCHEMA.ROUTINES
                WHERE ROUTINE_NAME = 'test_procedure' AND ROUTINE_TYPE = 'PROCEDURE'
            """)
            assert result.rows()[0]['proc_count'] == 1
            
            # Drop procedure
            try:
                await conn.execute("IF OBJECT_ID('test_procedure', 'P') IS NOT NULL DROP PROCEDURE test_procedure")
            except:
                pass
            
            # Verify procedure is gone
            result = await conn.query("""
                SELECT COUNT(*) as proc_count
                FROM INFORMATION_SCHEMA.ROUTINES
                WHERE ROUTINE_NAME = 'test_procedure' AND ROUTINE_TYPE = 'PROCEDURE'
            """)
            assert result.rows()[0]['proc_count'] == 0
            
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_create_drop_function():
    """Test creating and dropping user-defined functions."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Test if functions are supported by trying to create a simple one
            try:
                await conn.execute("CREATE FUNCTION test_feature_check() RETURNS INT AS BEGIN RETURN 1 END")
                await conn.execute("DROP FUNCTION test_feature_check")
            except Exception as e:
                if "Incorrect syntax near the keyword 'FUNCTION'" in str(e):
                    pytest.skip("User-defined functions not supported in this SQL Server edition")
                else:
                    raise
            
            # Clean up any existing function first
            try:
                await conn.execute("IF OBJECT_ID('dbo.test_function', 'FN') IS NOT NULL DROP FUNCTION dbo.test_function")
            except:
                pass
            
            # Create scalar function with proper syntax
            await conn.execute("""
                CREATE FUNCTION dbo.test_function(@input INT)
                RETURNS INT
                AS
                BEGIN
                    RETURN @input * @input;
                END
            """)
            
            # Test function
            result = await conn.query("SELECT dbo.test_function(5) as result")
            assert result.rows()[0]['result'] == 25

            # Drop function
            try:
                await conn.execute("""
                    IF OBJECT_ID('dbo.test_function', 'FN') IS NOT NULL 
                    DROP FUNCTION dbo.test_function
                """)
            except:
                pass
            
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_constraints():
    """Test creating and dropping constraints."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Create table with constraints
            await conn.execute("""
                CREATE TABLE test_constraints (
                    id INT IDENTITY(1,1),
                    email VARCHAR(255),
                    age INT,
                    category_id INT
                )
            """)
            
            # Add primary key constraint
            await conn.execute("""
                ALTER TABLE test_constraints 
                ADD CONSTRAINT PK_test_constraints PRIMARY KEY (id)
            """)
            
            # Add unique constraint
            await conn.execute("""
                ALTER TABLE test_constraints 
                ADD CONSTRAINT UQ_test_constraints_email UNIQUE (email)
            """)
            
            # Add check constraint
            await conn.execute("""
                ALTER TABLE test_constraints 
                ADD CONSTRAINT CK_test_constraints_age CHECK (age >= 0 AND age <= 150)
            """)
            
            # Test constraints work
            await conn.execute("INSERT INTO test_constraints (email, age) VALUES ('test@example.com', 25)")

            # This should fail due to check constraint
            with pytest.raises(Exception):
                await conn.execute("INSERT INTO test_constraints (email, age) VALUES ('test2@example.com', 200)")

            # Drop constraints
            await conn.execute("ALTER TABLE test_constraints DROP CONSTRAINT CK_test_constraints_age")
            await conn.execute("ALTER TABLE test_constraints DROP CONSTRAINT UQ_test_constraints_email")
            await conn.execute("ALTER TABLE test_constraints DROP CONSTRAINT PK_test_constraints")

            # Clean up
            await conn.execute("DROP TABLE test_constraints")

    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_ddl_operations():
    """Test DDL operations with async connections."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            await conn.execute("DROP TABLE IF EXISTS test_async_ddl")
            await conn.execute("""
                CREATE TABLE test_async_ddl (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(100),
                    created_date DATETIME DEFAULT GETDATE()
                )
            """)
            
            # Insert data
            await conn.execute("""
                INSERT INTO test_async_ddl (name) VALUES ('Async Test')
            """)
            
            # Query data
            results = await conn.query("SELECT * FROM test_async_ddl")
            assert results.has_rows() and len(results.rows()) == 1
            assert results.has_rows() and results.rows()[0]['name'] == 'Async Test'

            # Clean up
            await conn.execute("DROP TABLE test_async_ddl")
            
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_schema_operations():
    """Test schema creation and management."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Test if schemas are supported by trying to create a simple one
            try:
                await conn.execute("CREATE SCHEMA test_feature_check")
                await conn.execute("DROP SCHEMA test_feature_check")
            except Exception as e:
                if "Incorrect syntax near the keyword 'SCHEMA'" in str(e):
                    pytest.skip("Schemas not supported in this SQL Server edition")
                else:
                    raise
            
            # Clean up any existing objects first
            try:
                await conn.execute("IF OBJECT_ID('test_schema.test_table', 'U') IS NOT NULL DROP TABLE test_schema.test_table")
                await conn.execute("IF SCHEMA_ID('test_schema') IS NOT NULL DROP SCHEMA test_schema")
            except:
                pass
            
            # Create schema
            await conn.execute("CREATE SCHEMA test_schema")
            
            # Create table in schema
            await conn.execute("""
                CREATE TABLE test_schema.test_table (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(100)
                )
            """)
            
            # Insert data
            await conn.execute("INSERT INTO test_schema.test_table (name) VALUES ('Schema Test')")

            # Query data
            results = await conn.query("SELECT * FROM test_schema.test_table")
            assert results.has_rows() and len(results.rows()) == 1
            assert results.has_rows() and results.rows()[0]['name'] == 'Schema Test'

            # Clean up
            try:
                await conn.execute("""
                    IF OBJECT_ID('test_schema.test_table', 'U') IS NOT NULL 
                    DROP TABLE test_schema.test_table
                """)
                await conn.execute("""
                    IF SCHEMA_ID('test_schema') IS NOT NULL 
                    DROP SCHEMA test_schema
                """)
            except:
                pass
            
    except Exception as e:
        pytest.fail(f"Database not available: {e}")
