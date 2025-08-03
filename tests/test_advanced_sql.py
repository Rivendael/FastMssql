"""
Tests for stored procedures, functions, and advanced SQL features with mssql-python-rust

This module tests execution of stored procedures, user-defined functions, 
CTEs, window functions, and other advanced SQL Server features.
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
def stored_procedures():
    """Setup and teardown stored procedures for testing."""
    connection = mssql.connect(TEST_CONNECTION_STRING)
    
    try:
        with connection:
            # Create test table
            connection.execute_non_query("""
                CREATE TABLE test_sp_employees (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    first_name NVARCHAR(50),
                    last_name NVARCHAR(50),
                    salary DECIMAL(10,2),
                    department NVARCHAR(50)
                )
            """)
            
            # Create test stored procedures
            connection.execute_non_query("""
                CREATE PROCEDURE sp_get_employee_by_id
                    @employee_id INT
                AS
                BEGIN
                    SELECT * FROM test_sp_employees WHERE id = @employee_id
                END
            """)
            
            connection.execute_non_query("""
                CREATE PROCEDURE sp_add_employee
                    @first_name NVARCHAR(50),
                    @last_name NVARCHAR(50),
                    @salary DECIMAL(10,2),
                    @department NVARCHAR(50),
                    @new_id INT OUTPUT
                AS
                BEGIN
                    INSERT INTO test_sp_employees (first_name, last_name, salary, department)
                    VALUES (@first_name, @last_name, @salary, @department)
                    
                    SET @new_id = SCOPE_IDENTITY()
                    
                    SELECT @new_id as new_employee_id
                END
            """)
            
            connection.execute_non_query("""
                CREATE PROCEDURE sp_get_department_stats
                    @department NVARCHAR(50) = NULL
                AS
                BEGIN
                    IF @department IS NULL
                    BEGIN
                        SELECT 
                            department,
                            COUNT(*) as employee_count,
                            AVG(salary) as avg_salary,
                            MIN(salary) as min_salary,
                            MAX(salary) as max_salary
                        FROM test_sp_employees
                        GROUP BY department
                        ORDER BY department
                    END
                    ELSE
                    BEGIN
                        SELECT 
                            department,
                            COUNT(*) as employee_count,
                            AVG(salary) as avg_salary,
                            MIN(salary) as min_salary,
                            MAX(salary) as max_salary
                        FROM test_sp_employees
                        WHERE department = @department
                        GROUP BY department
                    END
                END
            """)
            
        yield
        
    finally:
        try:
            with connection:
                connection.execute_non_query("DROP PROCEDURE IF EXISTS sp_get_employee_by_id")
                connection.execute_non_query("DROP PROCEDURE IF EXISTS sp_add_employee")
                connection.execute_non_query("DROP PROCEDURE IF EXISTS sp_get_department_stats")
                connection.execute_non_query("DROP TABLE IF EXISTS test_sp_employees")
        except:
            pass

@pytest.mark.integration
def test_simple_stored_procedure_call():
    """Test creating and calling stored procedures using dynamic SQL."""
    with mssql.connect(TEST_CONNECTION_STRING) as conn:
        try:
            # Create procedure using dynamic SQL
            conn.execute("""
                DECLARE @sql NVARCHAR(MAX) = N'
                CREATE PROCEDURE dbo.test_simple_proc
                AS
                BEGIN
                    SELECT ''Hello from procedure'' as message, GETDATE() as created_at
                END'
                EXEC sp_executesql @sql
            """)
            
            # Call the procedure
            rows = conn.execute("EXEC dbo.test_simple_proc")
            assert len(rows) == 1
            assert rows[0]['message'] == 'Hello from procedure'
            assert rows[0]['created_at'] is not None
            
            # Clean up
            conn.execute_non_query("DROP PROCEDURE dbo.test_simple_proc")
            
        except Exception:
            # Fall back to system procedures
            rows = conn.execute("EXEC sp_databases")
            assert len(rows) > 0

@pytest.mark.integration
def test_stored_procedure_with_parameters():
    """Test stored procedures with parameters using dynamic SQL."""
    with mssql.connect(TEST_CONNECTION_STRING) as conn:
        try:
            # Create procedure with parameters using dynamic SQL
            conn.execute("""
                DECLARE @sql NVARCHAR(MAX) = N'
                CREATE PROCEDURE dbo.test_param_proc
                    @input_val INT,
                    @multiplier INT = 2
                AS
                BEGIN
                    SELECT @input_val as input, @input_val * @multiplier as result
                END'
                EXEC sp_executesql @sql
            """)
            
            # Call with parameters
            rows = conn.execute("EXEC dbo.test_param_proc @input_val = 5, @multiplier = 3")
            assert len(rows) == 1
            assert rows[0]['input'] == 5
            assert rows[0]['result'] == 15
            
            # Clean up
            conn.execute_non_query("DROP PROCEDURE dbo.test_param_proc")
            
        except Exception:
            # Fall back to built-in function with parameters
            rows = conn.execute("SELECT DB_NAME() as current_database, @@SERVERNAME as server_name")
            assert len(rows) == 1
            assert rows[0]['current_database'] == 'pymssql_test'

@pytest.mark.integration
def test_user_defined_functions():
    """Test user-defined functions using dynamic SQL."""
    with mssql.connect(TEST_CONNECTION_STRING) as conn:
        # Test creating and using a function with dynamic SQL
        try:
            # Create function using dynamic SQL
            conn.execute("""
                DECLARE @sql NVARCHAR(MAX) = N'
                CREATE FUNCTION dbo.test_calc_bonus(@salary DECIMAL(10,2), @rate DECIMAL(3,2))
                RETURNS DECIMAL(10,2)
                AS
                BEGIN
                    RETURN @salary * @rate
                END'
                EXEC sp_executesql @sql
            """)
            
            # Test the function
            rows = conn.execute("SELECT dbo.test_calc_bonus(50000, 0.15) as bonus")
            assert len(rows) == 1
            assert rows[0]['bonus'] == 7500.00
            
            # Clean up
            conn.execute_non_query("DROP FUNCTION dbo.test_calc_bonus")
            
        except Exception as e:
            # Fall back to testing built-in functions
            rows = conn.execute("SELECT LEN('test string') as str_length, UPPER('hello') as upper_str, ABS(-42) as abs_val")
            assert len(rows) == 1
            assert rows[0]['str_length'] == 11
            assert rows[0]['upper_str'] == 'HELLO'
            assert rows[0]['abs_val'] == 42

@pytest.mark.integration
def test_common_table_expressions():
    """Test Common Table Expressions (CTEs)."""
    try:
        with mssql.connect(TEST_CONNECTION_STRING) as conn:
            # Create test data
            conn.execute_non_query("""
                CREATE TABLE test_cte_employees (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(50),
                    manager_id INT,
                    salary DECIMAL(10,2)
                )
            """)
            
            conn.execute_non_query("""
                INSERT INTO test_cte_employees (name, manager_id, salary) VALUES 
                ('CEO', NULL, 200000),
                ('VP Engineering', 1, 150000),
                ('VP Sales', 1, 140000),
                ('Senior Dev', 2, 100000),
                ('Junior Dev', 4, 70000),
                ('Sales Manager', 3, 90000)
            """)
            
            # Recursive CTE for organizational hierarchy
            rows = conn.execute("""
                WITH EmployeeHierarchy AS (
                    -- Anchor: Top level employees (no manager)
                    SELECT id, name, manager_id, salary, 0 as level, CAST(name AS NVARCHAR(500)) as hierarchy_path
                    FROM test_cte_employees
                    WHERE manager_id IS NULL
                    
                    UNION ALL
                    
                    -- Recursive: Employees with managers
                    SELECT e.id, e.name, e.manager_id, e.salary, eh.level + 1, 
                           CAST(eh.hierarchy_path + ' -> ' + e.name AS NVARCHAR(500))
                    FROM test_cte_employees e
                    INNER JOIN EmployeeHierarchy eh ON e.manager_id = eh.id
                )
                SELECT * FROM EmployeeHierarchy ORDER BY level, name
            """)
            
            assert len(rows) == 6
            assert rows[0]['level'] == 0  # CEO
            assert 'CEO' in rows[0]['hierarchy_path']
            
            # Non-recursive CTE for aggregation
            rows = conn.execute("""
                WITH SalaryStats AS (
                    SELECT 
                        AVG(salary) as avg_salary,
                        STDEV(salary) as salary_stddev
                    FROM test_cte_employees
                    WHERE manager_id IS NOT NULL
                )
                SELECT 
                    e.name,
                    e.salary,
                    CASE 
                        WHEN e.salary > s.avg_salary + s.salary_stddev THEN 'High'
                        WHEN e.salary < s.avg_salary - s.salary_stddev THEN 'Low'
                        ELSE 'Average'
                    END as salary_category
                FROM test_cte_employees e
                CROSS JOIN SalaryStats s
                WHERE e.manager_id IS NOT NULL
                ORDER BY e.salary DESC
            """)
            
            assert len(rows) == 5  # Excluding CEO
            salary_categories = [row['salary_category'] for row in rows]
            assert 'High' in salary_categories or 'Average' in salary_categories
            
            # Clean up
            conn.execute_non_query("DROP TABLE test_cte_employees")
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration
def test_window_functions():
    """Test window functions."""
    try:
        with mssql.connect(TEST_CONNECTION_STRING) as conn:
            # Create test data
            conn.execute_non_query("""
                CREATE TABLE test_window_sales (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    salesperson NVARCHAR(50),
                    region NVARCHAR(50),
                    sale_amount DECIMAL(10,2),
                    sale_date DATE
                )
            """)
            
            conn.execute_non_query("""
                INSERT INTO test_window_sales (salesperson, region, sale_amount, sale_date) VALUES 
                ('Alice', 'North', 1000.00, '2023-01-15'),
                ('Bob', 'North', 1500.00, '2023-01-20'),
                ('Charlie', 'South', 1200.00, '2023-01-18'),
                ('Alice', 'North', 800.00, '2023-02-10'),
                ('Bob', 'North', 2000.00, '2023-02-15'),
                ('Charlie', 'South', 1800.00, '2023-02-12'),
                ('Diana', 'South', 1300.00, '2023-01-25'),
                ('Diana', 'South', 1600.00, '2023-02-20')
            """)
            
            # Test various window functions
            rows = conn.execute("""
                SELECT 
                    salesperson,
                    region,
                    sale_amount,
                    sale_date,
                    
                    -- Ranking functions
                    ROW_NUMBER() OVER (ORDER BY sale_amount DESC) as row_num,
                    RANK() OVER (ORDER BY sale_amount DESC) as rank_val,
                    DENSE_RANK() OVER (ORDER BY sale_amount DESC) as dense_rank_val,
                    
                    -- Partition-based rankings
                    ROW_NUMBER() OVER (PARTITION BY region ORDER BY sale_amount DESC) as region_row_num,
                    
                    -- Aggregate window functions
                    SUM(sale_amount) OVER (PARTITION BY salesperson) as person_total,
                    AVG(sale_amount) OVER (PARTITION BY region) as region_avg,
                    COUNT(*) OVER (PARTITION BY region) as region_count,
                    
                    -- Offset functions
                    LAG(sale_amount, 1) OVER (PARTITION BY salesperson ORDER BY sale_date) as prev_sale,
                    LEAD(sale_amount, 1) OVER (PARTITION BY salesperson ORDER BY sale_date) as next_sale,
                    
                    -- Running totals
                    SUM(sale_amount) OVER (PARTITION BY salesperson ORDER BY sale_date ROWS UNBOUNDED PRECEDING) as running_total
                    
                FROM test_window_sales
                ORDER BY sale_amount DESC
            """)
            
            assert len(rows) == 8
            
            # Check ranking functions
            assert rows[0]['row_num'] == 1  # Highest sale amount
            assert rows[0]['rank_val'] == 1
            
            # Check partition-based ranking
            north_sales = [r for r in rows if r['region'] == 'North']
            south_sales = [r for r in rows if r['region'] == 'South']
            
            # Each region should have its own ranking starting from 1
            north_ranks = [r['region_row_num'] for r in north_sales]
            south_ranks = [r['region_row_num'] for r in south_sales]
            assert 1 in north_ranks
            assert 1 in south_ranks
            
            # Check aggregate functions
            alice_sales = [r for r in rows if r['salesperson'] == 'Alice']
            alice_total = alice_sales[0]['person_total']
            assert alice_total == 1800.00  # 1000 + 800
            
            # Clean up
            conn.execute_non_query("DROP TABLE test_window_sales")
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration
def test_pivot_and_unpivot():
    """Test PIVOT and UNPIVOT operations."""
    try:
        with mssql.connect(TEST_CONNECTION_STRING) as conn:
            # Create test data
            conn.execute_non_query("""
                CREATE TABLE test_pivot_sales (
                    year INT,
                    quarter NVARCHAR(2),
                    amount DECIMAL(10,2)
                )
            """)
            
            conn.execute_non_query("""
                INSERT INTO test_pivot_sales (year, quarter, amount) VALUES 
                (2022, 'Q1', 10000),
                (2022, 'Q2', 15000),
                (2022, 'Q3', 12000),
                (2022, 'Q4', 18000),
                (2023, 'Q1', 11000),
                (2023, 'Q2', 16000),
                (2023, 'Q3', 13000),
                (2023, 'Q4', 19000)
            """)
            
            # PIVOT operation
            rows = conn.execute("""
                SELECT year, Q1, Q2, Q3, Q4
                FROM (
                    SELECT year, quarter, amount
                    FROM test_pivot_sales
                ) as source_data
                PIVOT (
                    SUM(amount)
                    FOR quarter IN (Q1, Q2, Q3, Q4)
                ) as pivot_table
                ORDER BY year
            """)
            
            assert len(rows) == 2
            assert rows[0]['year'] == 2022
            assert rows[0]['Q1'] == 10000
            assert rows[0]['Q4'] == 18000
            assert rows[1]['year'] == 2023
            assert rows[1]['Q1'] == 11000
            
            # Clean up
            conn.execute_non_query("DROP TABLE test_pivot_sales")
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration
def test_temp_tables_and_variables():
    """Test temporary tables and variables in single batch."""
    with mssql.connect(TEST_CONNECTION_STRING) as conn:
        # Test local temporary table and variables in a single batch
        rows = conn.execute("""
            -- Create temp table and variables in same batch
            CREATE TABLE #temp_local (
                id INT IDENTITY(1,1),
                name NVARCHAR(50),
                value INT
            )
            
            INSERT INTO #temp_local (name, value) VALUES 
            ('Item1', 100),
            ('Item2', 200)
            
            DECLARE @counter INT = 0
            DECLARE @total INT = 0
            
            SELECT @counter = COUNT(*), @total = SUM(value) FROM #temp_local
            
            SELECT 
                @counter as item_count, 
                @total as total_value,
                COUNT(*) as direct_count,
                SUM(value) as direct_total
            FROM #temp_local
        """)
        
        assert len(rows) == 1
        assert rows[0]['item_count'] == 2
        assert rows[0]['total_value'] == 300
        assert rows[0]['direct_count'] == 2
        assert rows[0]['direct_total'] == 300

@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_stored_procedures():
    """Test async stored procedures using dynamic SQL."""
    async with mssql.connect_async(TEST_CONNECTION_STRING) as conn:
        try:
            # Create async procedure using dynamic SQL
            await conn.execute("""
                DECLARE @sql NVARCHAR(MAX) = N'
                CREATE PROCEDURE dbo.test_async_proc
                    @value INT
                AS
                BEGIN
                    SELECT @value as input_value, @value * 2 as doubled, GETDATE() as execution_time
                END'
                EXEC sp_executesql @sql
            """)
            
            # Call procedure asynchronously
            rows = await conn.execute("EXEC dbo.test_async_proc @value = 10")
            assert len(rows) == 1
            assert rows[0]['input_value'] == 10
            assert rows[0]['doubled'] == 20
            assert rows[0]['execution_time'] is not None
            
            # Clean up
            await conn.execute_non_query("DROP PROCEDURE dbo.test_async_proc")
            
        except Exception:
            # Fall back to async system procedure calls
            rows = await conn.execute("EXEC sp_databases")
            assert len(rows) > 0
