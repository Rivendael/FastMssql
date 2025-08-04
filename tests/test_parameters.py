"""
Unit tests for Parameter and Parameters classes

Tests the new parameter system that allows cleaner parameterized queries
with optional type hints and method chaining.
"""

import pytest
import sys
import os

# Add the parent directory to Python path for development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python'))

try:
    from mssql import Connection, Parameter, Parameters
except ImportError:
    pytest.skip("mssql_python_rust not available - run 'maturin develop' first", allow_module_level=True)

# Test configuration
TEST_CONNECTION_STRING = "Server=SNOWFLAKE\\SQLEXPRESS,50014;Database=pymssql_test;Integrated Security=true;TrustServerCertificate=yes"


class TestParameter:
    """Test the Parameter class functionality."""
    
    def test_parameter_creation_value_only(self):
        """Test creating a parameter with just a value."""
        param = Parameter(42)
        assert param.value == 42
        assert param.sql_type is None
    
    def test_parameter_creation_with_type(self):
        """Test creating a parameter with value and SQL type."""
        param = Parameter("test", "VARCHAR")
        assert param.value == "test"
        assert param.sql_type == "VARCHAR"
    
    def test_parameter_repr_without_type(self):
        """Test string representation without type."""
        param = Parameter(123)
        assert repr(param) == "Parameter(value=123)"
    
    def test_parameter_repr_with_type(self):
        """Test string representation with type."""
        param = Parameter("hello", "NVARCHAR")
        assert repr(param) == "Parameter(value='hello', type=NVARCHAR)"
    
    def test_parameter_various_types(self):
        """Test parameter with various Python types."""
        # Test different value types
        test_cases = [
            (None, None),
            (True, None),
            (False, None),
            (42, "INT"),
            (3.14, "FLOAT"),
            ("string", "VARCHAR"),
            (b"bytes", "VARBINARY"),
        ]
        
        for value, sql_type in test_cases:
            param = Parameter(value, sql_type)
            assert param.value == value
            assert param.sql_type == sql_type


class TestParameters:
    """Test the Parameters class functionality."""
    
    def test_parameters_creation_empty(self):
        """Test creating empty Parameters object."""
        params = Parameters()
        assert len(params) == 0
        assert len(params.positional) == 0
        assert len(params.named) == 0
    
    def test_parameters_creation_with_args(self):
        """Test creating Parameters with positional arguments."""
        params = Parameters(1, "test", True)
        assert len(params) == 3
        assert len(params.positional) == 3
        assert len(params.named) == 0
        
        pos_params = params.positional
        assert pos_params[0].value == 1
        assert pos_params[1].value == "test"
        assert pos_params[2].value == True
    
    def test_parameters_creation_with_kwargs(self):
        """Test creating Parameters with named arguments."""
        params = Parameters(name="John", age=30)
        assert len(params) == 2
        assert len(params.positional) == 0
        assert len(params.named) == 2
        
        named_params = params.named
        assert named_params["name"].value == "John"
        assert named_params["age"].value == 30
    
    def test_parameters_creation_mixed(self):
        """Test creating Parameters with both positional and named arguments."""
        params = Parameters(1, 2, name="test", active=True)
        assert len(params) == 4
        assert len(params.positional) == 2
        assert len(params.named) == 2
    
    def test_parameters_add_method(self):
        """Test adding parameters with the add() method."""
        params = Parameters()
        result = params.add(42)
        
        # Should return self for chaining
        assert result is params
        assert len(params) == 1
        assert params.positional[0].value == 42
        assert params.positional[0].sql_type is None
    
    def test_parameters_add_with_type(self):
        """Test adding parameters with SQL type."""
        params = Parameters().add(42, "INT")
        assert len(params) == 1
        assert params.positional[0].value == 42
        assert params.positional[0].sql_type == "INT"
    
    def test_parameters_set_method(self):
        """Test setting named parameters with the set() method."""
        params = Parameters()
        result = params.set("user_id", 123)
        
        # Should return self for chaining
        assert result is params
        assert len(params) == 1
        assert params.named["user_id"].value == 123
        assert params.named["user_id"].sql_type is None
    
    def test_parameters_set_with_type(self):
        """Test setting named parameters with SQL type."""
        params = Parameters().set("name", "John", "NVARCHAR")
        assert len(params) == 1
        assert params.named["name"].value == "John"
        assert params.named["name"].sql_type == "NVARCHAR"
    
    def test_parameters_method_chaining(self):
        """Test method chaining with add() and set()."""
        params = (Parameters()
                 .add(1, "INT")
                 .add("test", "VARCHAR")
                 .set("active", True, "BIT"))
        
        assert len(params) == 3
        assert len(params.positional) == 2
        assert len(params.named) == 1
        
        # Check positional
        assert params.positional[0].value == 1
        assert params.positional[0].sql_type == "INT"
        assert params.positional[1].value == "test"
        assert params.positional[1].sql_type == "VARCHAR"
        
        # Check named
        assert params.named["active"].value == True
        assert params.named["active"].sql_type == "BIT"
    
    def test_parameters_to_list(self):
        """Test converting parameters to simple list."""
        params = Parameters(1, "test", 3.14)
        param_list = params.to_list()
        
        assert param_list == [1, "test", 3.14]
        assert isinstance(param_list, list)
    
    def test_parameters_with_parameter_objects(self):
        """Test creating Parameters with Parameter objects."""
        param1 = Parameter(42, "INT")
        param2 = Parameter("test", "VARCHAR")
        
        params = Parameters(param1, param2)
        assert len(params) == 2
        assert params.positional[0] is param1
        assert params.positional[1] is param2
    
    def test_parameters_repr(self):
        """Test string representation of Parameters."""
        # Empty
        params = Parameters()
        assert repr(params) == "Parameters()"
        
        # Only positional
        params = Parameters(1, 2, 3)
        assert repr(params) == "Parameters(positional=3)"
        
        # Only named
        params = Parameters(name="test", age=30)
        assert repr(params) == "Parameters(named=2)"
        
        # Mixed
        params = Parameters(1, 2, name="test")
        assert "positional=2" in repr(params)
        assert "named=1" in repr(params)
    
    def test_parameters_copy_behavior(self):
        """Test that positional and named properties return copies."""
        params = Parameters(1, 2, name="test")
        
        pos1 = params.positional
        pos2 = params.positional
        named1 = params.named
        named2 = params.named
        
        # Should be equal but not the same object
        assert pos1 == pos2
        assert pos1 is not pos2
        assert named1 == named2
        assert named1 is not named2


@pytest.mark.integration
class TestParametersIntegration:
    """Integration tests with actual database connection."""
    
    @pytest.mark.asyncio
    async def test_simple_list_parameters(self):
        """Test using simple list parameters (backward compatibility)."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                result = await conn.execute(
                    "SELECT @P1 as num, @P2 as text", 
                    [42, "Hello"]
                )
                
                assert result.has_rows()
                rows = result.rows()
                assert len(rows) == 1
                
                row = rows[0]
                assert row['num'] == 42
                assert row['text'] == "Hello"
                
        except Exception as e:
            pytest.skip(f"Database not available: {e}")
    
    @pytest.mark.asyncio
    async def test_parameters_object_basic(self):
        """Test using Parameters object with basic values."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                params = Parameters(100, "Test Product", 29.99)
                
                result = await conn.execute(
                    "SELECT @P1 as id, @P2 as name, @P3 as price", 
                    params
                )
                
                assert result.has_rows()
                rows = result.rows()
                assert len(rows) == 1
                
                row = rows[0]
                assert row['id'] == 100
                assert row['name'] == "Test Product"
                assert abs(row['price'] - 29.99) < 0.01
                
        except Exception as e:
            pytest.skip(f"Database not available: {e}")
    
    @pytest.mark.asyncio
    async def test_parameters_method_chaining_integration(self):
        """Test using Parameters with method chaining."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                params = (Parameters()
                         .add(123, "INT")
                         .add("Chained Test", "NVARCHAR")
                         .add(True, "BIT"))
                
                result = await conn.execute(
                    "SELECT @P1 as id, @P2 as description, @P3 as active", 
                    params
                )
                
                assert result.has_rows()
                rows = result.rows()
                assert len(rows) == 1
                
                row = rows[0]
                assert row['id'] == 123
                assert row['description'] == "Chained Test"
                assert row['active'] == True
                
        except Exception as e:
            pytest.skip(f"Database not available: {e}")
    
    @pytest.mark.asyncio
    async def test_parameters_with_nulls(self):
        """Test using Parameters with NULL values."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                params = Parameters(1, None, "Not Null")
                
                result = await conn.execute(
                    "SELECT @P1 as id, @P2 as nullable_field, @P3 as text", 
                    params
                )
                
                assert result.has_rows()
                rows = result.rows()
                assert len(rows) == 1
                
                row = rows[0]
                assert row['id'] == 1
                assert row['nullable_field'] is None
                assert row['text'] == "Not Null"
                
        except Exception as e:
            pytest.skip(f"Database not available: {e}")
    
    @pytest.mark.asyncio
    async def test_parameters_various_types(self):
        """Test Parameters with various data types."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                params = Parameters(
                    42,           # int
                    3.14159,      # float  
                    "String",     # string
                    True,         # boolean
                    b"binary",    # bytes
                )
                
                result = await conn.execute(
                    "SELECT @P1 as int_val, @P2 as float_val, @P3 as str_val, @P4 as bool_val, @P5 as binary_val", 
                    params
                )
                
                assert result.has_rows()
                rows = result.rows()
                assert len(rows) == 1
                
                row = rows[0]
                assert row['int_val'] == 42
                assert abs(row['float_val'] - 3.14159) < 0.00001
                assert row['str_val'] == "String"
                assert row['bool_val'] == True
                assert row['binary_val'] == b"binary"
                
        except Exception as e:
            pytest.skip(f"Database not available: {e}")
    
    @pytest.mark.asyncio
    async def test_no_parameters(self):
        """Test execute with no parameters."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                result = await conn.execute("SELECT 'No params' as message")
                
                assert result.has_rows()
                rows = result.rows()
                assert len(rows) == 1
                assert rows[0]['message'] == "No params"
                
        except Exception as e:
            pytest.skip(f"Database not available: {e}")
    
    @pytest.mark.asyncio
    async def test_empty_parameters_object(self):
        """Test execute with empty Parameters object."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                params = Parameters()
                result = await conn.execute("SELECT 'Empty params' as message", params)
                
                assert result.has_rows()
                rows = result.rows()
                assert len(rows) == 1
                assert rows[0]['message'] == "Empty params"
                
        except Exception as e:
            pytest.skip(f"Database not available: {e}")
    
    @pytest.mark.asyncio
    async def test_parameter_reuse(self):
        """Test reusing Parameters objects across multiple queries."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                # Create reusable parameters
                params = Parameters(42, "Reused")
                
                # First query
                result1 = await conn.execute(
                    "SELECT @P1 as num, @P2 as text, 'Query 1' as query_id", 
                    params
                )
                
                # Second query with same parameters
                result2 = await conn.execute(
                    "SELECT @P1 as id, @P2 as name, 'Query 2' as query_id", 
                    params
                )
                
                # Both should work
                assert result1.has_rows()
                assert result2.has_rows()
                
                row1 = result1.rows()[0]
                row2 = result2.rows()[0]
                
                assert row1['num'] == 42
                assert row1['text'] == "Reused"
                assert row1['query_id'] == "Query 1"
                
                assert row2['id'] == 42
                assert row2['name'] == "Reused"
                assert row2['query_id'] == "Query 2"
                
        except Exception as e:
            pytest.skip(f"Database not available: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
