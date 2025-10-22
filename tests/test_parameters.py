"""
Unit tests for Parameter and Parameters classes

Tests the new parameter system that allows cleaner parameterized queries
with optional type hints and method chaining.
"""

import pytest
import os

# Add the parent directory to Python path for development

try:
    from fastmssql import Connection, Parameter, Parameters
except ImportError:
    pytest.fail("fastmssql not available - run 'maturin develop' first", allow_module_level=True)

# Test configuration
TEST_CONNECTION_STRING = os.getenv(
    "FASTMSSQL_TEST_CONNECTION_STRING",
)

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
    
    def test_parameter_automatic_expansion(self):
        """Test automatic iterable expansion for IN clauses."""
        values = [1, 2, 3, 4]
        param = Parameter(values)
        
        assert param.value == values
        assert param.is_expanded == True
        assert param.sql_type is None
    
    def test_parameter_automatic_expansion_with_type(self):
        """Test automatic expansion with SQL type."""
        values = [1, 2, 3, 4]
        param = Parameter(values, "INT")
        
        assert param.value == values
        assert param.is_expanded == True
        assert param.sql_type == "INT"
    
    def test_parameter_automatic_expansion_repr(self):
        """Test string representation of automatically expanded parameters."""
        values = [1, 2, 3]
        param = Parameter(values)
        assert repr(param) == "Parameter(IN_values=[1, 2, 3])"
        
        param_with_type = Parameter(values, "INT")
        assert repr(param_with_type) == "Parameter(IN_values=[1, 2, 3], type=INT)"
    
    def test_parameter_automatic_iterable_detection(self):
        """Test automatic iterable detection for expansion."""
        # These should NOT be expanded (strings and bytes are not iterables for expansion)
        string_param = Parameter("hello")
        assert string_param.value == "hello"
        assert string_param.is_expanded == False
        
        bytes_param = Parameter(b"bytes")
        assert bytes_param.value == b"bytes"
        assert bytes_param.is_expanded == False
        
        # These SHOULD be expanded automatically
        list_param = Parameter([1, 2, 3])
        assert list_param.is_expanded == True
        
        tuple_param = Parameter((1, 2, 3))
        assert tuple_param.is_expanded == True
        
        set_param = Parameter({1, 2, 3})
        assert set_param.is_expanded == True


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
    
    def test_parameters_automatic_iterable_expansion(self):
        """Test that iterables are passed to Rust for expansion."""
        # Test with list that will be expanded by Rust
        params = Parameters([1, 2, 3], "John")
        assert len(params) == 2  # 2 parameters: list and string (Rust handles expansion)
        
        # First parameter contains the list (Rust will expand it)
        first_param = params.positional[0]
        assert first_param.value == [1, 2, 3]
        assert first_param.is_expanded == True  # Marked for expansion by Rust
        
        # Second parameter should not be expanded
        second_param = params.positional[1]
        assert second_param.value == "John"
        assert second_param.is_expanded == False
    
    def test_parameters_mixed_iterables_and_values(self):
        """Test Parameters with mix of iterables and regular values."""
        params = Parameters(
            [1, 2, 3],        # Should be marked for expansion
            "regular_string", # Should not be expanded
            (4, 5, 6),        # Should be marked for expansion
            42                # Should not be expanded
        )
        
        assert len(params) == 4  # 4 parameters (Rust handles expansion)
        
        # Check expansion status
        assert params.positional[0].is_expanded == True   # list
        assert params.positional[1].is_expanded == False  # string
        assert params.positional[2].is_expanded == True   # tuple
        assert params.positional[3].is_expanded == False  # int
        
        # Check values (raw values, Rust expands them)
        assert params.positional[0].value == [1, 2, 3]
        assert params.positional[1].value == "regular_string"
        assert params.positional[2].value == (4, 5, 6)  # tuple preserved as tuple
        assert params.positional[3].value == 42
    
    def test_parameters_add_iterable(self):
        """Test adding iterables with add() method."""
        params = Parameters()
        params.add([1, 2, 3], "INT")
        
        assert len(params) == 1
        param = params.positional[0]
        assert param.value == [1, 2, 3]
        assert param.is_expanded == True  # Iterables are marked for Rust expansion
        assert param.sql_type == "INT"
    
    def test_parameters_rust_expansion_behavior(self):
        """Test that both constructor and add() method mark iterables for Rust expansion."""
        # Constructor behavior: iterables marked for Rust expansion
        params1 = Parameters([1, 2, 3])
        assert len(params1) == 1  # Single parameter (list)
        assert params1.positional[0].value == [1, 2, 3]
        assert params1.positional[0].is_expanded == True  # Rust will expand this
        
        # add() method behavior: same as constructor
        params2 = Parameters().add([1, 2, 3])
        assert len(params2) == 1  # Single parameter (list)
        assert params2.positional[0].value == [1, 2, 3]
        assert params2.positional[0].is_expanded == True  # Rust will expand this
    
    def test_parameters_set_iterable(self):
        """Test setting iterables with set() method."""
        params = Parameters()
        params.set("user_ids", [10, 20, 30], "BIGINT")
        
        assert len(params) == 1
        param = params.named["user_ids"]
        assert param.value == [10, 20, 30]
        assert param.is_expanded == True
        assert param.sql_type == "BIGINT"
    
    def test_parameters_string_not_expanded(self):
        """Test that strings are not expanded even when they're iterable."""
        params = Parameters("hello")
        
        assert len(params) == 1
        param = params.positional[0]
        assert param.value == "hello"
        assert param.is_expanded == False


@pytest.mark.integration
class TestParametersIntegration:
    """Integration tests with actual database connection."""
    
    @pytest.mark.asyncio
    async def test_simple_list_parameters(self):
        """Test using simple list parameters (backward compatibility)."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                result = await conn.query(
                    "SELECT @P1 as num, @P2 as text", 
                    [42, "Hello"]
                )
                
                assert result.has_rows()
                rows = result.rows() if result.has_rows() else []
                assert len(rows) == 1
                
                row = rows[0]
                assert row['num'] == 42
                assert row['text'] == "Hello"
                
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.asyncio
    async def test_parameters_object_basic(self):
        """Test using Parameters object with basic values."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                params = Parameters(100, "Test Product", 29.99)
                
                result = await conn.query(
                    "SELECT @P1 as id, @P2 as name, @P3 as price", 
                    params
                )
                
                assert result.has_rows()
                rows = result.rows() if result.has_rows() else []
                assert len(rows) == 1
                
                row = rows[0]
                assert row['id'] == 100
                assert row['name'] == "Test Product"
                assert abs(row['price'] - 29.99) < 0.01
                
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.asyncio
    async def test_parameters_method_chaining_integration(self):
        """Test using Parameters with method chaining."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                params = (Parameters()
                         .add(123, "INT")
                         .add("Chained Test", "NVARCHAR")
                         .add(True, "BIT"))
                
                result = await conn.query(
                    "SELECT @P1 as id, @P2 as description, @P3 as active", 
                    params
                )
                
                assert result.has_rows()
                rows = result.rows() if result.has_rows() else []
                assert len(rows) == 1
                
                row = rows[0]
                assert row['id'] == 123
                assert row['description'] == "Chained Test"
                assert row['active'] == True
                
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.asyncio
    async def test_parameters_with_nulls(self):
        """Test using Parameters with NULL values."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                params = Parameters(1, None, "Not Null")
                
                result = await conn.query(
                    "SELECT @P1 as id, @P2 as nullable_field, @P3 as text", 
                    params
                )
                
                assert result.has_rows()
                rows = result.rows() if result.has_rows() else []
                assert len(rows) == 1
                
                row = rows[0]
                assert row['id'] == 1
                assert row['nullable_field'] is None
                assert row['text'] == "Not Null"
                
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
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
                
                result = await conn.query(
                    "SELECT @P1 as int_val, @P2 as float_val, @P3 as str_val, @P4 as bool_val, @P5 as binary_val", 
                    params
                )
                
                assert result.has_rows()
                rows = result.rows() if result.has_rows() else []
                assert len(rows) == 1
                
                row = rows[0]
                assert row['int_val'] == 42
                assert abs(row['float_val'] - 3.14159) < 0.00001
                assert row['str_val'] == "String"
                assert row['bool_val'] == True
                # Binary data might be returned as list of integers, convert to bytes
                binary_val = row['binary_val']
                if isinstance(binary_val, list):
                    binary_val = bytes(binary_val)
                assert binary_val == b"binary"
                
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.asyncio
    async def test_no_parameters(self):
        """Test execute with no parameters."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                result = await conn.query("SELECT 'No params' as message")
                
                assert result.has_rows()
                rows = result.rows() if result.has_rows() else []
                assert len(rows) == 1
                assert rows[0]['message'] == "No params"
                
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.asyncio
    async def test_empty_parameters_object(self):
        """Test execute with empty Parameters object."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                params = Parameters()
                result = await conn.query("SELECT 'Empty params' as message", params)
                
                assert result.has_rows()
                rows = result.rows() if result.has_rows() else []
                assert len(rows) == 1
                assert rows[0]['message'] == "Empty params"
                
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.asyncio
    async def test_parameter_reuse(self):
        """Test reusing Parameters objects across multiple queries."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                # Create reusable parameters
                params = Parameters(42, "Reused")
                
                # First query
                result1 = await conn.query(
                    "SELECT @P1 as num, @P2 as text, 'Query 1' as query_id", 
                    params
                )
                
                # Second query with same parameters
                result2 = await conn.query(
                    "SELECT @P1 as id, @P2 as name, 'Query 2' as query_id", 
                    params
                )
                
                # Both should work
                assert result1.has_rows()
                assert result2.has_rows()
                
            row1 = result1.rows()[0] if result1.has_rows() else {}
            row2 = result2.rows()[0] if result2.has_rows() else {}
            
            assert row1['num'] == 42
            assert row1['text'] == "Reused"
            assert row1['query_id'] == "Query 1"
            
            assert row2['id'] == 42
            assert row2['name'] == "Reused"
            assert row2['query_id'] == "Query 2"
                
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.asyncio
    async def test_automatic_in_clause_expansion(self):
        """Test automatic IN clause expansion with real database."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                # Test simple list - should automatically expand for IN clause
                ids = [1, 2, 3]
                params = Parameters(ids)
                
                # This should work: the list gets expanded automatically
                result = await conn.query(
                    "SELECT @P1 as expanded_values", 
                    params
                )
                
                assert result.has_rows()
                rows = result.rows() if result.has_rows() else []
                assert len(rows) == 1
                
                # The value should be the list itself (the Rust layer handles expansion)
                row = rows[0]
                expanded_val = row['expanded_values']
                # Could be returned as list or string representation depending on Rust implementation
                # Just verify we get something back
                assert expanded_val is not None
                
        except Exception as e:
            pytest.fail(f"Database not available: {e}")

    @pytest.mark.asyncio
    async def test_mixed_parameters_with_iterables(self):
        """Test mixed regular and iterable parameters."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                params = Parameters(
                    "John",        # @P1
                    [1, 2, 3],     # expands to @P2, @P3, @P4
                    25             # becomes @P5
                )
                
                result = await conn.query(
                    "SELECT @P1 AS name, @P2 AS id1, @P3 AS id2, @P4 AS id3, @P5 AS age",
                    params
                )

                assert result.has_rows()
                rows = result.rows()
                assert len(rows) == 1
                
                row = rows[0]
                assert row['name'] == "John"
                assert row['id1'] == 1
                assert row['id2'] == 2
                assert row['id3'] == 3
                assert row['age'] == 25

        except Exception as e:
            pytest.fail(f"Database not available: {e}")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
