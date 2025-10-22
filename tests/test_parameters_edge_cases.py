"""
Edge case and error handling tests for Parameter and Parameters classes

Tests various edge cases, error conditions, and boundary scenarios.
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

class TestParameterEdgeCases:
    """Test edge cases for Parameter class."""
    
    def test_parameter_with_none_value(self):
        """Test Parameter with None value."""
        param = Parameter(None)
        assert param.value is None
        assert param.sql_type is None
        assert "None" in repr(param)
    
    def test_parameter_with_empty_string(self):
        """Test Parameter with empty string."""
        param = Parameter("", "VARCHAR")
        assert param.value == ""
        assert param.sql_type == "VARCHAR"
    
    def test_parameter_with_zero_values(self):
        """Test Parameter with various zero values."""
        test_cases = [
            (0, "INT"),
            (0.0, "FLOAT"),
            (False, "BIT"),
        ]
        
        for value, sql_type in test_cases:
            param = Parameter(value, sql_type)
            assert param.value == value
            assert param.sql_type == sql_type
    
    def test_parameter_with_large_values(self):
        """Test Parameter with large values."""
        # Large integer
        large_int = 9223372036854775807  # max int64
        param = Parameter(large_int, "BIGINT")
        assert param.value == large_int
        
        # Large string
        large_string = "x" * 10000
        param = Parameter(large_string, "NVARCHAR(MAX)")
        assert param.value == large_string
        assert len(param.value) == 10000
    
    def test_parameter_with_unicode(self):
        """Test Parameter with Unicode strings."""
        unicode_strings = [
            "Hello 世界",
            "Café",
            "🌟⭐✨",
            "Здравствуй мир",
            "مرحبا بالعالم"
        ]
        
        for unicode_str in unicode_strings:
            param = Parameter(unicode_str, "NVARCHAR")
            assert param.value == unicode_str


class TestParametersEdgeCases:
    """Test edge cases for Parameters class."""
    
    def test_parameters_with_many_positional(self):
        """Test Parameters with many positional arguments."""
        # Create with 20 positional parameters
        values = list(range(20))
        params = Parameters(*values)
        
        assert len(params) == 20
        assert len(params.positional) == 20
        assert len(params.named) == 0
        
        result_list = params.to_list()
        assert result_list == values
    
    def test_parameters_with_many_named(self):
        """Test Parameters with many named parameters."""
        # Create with 15 named parameters
        kwargs = {f"param_{i}": i for i in range(15)}
        params = Parameters(**kwargs)
        
        assert len(params) == 15
        assert len(params.positional) == 0
        assert len(params.named) == 15
        
        named = params.named
        for i in range(15):
            assert named[f"param_{i}"].value == i
    
    def test_parameters_mixed_large(self):
        """Test Parameters with mix of many positional and named."""
        pos_values = list(range(10))
        named_values = {f"named_{i}": i + 100 for i in range(10)}
        
        params = Parameters(*pos_values, **named_values)
        
        assert len(params) == 20
        assert len(params.positional) == 10
        assert len(params.named) == 10
    
    def test_parameters_chaining_many_operations(self):
        """Test long chain of parameter operations."""
        params = Parameters()
        
        # Chain many add operations
        for i in range(50):
            params = params.add(i)
        
        # Chain many set operations
        for i in range(25):
            params = params.set(f"named_{i}", i + 1000)
        
        assert len(params) == 75
        assert len(params.positional) == 50
        assert len(params.named) == 25
    
    def test_parameters_empty_operations(self):
        """Test Parameters operations on empty object."""
        params = Parameters()
        
        # Should be empty
        assert len(params) == 0
        assert params.to_list() == []
        assert params.positional == []
        assert params.named == {}
        
        # Should handle copy operations
        pos_copy = params.positional
        named_copy = params.named
        assert pos_copy == []
        assert named_copy == {}
    
    def test_parameters_overwrite_named(self):
        """Test overwriting named parameters."""
        params = Parameters(name="original")
        assert params.named["name"].value == "original"
        
        # Overwrite with set method
        params = params.set("name", "updated")
        assert params.named["name"].value == "updated"
        assert len(params.named) == 1  # Should still be just one


class TestParametersTypeHandling:
    """Test type handling in Parameters."""
    
    def test_parameters_mixed_parameter_objects(self):
        """Test mixing Parameter objects and raw values."""
        param1 = Parameter(42, "INT")
        param2 = Parameter("test", "VARCHAR")
        
        # Mix Parameter objects with raw values
        params = Parameters(param1, "raw_string", param2, 3.14)
        
        assert len(params) == 4
        
        pos = params.positional
        assert pos[0] is param1  # Should be the same object
        assert pos[1].value == "raw_string"  # Should be wrapped
        assert pos[1].sql_type is None
        assert pos[2] is param2
        assert pos[3].value == 3.14
    
    def test_parameters_complex_types(self):
        """Test Parameters with complex Python types."""
        import datetime
        from decimal import Decimal
        
        # Test various complex types
        now = datetime.datetime.now()
        decimal_val = Decimal("123.456")
        bytes_val = b"binary data"
        
        params = Parameters(now, decimal_val, bytes_val)
        
        pos = params.positional
        assert pos[0].value == now
        assert pos[1].value == decimal_val
        assert pos[2].value == bytes_val


@pytest.mark.integration
class TestParametersIntegrationEdgeCases:
    """Integration tests for edge cases with database."""
    
    @pytest.mark.asyncio
    async def test_parameters_with_sql_injection_attempt(self):
        """Test that parameters properly prevent SQL injection."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                # Attempt SQL injection through parameter
                malicious_input = "'; DROP TABLE users; --"
                
                params = Parameters(malicious_input)
                
                # This should treat the input as a literal string parameter
                result = await conn.query(
                    "SELECT @P1 as user_input",
                    params
                )
                
                rows = result.rows() if result.has_rows() else []
                assert len(rows) == 1
                # The malicious input should be returned as-is (safely parameterized)
                assert rows[0]['user_input'] == malicious_input
                
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.asyncio
    async def test_parameters_with_special_characters(self):
        """Test parameters with special SQL characters."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                special_strings = [
                    "contains'apostrophe",
                    'contains"quote',
                    "contains\\backslash",
                    "contains\nnewline",
                    "contains\ttab",
                    "contains;semicolon",
                    "contains--comment",
                    "contains/*comment*/",
                ]
                
                for special_str in special_strings:
                    params = Parameters(special_str)
                    
                    result = await conn.query(
                        "SELECT @P1 as special_input",
                        params
                    )
                    
                    rows = result.rows() if result.has_rows() else []
                    assert len(rows) == 1
                    assert rows[0]['special_input'] == special_str
                
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.asyncio
    async def test_parameters_with_very_long_strings(self):
        """Test parameters with very long strings."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                # Create a very long string (but not too long to cause issues)
                long_string = "x" * 4000  # 4KB string
                
                params = Parameters(long_string)
                
                result = await conn.query(
                    "SELECT LEN(@P1) as string_length, LEFT(@P1, 10) as string_start",
                    params
                )
                
                rows = result.rows() if result.has_rows() else []
                assert len(rows) == 1
                assert rows[0]['string_length'] == 4000
                assert rows[0]['string_start'] == "xxxxxxxxxx"
                
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.asyncio
    async def test_parameters_with_null_in_different_positions(self):
        """Test NULL parameters in various positions."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                # Test NULL in different positions
                test_cases = [
                    (None, "second", "third"),
                    ("first", None, "third"),
                    ("first", "second", None),
                    (None, None, "third"),
                    ("first", None, None),
                    (None, None, None),
                ]
                
                for case in test_cases:
                    params = Parameters(*case)
                    
                    result = await conn.query(
                        "SELECT @P1 as col1, @P2 as col2, @P3 as col3",
                        params
                    )
                    
                    rows = result.rows() if result.has_rows() else []
                    assert len(rows) == 1
                    row = rows[0]
                    
                    assert row['col1'] == case[0]
                    assert row['col2'] == case[1] 
                    assert row['col3'] == case[2]
                
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.asyncio
    async def test_parameters_mismatch_count(self):
        """Test error handling when parameter count doesn't match placeholders."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                # Too few parameters
                with pytest.raises(Exception):
                    params = Parameters(42)  # Only 1 parameter
                    await conn.query("SELECT @P1 as col1, @P2 as col2", params)  # 2 placeholders
                
                # Too many parameters (might be silently ignored by some databases)
                params = Parameters(1, 2, 3)  # 3 parameters
                # This might not raise an error depending on database behavior
                try:
                    result = await conn.query("SELECT @P1 as col1", params)  # 1 placeholder
                    # If it doesn't error, verify it used the first parameter
                    rows = result.rows() if result.has_rows() else []
                    assert rows[0]['col1'] == 1
                except Exception:
                    # Error is also acceptable for parameter count mismatch
                    pass
                
        except Exception as e:
            pytest.fail(f"Database not available: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
