import os
import pytest
from dotenv import load_dotenv

@pytest.fixture(scope="session")
def test_connection_string():
    # Load environment variables from .env file
    load_dotenv()
    value = os.getenv("FASTMSSQL_TEST_CONNECTION_STRING")
    if value is None:
        pytest.fail("FASTMSSQL_TEST_CONNECTION_STRING not set in environment or .env file")
    return value
