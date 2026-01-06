import os

import pytest
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Configuration object for all test database connections."""

    def __init__(self):
        self.connection_string: str = os.getenv("FASTMSSQL_TEST_CONNECTION_STRING")
        self.username: str = os.getenv("FAST_MSSQL_TEST_DB_USER", "SA")
        self.password: str = os.getenv(
            "FAST_MSSQL_TEST_DB_PASSWORD", "YourStrong@Password"
        )
        self.server: str = os.getenv("FAST_MSSQL_TEST_SERVER", "localhost")
        self.port: int = int(os.getenv("FAST_MSSQL_TEST_PORT", "1433"))
        self.database: str = os.getenv("FAST_MSSQL_TEST_DATABASE")

    def asdict(self):
        return vars(self)


@pytest.fixture(scope="session")
def test_config():
    """Get the test configuration object."""
    return Config()


def pytest_configure(config):
    """Configure pytest settings globally."""
    # Set timeout to 30 seconds for integration tests (database operations can be slow)
    config.option.timeout = 30
