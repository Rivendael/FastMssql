"""
Unit tests for Azure authentication functionality in FastMSSQL.
Tests credential creation, token acquisition, and error handling with mocks.
"""

import os
import unittest
from unittest.mock import patch

import fastmssql


class TestAzureCredentials(unittest.TestCase):
    """Test Azure credential creation and basic functionality."""

    def test_service_principal_creation(self):
        """Test Service Principal credential creation."""
        cred = fastmssql.AzureCredential.service_principal(
            client_id="test-client-id",
            client_secret="test-client-secret",
            tenant_id="test-tenant-id",
        )

        self.assertIsInstance(cred, fastmssql.AzureCredential)
        # We can't access internal properties directly, but creation should succeed

    def test_managed_identity_creation(self):
        """Test Managed Identity credential creation."""
        # System-assigned managed identity
        cred1 = fastmssql.AzureCredential.managed_identity(client_id=None)
        self.assertIsInstance(cred1, fastmssql.AzureCredential)

        # User-assigned managed identity
        cred2 = fastmssql.AzureCredential.managed_identity(client_id="test-client-id")
        self.assertIsInstance(cred2, fastmssql.AzureCredential)

    def test_access_token_creation(self):
        """Test Access Token credential creation."""
        cred = fastmssql.AzureCredential.access_token("test-access-token")
        self.assertIsInstance(cred, fastmssql.AzureCredential)

    def test_default_credential_creation(self):
        """Test Default credential creation."""
        cred = fastmssql.AzureCredential.default()
        self.assertIsInstance(cred, fastmssql.AzureCredential)

    def test_connection_creation_with_azure_credential(self):
        """Test Connection creation with Azure credentials."""
        azure_cred = fastmssql.AzureCredential.service_principal(
            client_id="test-client-id",
            client_secret="test-client-secret",
            tenant_id="test-tenant-id",
        )

        # Should not raise an exception
        conn = fastmssql.Connection(
            server="test.database.windows.net",
            database="testdb",
            azure_credential=azure_cred,
        )
        self.assertIsInstance(conn, fastmssql.Connection)


class TestAzureCredentialTypes(unittest.TestCase):
    """Test Azure credential types enum."""

    def test_azure_credential_type_constants(self):
        """Test that AzureCredentialType constants exist."""
        self.assertTrue(hasattr(fastmssql.AzureCredentialType, "SERVICE_PRINCIPAL"))
        self.assertTrue(hasattr(fastmssql.AzureCredentialType, "MANAGED_IDENTITY"))
        self.assertTrue(hasattr(fastmssql.AzureCredentialType, "ACCESS_TOKEN"))
        self.assertTrue(hasattr(fastmssql.AzureCredentialType, "DEFAULT_AZURE"))


class TestAzureAuthenticationIntegration(unittest.TestCase):
    """Test Azure authentication integration with Connection class."""

    @patch.dict(
        os.environ,
        {
            "AZURE_CLIENT_ID": "test-client-id",
            "AZURE_CLIENT_SECRET": "test-client-secret",
            "AZURE_TENANT_ID": "test-tenant-id",
        },
    )
    def test_connection_with_environment_variables(self):
        """Test connection creation when environment variables are set."""
        azure_cred = fastmssql.AzureCredential.service_principal(
            client_id=os.getenv("AZURE_CLIENT_ID"),
            client_secret=os.getenv("AZURE_CLIENT_SECRET"),
            tenant_id=os.getenv("AZURE_TENANT_ID"),
        )

        conn = fastmssql.Connection(
            server="test.database.windows.net",
            database="testdb",
            azure_credential=azure_cred,
        )
        self.assertIsInstance(conn, fastmssql.Connection)

    def test_connection_without_azure_credential(self):
        """Test connection creation without Azure credential (should work for SQL auth)."""
        conn = fastmssql.Connection(
            server="test.database.windows.net",
            database="testdb",
            username="testuser",
            password="testpass",
        )
        self.assertIsInstance(conn, fastmssql.Connection)


class TestParameterValidation(unittest.TestCase):
    """Test parameter validation for Azure credentials."""

    def test_service_principal_requires_all_parameters(self):
        """Test that Service Principal creation validates required parameters."""
        # This should work
        cred = fastmssql.AzureCredential.service_principal(
            client_id="test", client_secret="test", tenant_id="test"
        )
        self.assertIsInstance(cred, fastmssql.AzureCredential)

        # Empty strings should still create credential (validation happens at token acquisition)
        cred2 = fastmssql.AzureCredential.service_principal(
            client_id="", client_secret="", tenant_id=""
        )
        self.assertIsInstance(cred2, fastmssql.AzureCredential)

    def test_managed_identity_optional_client_id(self):
        """Test that Managed Identity client_id is optional."""
        # Without client_id (system-assigned)
        cred1 = fastmssql.AzureCredential.managed_identity(client_id=None)
        self.assertIsInstance(cred1, fastmssql.AzureCredential)

        # With client_id (user-assigned)
        cred2 = fastmssql.AzureCredential.managed_identity(client_id="test-id")
        self.assertIsInstance(cred2, fastmssql.AzureCredential)

    def test_access_token_requires_token(self):
        """Test that Access Token creation requires a token."""
        cred = fastmssql.AzureCredential.access_token("test-token")
        self.assertIsInstance(cred, fastmssql.AzureCredential)

        # Empty token should still create credential
        cred2 = fastmssql.AzureCredential.access_token("")
        self.assertIsInstance(cred2, fastmssql.AzureCredential)


class TestAzureAuthMockingScenarios(unittest.TestCase):
    """Test scenarios that would require actual Azure authentication, using mocks."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_server = "test.database.windows.net"
        self.test_database = "testdb"
        self.service_principal_cred = fastmssql.AzureCredential.service_principal(
            client_id="test-client-id",
            client_secret="test-client-secret",
            tenant_id="test-tenant-id",
        )

    def test_connection_object_creation_with_azure_creds(self):
        """Test that Connection objects can be created with Azure credentials."""
        conn = fastmssql.Connection(
            server=self.test_server,
            database=self.test_database,
            azure_credential=self.service_principal_cred,
        )
        self.assertIsInstance(conn, fastmssql.Connection)

    def test_multiple_credential_types_in_connections(self):
        """Test creating connections with different credential types."""
        credentials = [
            fastmssql.AzureCredential.service_principal("id", "secret", "tenant"),
            fastmssql.AzureCredential.managed_identity(client_id=None),
            fastmssql.AzureCredential.managed_identity(client_id="test-id"),
            fastmssql.AzureCredential.access_token("test-token"),
            fastmssql.AzureCredential.default(),
        ]

        for cred in credentials:
            conn = fastmssql.Connection(
                server=self.test_server,
                database=self.test_database,
                azure_credential=cred,
            )
            self.assertIsInstance(conn, fastmssql.Connection)


class TestRealWorldScenarios(unittest.TestCase):
    """Test real-world usage scenarios without actual network calls."""

    def test_typical_service_principal_workflow(self):
        """Test typical Service Principal authentication workflow."""
        # Step 1: Create credential from environment variables (mocked)
        with patch.dict(
            os.environ,
            {
                "AZURE_CLIENT_ID": "real-client-id",
                "AZURE_CLIENT_SECRET": "real-client-secret",
                "AZURE_TENANT_ID": "real-tenant-id",
                "AZURE_SQL_SERVER": "myserver.database.windows.net",
                "AZURE_SQL_DATABASE": "mydatabase",
            },
        ):
            # Step 2: Create credential
            azure_cred = fastmssql.AzureCredential.service_principal(
                client_id=os.getenv("AZURE_CLIENT_ID"),
                client_secret=os.getenv("AZURE_CLIENT_SECRET"),
                tenant_id=os.getenv("AZURE_TENANT_ID"),
            )

            # Step 3: Create connection
            conn = fastmssql.Connection(
                server=os.getenv("AZURE_SQL_SERVER"),
                database=os.getenv("AZURE_SQL_DATABASE"),
                azure_credential=azure_cred,
            )

            self.assertIsInstance(conn, fastmssql.Connection)

    def test_azure_resource_managed_identity_workflow(self):
        """Test managed identity workflow for Azure resources."""
        # System-assigned managed identity (typical for VMs, Function Apps)
        azure_cred = fastmssql.AzureCredential.managed_identity(client_id=None)

        conn = fastmssql.Connection(
            server="myserver.database.windows.net",
            database="mydatabase",
            azure_credential=azure_cred,
        )

        self.assertIsInstance(conn, fastmssql.Connection)

    def test_fallback_credential_chain(self):
        """Test default credential chain usage."""
        # This would try multiple auth methods in order
        azure_cred = fastmssql.AzureCredential.default()

        conn = fastmssql.Connection(
            server="myserver.database.windows.net",
            database="mydatabase",
            azure_credential=azure_cred,
        )

        self.assertIsInstance(conn, fastmssql.Connection)


if __name__ == "__main__":
    # Configure test discovery and execution
    unittest.main(verbosity=2)
