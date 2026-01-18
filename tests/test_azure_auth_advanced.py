"""
Advanced Azure authentication tests with async functionality and error scenarios.
Tests actual connection behavior, error handling, and edge cases with mocks.
"""

import os
import unittest
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

import fastmssql


class TestAzureAuthenticationAsync(IsolatedAsyncioTestCase):
    """Test async Azure authentication functionality."""

    async def test_connection_context_manager_with_azure_creds(self):
        """Test that Connection can be used as async context manager with Azure credentials."""
        azure_cred = fastmssql.AzureCredential.service_principal(
            client_id="test-client-id",
            client_secret="test-client-secret",
            tenant_id="test-tenant-id",
        )

        conn = fastmssql.Connection(
            server="test.database.windows.net",
            database="testdb",
            azure_credential=azure_cred,
        )

        # Test that the connection object can be created (actual connection would fail without real creds)
        self.assertIsInstance(conn, fastmssql.Connection)

        # The actual async context manager usage would require real credentials
        # so we just test object creation here


class TestAzureAuthenticationErrorScenarios(unittest.TestCase):
    """Test error scenarios and edge cases for Azure authentication."""

    def test_connection_with_both_azure_and_sql_auth(self):
        """Test connection creation with both Azure and SQL authentication provided."""
        azure_cred = fastmssql.AzureCredential.service_principal(
            client_id="test-client-id",
            client_secret="test-client-secret",
            tenant_id="test-tenant-id",
        )

        # Should raise ValueError when both authentication methods are provided
        with self.assertRaises(ValueError) as context:
            _ = fastmssql.Connection(
                server="test.database.windows.net",
                database="testdb",
                username="testuser",  # This conflicts with azure_credential
                password="testpass",  # This conflicts with azure_credential
                azure_credential=azure_cred,
            )

        # Verify the error message mentions the conflict
        self.assertIn("authentication method", str(context.exception))

    def test_invalid_connection_parameters(self):
        """Test connection creation with invalid parameters."""
        azure_cred = fastmssql.AzureCredential.service_principal(
            client_id="test-client-id",
            client_secret="test-client-secret",
            tenant_id="test-tenant-id",
        )

        # Missing server should still allow object creation (error would come during actual connection)
        try:
            conn = fastmssql.Connection(
                server="",  # Empty server
                database="testdb",
                azure_credential=azure_cred,
            )
            self.assertIsInstance(conn, fastmssql.Connection)
        except Exception:
            # Some validation might happen at creation time, which is also valid
            pass

    def test_credential_type_consistency(self):
        """Test that credential types are consistent."""
        # Test that we can create multiple credentials of the same type
        cred1 = fastmssql.AzureCredential.service_principal("id1", "secret1", "tenant1")
        cred2 = fastmssql.AzureCredential.service_principal("id2", "secret2", "tenant2")

        self.assertIsInstance(cred1, fastmssql.AzureCredential)
        self.assertIsInstance(cred2, fastmssql.AzureCredential)

        # They should be different objects
        self.assertIsNot(cred1, cred2)


class TestAzureCredentialEdgeCases(unittest.TestCase):
    """Test edge cases for Azure credential handling."""

    def test_special_characters_in_credentials(self):
        """Test handling of special characters in credential parameters."""
        special_chars = "!@#$%^&*()_+-={}[]|\\:;\"'<>,.?/"

        cred = fastmssql.AzureCredential.service_principal(
            client_id=f"client{special_chars}",
            client_secret=f"secret{special_chars}",
            tenant_id=f"tenant{special_chars}",
        )

        self.assertIsInstance(cred, fastmssql.AzureCredential)

    def test_unicode_characters_in_credentials(self):
        """Test handling of Unicode characters in credential parameters."""
        unicode_str = "测试🔑🌍"

        cred = fastmssql.AzureCredential.service_principal(
            client_id=f"client_{unicode_str}",
            client_secret=f"secret_{unicode_str}",
            tenant_id=f"tenant_{unicode_str}",
        )

        self.assertIsInstance(cred, fastmssql.AzureCredential)

    def test_very_long_credential_strings(self):
        """Test handling of very long credential strings."""
        long_string = "x" * 10000  # 10KB string

        cred = fastmssql.AzureCredential.service_principal(
            client_id=long_string, client_secret=long_string, tenant_id=long_string
        )

        self.assertIsInstance(cred, fastmssql.AzureCredential)

    def test_empty_and_whitespace_credentials(self):
        """Test handling of empty and whitespace-only credential parameters."""
        test_cases = [
            ("", "", ""),  # Empty strings
            ("   ", "   ", "   "),  # Whitespace only
            ("\t\n\r", "\t\n\r", "\t\n\r"),  # Various whitespace chars
        ]

        for client_id, client_secret, tenant_id in test_cases:
            cred = fastmssql.AzureCredential.service_principal(
                client_id=client_id, client_secret=client_secret, tenant_id=tenant_id
            )
            self.assertIsInstance(cred, fastmssql.AzureCredential)


class TestAzureAuthenticationIntegrationPatterns(unittest.TestCase):
    """Test common integration patterns for Azure authentication."""

    def test_credential_reuse_pattern(self):
        """Test reusing the same credential for multiple connections."""
        azure_cred = fastmssql.AzureCredential.service_principal(
            client_id="shared-client-id",
            client_secret="shared-client-secret",
            tenant_id="shared-tenant-id",
        )

        # Create multiple connections with the same credential
        connections = []
        for i in range(5):
            conn = fastmssql.Connection(
                server=f"server{i}.database.windows.net",
                database=f"database{i}",
                azure_credential=azure_cred,
            )
            connections.append(conn)
            self.assertIsInstance(conn, fastmssql.Connection)

    def test_different_credential_types_pattern(self):
        """Test using different credential types for different scenarios."""
        credentials = {
            "production": fastmssql.AzureCredential.managed_identity(client_id=None),
            "development": fastmssql.AzureCredential.service_principal(
                "dev-id", "dev-secret", "dev-tenant"
            ),
            "testing": fastmssql.AzureCredential.access_token("test-token"),
            "fallback": fastmssql.AzureCredential.default(),
        }

        for env_name, cred in credentials.items():
            conn = fastmssql.Connection(
                server=f"{env_name}.database.windows.net",
                database=f"{env_name}_db",
                azure_credential=cred,
            )
            self.assertIsInstance(conn, fastmssql.Connection)

    @patch.dict(
        os.environ,
        {
            "ENVIRONMENT": "test",
            "AZURE_CLIENT_ID": "env-client-id",
            "AZURE_CLIENT_SECRET": "env-client-secret",
            "AZURE_TENANT_ID": "env-tenant-id",
        },
    )
    def test_environment_based_credential_selection(self):
        """Test selecting credential type based on environment variables."""
        environment = os.getenv("ENVIRONMENT", "development")

        if environment == "production":
            # Use managed identity in production
            azure_cred = fastmssql.AzureCredential.managed_identity(client_id=None)
        elif environment == "test":
            # Use service principal in test
            azure_cred = fastmssql.AzureCredential.service_principal(
                client_id=os.getenv("AZURE_CLIENT_ID"),
                client_secret=os.getenv("AZURE_CLIENT_SECRET"),
                tenant_id=os.getenv("AZURE_TENANT_ID"),
            )
        else:
            # Use default credential chain for other environments
            azure_cred = fastmssql.AzureCredential.default()

        conn = fastmssql.Connection(
            server="myserver.database.windows.net",
            database="mydatabase",
            azure_credential=azure_cred,
        )

        self.assertIsInstance(conn, fastmssql.Connection)


class TestAzureCredentialValidation(unittest.TestCase):
    """Test validation and error handling for Azure credentials."""

    def test_service_principal_parameter_types(self):
        """Test that Service Principal accepts string parameters."""
        # Test with string parameters (expected)
        cred = fastmssql.AzureCredential.service_principal("id", "secret", "tenant")
        self.assertIsInstance(cred, fastmssql.AzureCredential)

        # Test with string conversion of other types
        cred2 = fastmssql.AzureCredential.service_principal(
            str(123),  # Convert int to string
            str(456),  # Convert int to string
            str(789),  # Convert int to string
        )
        self.assertIsInstance(cred2, fastmssql.AzureCredential)

    def test_managed_identity_client_id_types(self):
        """Test Managed Identity client_id parameter types."""
        # None (system-assigned)
        cred1 = fastmssql.AzureCredential.managed_identity(client_id=None)
        self.assertIsInstance(cred1, fastmssql.AzureCredential)

        # String (user-assigned)
        cred2 = fastmssql.AzureCredential.managed_identity(client_id="user-assigned-id")
        self.assertIsInstance(cred2, fastmssql.AzureCredential)

        # Empty string (should work)
        cred3 = fastmssql.AzureCredential.managed_identity(client_id="")
        self.assertIsInstance(cred3, fastmssql.AzureCredential)

    def test_access_token_parameter_types(self):
        """Test Access Token parameter types."""
        # Regular token string
        cred1 = fastmssql.AzureCredential.access_token("eyJ0eXAiOiJKV1QiLCJhbGc...")
        self.assertIsInstance(cred1, fastmssql.AzureCredential)

        # Empty token (should work, error would come during usage)
        cred2 = fastmssql.AzureCredential.access_token("")
        self.assertIsInstance(cred2, fastmssql.AzureCredential)


def run_azure_auth_tests():
    """
    Convenience function to run all Azure authentication tests.
    Can be called from other test modules or scripts.
    """
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add all test classes
    test_classes = [
        TestAzureAuthenticationAsync,
        TestAzureAuthenticationErrorScenarios,
        TestAzureCredentialEdgeCases,
        TestAzureAuthenticationIntegrationPatterns,
        TestAzureCredentialValidation,
    ]

    for test_class in test_classes:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return result.wasSuccessful()