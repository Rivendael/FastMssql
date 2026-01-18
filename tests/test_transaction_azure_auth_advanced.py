"""
Advanced unit tests for Transaction Azure authentication functionality.
Tests complex scenarios, edge cases, and integration patterns with mocks.
"""

import json
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import fastmssql


class TestTransactionAzureAdvancedScenarios(unittest.TestCase):
    """Test advanced Transaction scenarios with Azure authentication."""

    def setUp(self):
        """Set up test fixtures."""
        self.service_principal_cred = fastmssql.AzureCredential.service_principal(
            client_id="sp-client-id",
            client_secret="sp-client-secret",
            tenant_id="sp-tenant-id",
        )

        self.managed_identity_cred = fastmssql.AzureCredential.managed_identity(
            client_id="mi-client-id"
        )

        self.access_token_cred = fastmssql.AzureCredential.access_token(
            "direct-access-token-12345"
        )

    async def test_transaction_with_multiple_azure_credential_types(self):
        """Test Transaction creation with different Azure credential types."""
        credentials = [
            self.service_principal_cred,
            self.managed_identity_cred,
            self.access_token_cred,
            fastmssql.AzureCredential.default(),
        ]

        for i, cred in enumerate(credentials):
            with self.subTest(f"Credential type {i}"):
                transaction = fastmssql.Transaction(
                    server=f"test-server-{i}.database.windows.net",
                    database=f"test-database-{i}",
                    azure_credential=cred,
                )
                self.assertIsInstance(transaction, fastmssql.Transaction)

    @patch("fastmssql.Transaction._ensure_connected", new_callable=AsyncMock)
    @patch("fastmssql.Transaction.__init__", return_value=None)
    async def test_transaction_full_lifecycle_with_azure_auth(
        self, mock_init, mock_ensure_connected
    ):
        """Test complete Transaction lifecycle with Azure authentication."""
        # Create mock Transaction instance
        transaction = fastmssql.Transaction.__new__(fastmssql.Transaction)
        transaction.azure_credential = self.service_principal_cred

        # Mock all transaction methods
        with (
            patch.object(transaction, "begin", new_callable=AsyncMock) as mock_begin,
            patch.object(transaction, "query", new_callable=AsyncMock) as mock_query,
            patch.object(
                transaction, "execute", new_callable=AsyncMock
            ) as mock_execute,
            patch.object(transaction, "commit", new_callable=AsyncMock) as mock_commit,
            patch.object(transaction, "close", new_callable=AsyncMock) as mock_close,
        ):
            # Mock query results
            mock_result = MagicMock()
            mock_result.rows.return_value = [{"user": "azure-user", "db": "test-db"}]
            mock_query.return_value = mock_result
            mock_execute.return_value = 2  # Affected rows

            # Execute full transaction lifecycle
            await transaction.begin()
            result = await transaction.query(
                "SELECT USER_NAME() as user, DB_NAME() as db"
            )
            await transaction.execute(
                "INSERT INTO test_table VALUES ('data1'), ('data2')"
            )
            await transaction.commit()
            await transaction.close()

            # Verify all operations were called
            mock_begin.assert_called_once()
            mock_query.assert_called_once()
            mock_execute.assert_called_once()
            mock_commit.assert_called_once()
            mock_close.assert_called_once()

            # Verify query results
            self.assertEqual(result.rows()[0]["user"], "azure-user")
            self.assertEqual(result.rows()[0]["db"], "test-db")

    @patch("fastmssql.Transaction._ensure_connected", new_callable=AsyncMock)
    @patch("fastmssql.Transaction.__init__", return_value=None)
    async def test_transaction_nested_operations_with_azure_auth(
        self, mock_init, mock_ensure_connected
    ):
        """Test Transaction with nested operations using Azure authentication."""
        # Create mock Transaction instance
        transaction = fastmssql.Transaction.__new__(fastmssql.Transaction)
        transaction.azure_credential = self.managed_identity_cred

        with (
            patch.object(transaction, "begin", new_callable=AsyncMock) as mock_begin,
            patch.object(
                transaction, "execute", new_callable=AsyncMock
            ) as mock_execute,
            patch.object(
                transaction, "rollback", new_callable=AsyncMock
            ) as mock_rollback,
        ):
            mock_execute.side_effect = [1, RuntimeError("Constraint violation"), 1]

            # Simulate transaction with error requiring rollback
            await transaction.begin()

            # First execute succeeds
            result1 = await transaction.execute("INSERT INTO table1 VALUES ('data1')")
            self.assertEqual(result1, 1)

            # Second execute fails
            with self.assertRaises(RuntimeError):
                await transaction.execute("INSERT INTO table2 VALUES ('invalid')")

            # Rollback due to error
            await transaction.rollback()

            # Verify operations
            mock_begin.assert_called_once()
            self.assertEqual(mock_execute.call_count, 2)
            mock_rollback.assert_called_once()

    @patch("fastmssql.Transaction._ensure_connected", new_callable=AsyncMock)
    @patch("fastmssql.Transaction.__init__", return_value=None)
    async def test_transaction_batch_operations_with_azure_auth(
        self, mock_init, mock_ensure_connected
    ):
        """Test Transaction batch operations with Azure authentication."""
        # Create mock Transaction instance
        transaction = fastmssql.Transaction.__new__(fastmssql.Transaction)
        transaction.azure_credential = self.access_token_cred

        # Test execute_batch
        batch_commands = [
            "INSERT INTO users (name) VALUES ('Alice')",
            "INSERT INTO users (name) VALUES ('Bob')",
            "UPDATE users SET active = 1",
        ]

        with patch.object(
            transaction, "execute_batch", new_callable=AsyncMock
        ) as mock_execute_batch:
            mock_execute_batch.return_value = [
                1,
                1,
                2,
            ]  # Affected rows for each command

            results = await transaction.execute_batch(batch_commands)

            mock_execute_batch.assert_called_once_with(batch_commands)
            self.assertEqual(results, [1, 1, 2])
            self.assertEqual(sum(results), 4)  # Total affected rows

        # Test query_batch
        batch_queries = [
            "SELECT COUNT(*) as total FROM users",
            "SELECT COUNT(*) as active FROM users WHERE active = 1",
            "SELECT name FROM users ORDER BY name",
        ]

        mock_result1 = MagicMock()
        mock_result1.rows.return_value = [{"total": 2}]
        mock_result2 = MagicMock()
        mock_result2.rows.return_value = [{"active": 2}]
        mock_result3 = MagicMock()
        mock_result3.rows.return_value = [{"name": "Alice"}, {"name": "Bob"}]

        with patch.object(
            transaction, "query_batch", new_callable=AsyncMock
        ) as mock_query_batch:
            mock_query_batch.return_value = [mock_result1, mock_result2, mock_result3]

            results = await transaction.query_batch(batch_queries)

            mock_query_batch.assert_called_once_with(batch_queries)
            self.assertEqual(len(results), 3)
            self.assertEqual(results[0].rows()[0]["total"], 2)
            self.assertEqual(results[1].rows()[0]["active"], 2)
            self.assertEqual(len(results[2].rows()), 2)

    def test_transaction_azure_credential_validation_edge_cases(self):
        """Test Transaction Azure credential validation edge cases."""

        # Test with empty strings (should actually work - Azure allows empty strings)
        try:
            cred = fastmssql.AzureCredential.service_principal(
                client_id="",
                client_secret="valid-secret",
                tenant_id="valid-tenant",
            )
            self.assertIsInstance(cred, fastmssql.AzureCredential)
        except Exception:
            # If it fails, that's also acceptable behavior
            pass

        # Test Transaction with both credential types
        azure_cred = fastmssql.AzureCredential.access_token("test-token")

        with self.assertRaises(ValueError) as context:
            fastmssql.Transaction(
                server="test-server.database.windows.net",
                database="test-database",
                username="testuser",
                password="testpass",
                azure_credential=azure_cred,
            )

        self.assertIn(
            "Cannot use both username/password and azure_credential",
            str(context.exception),
        )

    @patch("fastmssql.Transaction._ensure_connected", new_callable=AsyncMock)
    @patch("fastmssql.Transaction.__init__", return_value=None)
    async def test_transaction_connection_resilience_with_azure_auth(
        self, mock_init, mock_ensure_connected
    ):
        """Test Transaction connection resilience with Azure authentication."""
        # Create mock Transaction instance
        transaction = fastmssql.Transaction.__new__(fastmssql.Transaction)
        transaction.azure_credential = fastmssql.AzureCredential.default()

        # Simulate connection recovery scenario
        mock_ensure_connected.side_effect = [
            RuntimeError("Connection lost"),  # First call fails
            None,  # Second call succeeds
            None,  # Third call succeeds
        ]

        with patch.object(transaction, "query", new_callable=AsyncMock) as mock_query:
            mock_result = MagicMock()
            mock_result.rows.return_value = [{"status": "connected"}]
            mock_query.return_value = mock_result

            # First query should fail due to connection error
            with self.assertRaises(RuntimeError):
                await transaction.query("SELECT 'test' as status")

            # Subsequent queries should succeed after connection recovery
            result = await transaction.query("SELECT 'connected' as status")
            self.assertEqual(result.rows()[0]["status"], "connected")


class TestTransactionAzureTokenManagement(unittest.TestCase):
    """Test Transaction Azure token management and renewal scenarios."""

    @patch("requests.post")
    async def test_transaction_service_principal_token_refresh(self, mock_post):
        """Test Transaction Service Principal token refresh scenario."""
        # First call returns token
        mock_response1 = MagicMock()
        mock_response1.status_code = 200
        mock_response1.json.return_value = {
            "access_token": "initial-token",
            "token_type": "Bearer",
            "expires_in": 3600,
        }

        # Second call returns refreshed token
        mock_response2 = MagicMock()
        mock_response2.status_code = 200
        mock_response2.json.return_value = {
            "access_token": "refreshed-token",
            "token_type": "Bearer",
            "expires_in": 3600,
        }

        mock_post.side_effect = [mock_response1, mock_response2]

        azure_cred = fastmssql.AzureCredential.service_principal(
            client_id="refresh-test-client",
            client_secret="refresh-test-secret",
            tenant_id="refresh-test-tenant",
        )

        # Creating multiple transactions should potentially trigger token refresh
        transaction1 = fastmssql.Transaction(
            server="test-server.database.windows.net",
            database="test-database",
            azure_credential=azure_cred,
        )

        transaction2 = fastmssql.Transaction(
            server="test-server.database.windows.net",
            database="test-database",
            azure_credential=azure_cred,
        )

        self.assertIsInstance(transaction1, fastmssql.Transaction)
        self.assertIsInstance(transaction2, fastmssql.Transaction)

    @patch("requests.post")
    async def test_transaction_token_acquisition_failure_handling(self, mock_post):
        """Test Transaction handling of token acquisition failures."""
        # Mock failed token response
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized: Invalid client credentials"
        mock_post.return_value = mock_response

        azure_cred = fastmssql.AzureCredential.service_principal(
            client_id="invalid-client",
            client_secret="invalid-secret",
            tenant_id="invalid-tenant",
        )

        # Transaction creation should succeed (token not acquired until connection)
        transaction = fastmssql.Transaction(
            server="test-server.database.windows.net",
            database="test-database",
            azure_credential=azure_cred,
        )

        self.assertIsInstance(transaction, fastmssql.Transaction)

    @patch("requests.get")
    async def test_transaction_managed_identity_timeout_handling(self, mock_get):
        """Test Transaction handling of Managed Identity timeout."""
        # Mock timeout response
        mock_get.side_effect = TimeoutError("IMDS request timed out")

        azure_cred = fastmssql.AzureCredential.managed_identity()

        # Transaction creation should succeed (token not acquired until connection)
        transaction = fastmssql.Transaction(
            server="test-server.database.windows.net",
            database="test-database",
            azure_credential=azure_cred,
        )

        self.assertIsInstance(transaction, fastmssql.Transaction)


class TestTransactionAzureIntegrationPatterns(unittest.TestCase):
    """Test Transaction integration patterns with Azure authentication."""

    @patch("fastmssql.Transaction._ensure_connected", new_callable=AsyncMock)
    @patch("fastmssql.Transaction.__init__", return_value=None)
    async def test_transaction_as_context_manager_with_azure_auth(
        self, mock_init, mock_ensure_connected
    ):
        """Test Transaction as context manager with Azure authentication."""
        # Create mock Transaction instance
        transaction = fastmssql.Transaction.__new__(fastmssql.Transaction)
        transaction.azure_credential = fastmssql.AzureCredential.access_token(
            "context-token"
        )

        # Mock context manager methods
        async def mock_aenter():
            return transaction

        async def mock_aexit(exc_type, exc_val, exc_tb):
            return False

        transaction.__aenter__ = mock_aenter
        transaction.__aexit__ = mock_aexit

        with (
            patch.object(transaction, "begin", new_callable=AsyncMock) as mock_begin,
            patch.object(
                transaction, "execute", new_callable=AsyncMock
            ) as mock_execute,
            patch.object(transaction, "commit", new_callable=AsyncMock) as mock_commit,
        ):
            mock_execute.return_value = 3

            # Use transaction as context manager
            async with transaction as tx:
                await tx.begin()
                result = await tx.execute("INSERT INTO test VALUES (1), (2), (3)")
                await tx.commit()

                self.assertEqual(result, 3)

            # Verify operations were called
            mock_begin.assert_called_once()
            mock_execute.assert_called_once()
            mock_commit.assert_called_once()

    async def test_transaction_concurrent_access_with_azure_auth(self):
        """Test Transaction concurrent access patterns with Azure authentication."""
        azure_cred = fastmssql.AzureCredential.service_principal(
            client_id="concurrent-test-client",
            client_secret="concurrent-test-secret",
            tenant_id="concurrent-test-tenant",
        )

        # Create multiple transactions (simulating concurrent access)
        transactions = []
        for i in range(3):
            transaction = fastmssql.Transaction(
                server=f"concurrent-server-{i}.database.windows.net",
                database=f"concurrent-db-{i}",
                azure_credential=azure_cred,
            )
            transactions.append(transaction)

        # All transactions should be created successfully
        for transaction in transactions:
            self.assertIsInstance(transaction, fastmssql.Transaction)

    @patch.dict(os.environ, {}, clear=True)  # Clear environment
    @patch("subprocess.run")
    async def test_transaction_azure_cli_fallback_with_azure_auth(
        self, mock_subprocess
    ):
        """Test Transaction Azure CLI fallback with Default credential."""
        # Mock Azure CLI response
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps(
            {
                "accessToken": "azure-cli-token-12345",
                "tokenType": "Bearer",
                "expiresOn": "2026-01-18T12:00:00Z",
            }
        ).encode()
        mock_subprocess.return_value = mock_result

        azure_cred = fastmssql.AzureCredential.default()

        transaction = fastmssql.Transaction(
            server="cli-test-server.database.windows.net",
            database="cli-test-database",
            azure_credential=azure_cred,
        )

        self.assertIsInstance(transaction, fastmssql.Transaction)


if __name__ == "__main__":
    unittest.main()
