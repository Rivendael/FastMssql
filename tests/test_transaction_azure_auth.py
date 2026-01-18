"""
Unit tests for Transaction Azure authentication functionality.
Tests Transaction credential creation, token acquisition, and error handling with mocks.
"""

import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import fastmssql


class TestTransactionAzureCredentials(unittest.TestCase):
    """Test Transaction with Azure credential creation and basic functionality."""

    def test_transaction_service_principal_creation(self):
        """Test Transaction creation with Service Principal credential."""
        azure_cred = fastmssql.AzureCredential.service_principal(
            client_id="test-client-id",
            client_secret="test-client-secret",
            tenant_id="test-tenant-id",
        )

        transaction = fastmssql.Transaction(
            server="test-server.database.windows.net",
            database="test-database",
            azure_credential=azure_cred,
        )

        self.assertIsInstance(transaction, fastmssql.Transaction)

    def test_transaction_managed_identity_creation(self):
        """Test Transaction creation with Managed Identity credential."""
        azure_cred = fastmssql.AzureCredential.managed_identity(
            client_id="test-client-id"
        )

        transaction = fastmssql.Transaction(
            server="test-server.database.windows.net",
            database="test-database",
            azure_credential=azure_cred,
        )

        self.assertIsInstance(transaction, fastmssql.Transaction)

    def test_transaction_access_token_creation(self):
        """Test Transaction creation with Access Token credential."""
        azure_cred = fastmssql.AzureCredential.access_token("test-access-token")

        transaction = fastmssql.Transaction(
            server="test-server.database.windows.net",
            database="test-database",
            azure_credential=azure_cred,
        )

        self.assertIsInstance(transaction, fastmssql.Transaction)

    def test_transaction_default_credential_creation(self):
        """Test Transaction creation with Default credential."""
        azure_cred = fastmssql.AzureCredential.default()

        transaction = fastmssql.Transaction(
            server="test-server.database.windows.net",
            database="test-database",
            azure_credential=azure_cred,
        )

        self.assertIsInstance(transaction, fastmssql.Transaction)

    def test_transaction_invalid_auth_combination(self):
        """Test Transaction creation fails with both username and Azure credential."""
        azure_cred = fastmssql.AzureCredential.service_principal(
            client_id="test-client-id",
            client_secret="test-client-secret",
            tenant_id="test-tenant-id",
        )

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

    def test_transaction_no_auth_fails(self):
        """Test Transaction creation fails with no authentication."""
        with self.assertRaises(ValueError) as context:
            fastmssql.Transaction(
                server="test-server.database.windows.net",
                database="test-database",
            )

        self.assertIn(
            "either username/password or azure_credential must be provided",
            str(context.exception),
        )


class TestTransactionAzureOperations(unittest.TestCase):
    """Test Transaction operations with Azure authentication using mocks."""

    def setUp(self):
        """Set up test fixtures."""
        self.azure_cred = fastmssql.AzureCredential.service_principal(
            client_id="test-client-id",
            client_secret="test-client-secret",
            tenant_id="test-tenant-id",
        )

    @patch("fastmssql.Transaction._ensure_connected", new_callable=AsyncMock)
    @patch("fastmssql.Transaction.__init__", return_value=None)
    async def test_transaction_query_with_azure_auth(
        self, mock_init, mock_ensure_connected
    ):
        """Test Transaction query execution with Azure authentication."""
        # Create mock Transaction instance
        transaction = fastmssql.Transaction.__new__(fastmssql.Transaction)
        transaction.azure_credential = self.azure_cred

        # Mock the query method to return a result
        mock_result = MagicMock()
        mock_result.rows.return_value = [{"test_column": "test_value"}]
        mock_result.has_rows.return_value = True

        with patch.object(transaction, "query", new_callable=AsyncMock) as mock_query:
            mock_query.return_value = mock_result

            result = await transaction.query(
                "SELECT @P1 as test_column", ["test_param"]
            )

            mock_query.assert_called_once_with(
                "SELECT @P1 as test_column", ["test_param"]
            )
            self.assertEqual(result.rows()[0]["test_column"], "test_value")

    @patch("fastmssql.Transaction._ensure_connected", new_callable=AsyncMock)
    @patch("fastmssql.Transaction.__init__", return_value=None)
    async def test_transaction_execute_with_azure_auth(
        self, mock_init, mock_ensure_connected
    ):
        """Test Transaction execute with Azure authentication."""
        # Create mock Transaction instance
        transaction = fastmssql.Transaction.__new__(fastmssql.Transaction)
        transaction.azure_credential = self.azure_cred

        with patch.object(
            transaction, "execute", new_callable=AsyncMock
        ) as mock_execute:
            mock_execute.return_value = 1  # Affected rows

            result = await transaction.execute(
                "INSERT INTO test_table VALUES (@P1)", ["test_value"]
            )

            mock_execute.assert_called_once_with(
                "INSERT INTO test_table VALUES (@P1)", ["test_value"]
            )
            self.assertEqual(result, 1)

    @patch("fastmssql.Transaction._ensure_connected", new_callable=AsyncMock)
    @patch("fastmssql.Transaction.__init__", return_value=None)
    async def test_transaction_begin_with_azure_auth(
        self, mock_init, mock_ensure_connected
    ):
        """Test Transaction begin with Azure authentication."""
        # Create mock Transaction instance
        transaction = fastmssql.Transaction.__new__(fastmssql.Transaction)
        transaction.azure_credential = self.azure_cred

        with patch.object(transaction, "begin", new_callable=AsyncMock) as mock_begin:
            await transaction.begin()
            mock_begin.assert_called_once()

    @patch("fastmssql.Transaction._ensure_connected", new_callable=AsyncMock)
    @patch("fastmssql.Transaction.__init__", return_value=None)
    async def test_transaction_commit_with_azure_auth(
        self, mock_init, mock_ensure_connected
    ):
        """Test Transaction commit with Azure authentication."""
        # Create mock Transaction instance
        transaction = fastmssql.Transaction.__new__(fastmssql.Transaction)
        transaction.azure_credential = self.azure_cred

        with patch.object(transaction, "commit", new_callable=AsyncMock) as mock_commit:
            await transaction.commit()
            mock_commit.assert_called_once()

    @patch("fastmssql.Transaction._ensure_connected", new_callable=AsyncMock)
    @patch("fastmssql.Transaction.__init__", return_value=None)
    async def test_transaction_rollback_with_azure_auth(
        self, mock_init, mock_ensure_connected
    ):
        """Test Transaction rollback with Azure authentication."""
        # Create mock Transaction instance
        transaction = fastmssql.Transaction.__new__(fastmssql.Transaction)
        transaction.azure_credential = self.azure_cred

        with patch.object(
            transaction, "rollback", new_callable=AsyncMock
        ) as mock_rollback:
            await transaction.rollback()
            mock_rollback.assert_called_once()

    @patch("fastmssql.Transaction._ensure_connected", new_callable=AsyncMock)
    @patch("fastmssql.Transaction.__init__", return_value=None)
    async def test_transaction_execute_batch_with_azure_auth(
        self, mock_init, mock_ensure_connected
    ):
        """Test Transaction execute_batch with Azure authentication."""
        # Create mock Transaction instance
        transaction = fastmssql.Transaction.__new__(fastmssql.Transaction)
        transaction.azure_credential = self.azure_cred

        batch_commands = [
            "INSERT INTO test_table VALUES ('value1')",
            "INSERT INTO test_table VALUES ('value2')",
        ]

        with patch.object(
            transaction, "execute_batch", new_callable=AsyncMock
        ) as mock_execute_batch:
            mock_execute_batch.return_value = [1, 1]  # Affected rows for each command

            result = await transaction.execute_batch(batch_commands)

            mock_execute_batch.assert_called_once_with(batch_commands)
            self.assertEqual(result, [1, 1])

    @patch("fastmssql.Transaction._ensure_connected", new_callable=AsyncMock)
    @patch("fastmssql.Transaction.__init__", return_value=None)
    async def test_transaction_query_batch_with_azure_auth(
        self, mock_init, mock_ensure_connected
    ):
        """Test Transaction query_batch with Azure authentication."""
        # Create mock Transaction instance
        transaction = fastmssql.Transaction.__new__(fastmssql.Transaction)
        transaction.azure_credential = self.azure_cred

        batch_queries = [
            "SELECT 'test1' as value",
            "SELECT 'test2' as value",
        ]

        # Mock results for each query
        mock_result1 = MagicMock()
        mock_result1.rows.return_value = [{"value": "test1"}]
        mock_result2 = MagicMock()
        mock_result2.rows.return_value = [{"value": "test2"}]

        with patch.object(
            transaction, "query_batch", new_callable=AsyncMock
        ) as mock_query_batch:
            mock_query_batch.return_value = [mock_result1, mock_result2]

            results = await transaction.query_batch(batch_queries)

            mock_query_batch.assert_called_once_with(batch_queries)
            self.assertEqual(len(results), 2)
            self.assertEqual(results[0].rows()[0]["value"], "test1")
            self.assertEqual(results[1].rows()[0]["value"], "test2")


class TestTransactionAzureErrorHandling(unittest.TestCase):
    """Test Transaction error handling with Azure authentication."""

    def setUp(self):
        """Set up test fixtures."""
        self.azure_cred = fastmssql.AzureCredential.service_principal(
            client_id="test-client-id",
            client_secret="test-client-secret",
            tenant_id="test-tenant-id",
        )

    @patch("fastmssql.Transaction._ensure_connected", new_callable=AsyncMock)
    @patch("fastmssql.Transaction.__init__", return_value=None)
    async def test_transaction_query_error_handling(
        self, mock_init, mock_ensure_connected
    ):
        """Test Transaction query error handling with Azure authentication."""
        # Create mock Transaction instance
        transaction = fastmssql.Transaction.__new__(fastmssql.Transaction)
        transaction.azure_credential = self.azure_cred

        with patch.object(transaction, "query", new_callable=AsyncMock) as mock_query:
            # Simulate a database error
            mock_query.side_effect = RuntimeError("Database connection failed")

            with self.assertRaises(RuntimeError) as context:
                await transaction.query("SELECT 1")

            self.assertIn("Database connection failed", str(context.exception))

    @patch("fastmssql.Transaction._ensure_connected", new_callable=AsyncMock)
    @patch("fastmssql.Transaction.__init__", return_value=None)
    async def test_transaction_execute_error_handling(
        self, mock_init, mock_ensure_connected
    ):
        """Test Transaction execute error handling with Azure authentication."""
        # Create mock Transaction instance
        transaction = fastmssql.Transaction.__new__(fastmssql.Transaction)
        transaction.azure_credential = self.azure_cred

        with patch.object(
            transaction, "execute", new_callable=AsyncMock
        ) as mock_execute:
            # Simulate a database error
            mock_execute.side_effect = RuntimeError("SQL execution failed")

            with self.assertRaises(RuntimeError) as context:
                await transaction.execute("INSERT INTO non_existent_table VALUES (1)")

            self.assertIn("SQL execution failed", str(context.exception))

    @patch("fastmssql.Transaction._ensure_connected", new_callable=AsyncMock)
    @patch("fastmssql.Transaction.__init__", return_value=None)
    async def test_transaction_begin_error_handling(
        self, mock_init, mock_ensure_connected
    ):
        """Test Transaction begin error handling with Azure authentication."""
        # Create mock Transaction instance
        transaction = fastmssql.Transaction.__new__(fastmssql.Transaction)
        transaction.azure_credential = self.azure_cred

        with patch.object(transaction, "begin", new_callable=AsyncMock) as mock_begin:
            # Simulate a transaction error
            mock_begin.side_effect = RuntimeError("Failed to begin transaction")

            with self.assertRaises(RuntimeError) as context:
                await transaction.begin()

            self.assertIn("Failed to begin transaction", str(context.exception))

    @patch("fastmssql.Transaction._ensure_connected", new_callable=AsyncMock)
    @patch("fastmssql.Transaction.__init__", return_value=None)
    async def test_transaction_commit_error_handling(
        self, mock_init, mock_ensure_connected
    ):
        """Test Transaction commit error handling with Azure authentication."""
        # Create mock Transaction instance
        transaction = fastmssql.Transaction.__new__(fastmssql.Transaction)
        transaction.azure_credential = self.azure_cred

        with patch.object(transaction, "commit", new_callable=AsyncMock) as mock_commit:
            # Simulate a commit error
            mock_commit.side_effect = RuntimeError("Failed to commit transaction")

            with self.assertRaises(RuntimeError) as context:
                await transaction.commit()

            self.assertIn("Failed to commit transaction", str(context.exception))

    @patch("fastmssql.Transaction._ensure_connected", new_callable=AsyncMock)
    @patch("fastmssql.Transaction.__init__", return_value=None)
    async def test_transaction_rollback_error_handling(
        self, mock_init, mock_ensure_connected
    ):
        """Test Transaction rollback error handling with Azure authentication."""
        # Create mock Transaction instance
        transaction = fastmssql.Transaction.__new__(fastmssql.Transaction)
        transaction.azure_credential = self.azure_cred

        with patch.object(
            transaction, "rollback", new_callable=AsyncMock
        ) as mock_rollback:
            # Simulate a rollback error
            mock_rollback.side_effect = RuntimeError("Failed to rollback transaction")

            with self.assertRaises(RuntimeError) as context:
                await transaction.rollback()

            self.assertIn("Failed to rollback transaction", str(context.exception))


class TestTransactionAzureTokenAcquisition(unittest.TestCase):
    """Test Transaction Azure token acquisition scenarios."""

    @patch("requests.post")
    async def test_transaction_service_principal_token_acquisition(self, mock_post):
        """Test Transaction with Service Principal token acquisition."""
        # Mock successful token response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "mock-access-token",
            "token_type": "Bearer",
            "expires_in": 3600,
        }
        mock_post.return_value = mock_response

        azure_cred = fastmssql.AzureCredential.service_principal(
            client_id="test-client-id",
            client_secret="test-client-secret",
            tenant_id="test-tenant-id",
        )

        transaction = fastmssql.Transaction(
            server="test-server.database.windows.net",
            database="test-database",
            azure_credential=azure_cred,
        )

        # Just creating the transaction should succeed
        self.assertIsInstance(transaction, fastmssql.Transaction)

    @patch("requests.get")
    async def test_transaction_managed_identity_token_acquisition(self, mock_get):
        """Test Transaction with Managed Identity token acquisition."""
        # Mock successful IMDS response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "mock-imds-token",
            "token_type": "Bearer",
            "expires_in": 3600,
        }
        mock_get.return_value = mock_response

        azure_cred = fastmssql.AzureCredential.managed_identity(
            client_id="test-client-id"
        )

        transaction = fastmssql.Transaction(
            server="test-server.database.windows.net",
            database="test-database",
            azure_credential=azure_cred,
        )

        # Just creating the transaction should succeed
        self.assertIsInstance(transaction, fastmssql.Transaction)

    async def test_transaction_access_token_direct(self):
        """Test Transaction with direct access token."""
        azure_cred = fastmssql.AzureCredential.access_token("direct-access-token")

        transaction = fastmssql.Transaction(
            server="test-server.database.windows.net",
            database="test-database",
            azure_credential=azure_cred,
        )

        # Just creating the transaction should succeed
        self.assertIsInstance(transaction, fastmssql.Transaction)

    @patch.dict(
        os.environ,
        {
            "AZURE_CLIENT_ID": "env-client-id",
            "AZURE_CLIENT_SECRET": "env-client-secret",
            "AZURE_TENANT_ID": "env-tenant-id",
        },
    )
    @patch("requests.post")
    async def test_transaction_default_azure_env_vars(self, mock_post):
        """Test Transaction with Default Azure credential using environment variables."""
        # Mock successful token response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "env-var-token",
            "token_type": "Bearer",
            "expires_in": 3600,
        }
        mock_post.return_value = mock_response

        azure_cred = fastmssql.AzureCredential.default()

        transaction = fastmssql.Transaction(
            server="test-server.database.windows.net",
            database="test-database",
            azure_credential=azure_cred,
        )

        # Just creating the transaction should succeed
        self.assertIsInstance(transaction, fastmssql.Transaction)


if __name__ == "__main__":
    unittest.main()
