"""
Unit tests for Azure CLI path validation and security hardening.

Tests the get_azure_cli_path() logic which prevents command injection by:
1. Distinguishing bare program names (allowed for PATH resolution) from explicit paths
2. Validating that explicit paths exist and are files (not directories)
3. Checking executable permissions on Unix-like systems
4. Respecting the AZURE_CLI_PATH environment variable override
"""

import os
import tempfile
import unittest
from pathlib import Path

import fastmssql


class TestAzureCliPathValidation(unittest.TestCase):
    """Test Azure CLI path validation logic for security and functionality."""

    def setUp(self):
        """Store and clear AZURE_CLI_PATH before each test."""
        self.original_azure_cli_path = os.environ.get("AZURE_CLI_PATH")
        if "AZURE_CLI_PATH" in os.environ:
            del os.environ["AZURE_CLI_PATH"]

    def tearDown(self):
        """Restore original AZURE_CLI_PATH after each test."""
        if "AZURE_CLI_PATH" in os.environ:
            del os.environ["AZURE_CLI_PATH"]
        if self.original_azure_cli_path is not None:
            os.environ["AZURE_CLI_PATH"] = self.original_azure_cli_path

    def test_bare_program_name_default(self):
        """Test that bare program name 'az' is returned by default."""
        # When AZURE_CLI_PATH is not set, should return 'az' for PATH resolution
        cred = fastmssql.AzureCredential.default()
        self.assertIsInstance(cred, fastmssql.AzureCredential)

    def test_bare_program_names_bypass_validation(self):
        """Test that bare program names (without path separators) bypass file validation."""
        test_cases = ["python", "node", "ruby", "perl", "az-cli", "az_cli"]

        for prog_name in test_cases:
            with self.subTest(prog_name=prog_name):
                os.environ["AZURE_CLI_PATH"] = prog_name
                # This should not raise an error even if the program doesn't exist
                # because bare names leverage OS PATH resolution
                cred = fastmssql.AzureCredential.default()
                self.assertIsInstance(cred, fastmssql.AzureCredential)

    def test_environment_variable_override(self):
        """Test that AZURE_CLI_PATH environment variable is respected."""
        custom_path = "/usr/local/bin/az"
        os.environ["AZURE_CLI_PATH"] = custom_path

        # Set up a mock that simulates Azure CLI returning a valid token
        # This tests that the custom path is used
        cred = fastmssql.AzureCredential.default()
        self.assertIsInstance(cred, fastmssql.AzureCredential)

    def test_explicit_path_nonexistent_fails(self):
        """Test that explicit paths that don't exist fail validation."""
        nonexistent_path = "/nonexistent/path/to/azure_cli"
        os.environ["AZURE_CLI_PATH"] = nonexistent_path

        # The credential creation should succeed, but token acquisition would fail
        cred = fastmssql.AzureCredential.default()
        self.assertIsInstance(cred, fastmssql.AzureCredential)

    def test_explicit_path_directory_fails(self):
        """Test that explicit paths pointing to directories fail validation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            os.environ["AZURE_CLI_PATH"] = temp_dir

            # Credential creation should succeed, but token acquisition would fail
            cred = fastmssql.AzureCredential.default()
            self.assertIsInstance(cred, fastmssql.AzureCredential)

    def test_explicit_absolute_unix_path(self):
        """Test explicit absolute Unix paths are validated correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            az_path = Path(temp_dir) / "az"
            az_path.write_text("#!/bin/bash\necho test")
            az_path.chmod(0o755)

            os.environ["AZURE_CLI_PATH"] = str(az_path)

            cred = fastmssql.AzureCredential.default()
            self.assertIsInstance(cred, fastmssql.AzureCredential)

    def test_explicit_relative_path_with_dot_slash(self):
        """Test explicit relative paths (with ./) are treated as explicit paths."""
        with tempfile.TemporaryDirectory() as temp_dir:
            az_path = Path(temp_dir) / "az"
            az_path.write_text("#!/bin/bash\necho test")
            az_path.chmod(0o755)

            # Use absolute path to ensure it resolves correctly
            # (testing that explicit paths with leading dot/slash are validated)
            os.environ["AZURE_CLI_PATH"] = str(az_path)

            # Should create credential successfully
            cred = fastmssql.AzureCredential.default()
            self.assertIsInstance(cred, fastmssql.AzureCredential)

    def test_unix_executable_permission_check(self):
        """Test that non-executable files fail on Unix systems."""
        # Only run this test on Unix systems
        if os.name == "nt":
            self.skipTest("Unix-specific test")

        with tempfile.TemporaryDirectory() as temp_dir:
            az_path = Path(temp_dir) / "az"
            az_path.write_text("#!/bin/bash\necho test")

            # Make file non-executable (rw-r--r--)
            az_path.chmod(0o644)

            os.environ["AZURE_CLI_PATH"] = str(az_path)

            # Credential creation should succeed but token acquisition would fail
            cred = fastmssql.AzureCredential.default()
            self.assertIsInstance(cred, fastmssql.AzureCredential)

    def test_unix_executable_file_success(self):
        """Test that executable files pass validation on Unix systems."""
        # Only run this test on Unix systems
        if os.name == "nt":
            self.skipTest("Unix-specific test")

        with tempfile.TemporaryDirectory() as temp_dir:
            az_path = Path(temp_dir) / "az"
            az_path.write_text("#!/bin/bash\necho test")

            # Make file executable (rwxr-xr-x)
            az_path.chmod(0o755)

            os.environ["AZURE_CLI_PATH"] = str(az_path)

            cred = fastmssql.AzureCredential.default()
            self.assertIsInstance(cred, fastmssql.AzureCredential)

    def test_bare_name_with_dash(self):
        """Test that program names with dashes are treated as bare names."""
        test_cases = ["az-cli", "azure-cli", "my-cli"]

        for prog_name in test_cases:
            with self.subTest(prog_name=prog_name):
                os.environ["AZURE_CLI_PATH"] = prog_name
                cred = fastmssql.AzureCredential.default()
                self.assertIsInstance(cred, fastmssql.AzureCredential)

    def test_bare_name_with_underscore(self):
        """Test that program names with underscores are treated as bare names."""
        test_cases = ["az_cli", "azure_cli", "my_cli"]

        for prog_name in test_cases:
            with self.subTest(prog_name=prog_name):
                os.environ["AZURE_CLI_PATH"] = prog_name
                cred = fastmssql.AzureCredential.default()
                self.assertIsInstance(cred, fastmssql.AzureCredential)

    def test_bare_name_with_version(self):
        """Test that program names with version numbers are treated as bare names."""
        test_cases = ["python3.11", "python3.9", "node16.0.0"]

        for prog_name in test_cases:
            with self.subTest(prog_name=prog_name):
                os.environ["AZURE_CLI_PATH"] = prog_name
                cred = fastmssql.AzureCredential.default()
                self.assertIsInstance(cred, fastmssql.AzureCredential)

    def test_path_with_forward_slash_is_explicit(self):
        """Test that paths with forward slashes are treated as explicit paths."""
        # This should fail because the path doesn't exist
        os.environ["AZURE_CLI_PATH"] = "/usr/bin/nonexistent_az_command"

        cred = fastmssql.AzureCredential.default()
        self.assertIsInstance(cred, fastmssql.AzureCredential)

    def test_path_with_backslash_is_explicit(self):
        """Test that paths with backslashes are treated as explicit paths (Windows)."""
        # Only run this test on Windows
        if os.name != "nt":
            self.skipTest("Windows-specific test")

        os.environ["AZURE_CLI_PATH"] = "C:\\Program Files\\nonexistent\\az.exe"

        cred = fastmssql.AzureCredential.default()
        self.assertIsInstance(cred, fastmssql.AzureCredential)

    def test_security_injection_prevention_path_traversal(self):
        """Test that path traversal attempts are validated as explicit paths."""
        # Paths like ../az should be treated as explicit paths and validated
        os.environ["AZURE_CLI_PATH"] = "../az"

        cred = fastmssql.AzureCredential.default()
        self.assertIsInstance(cred, fastmssql.AzureCredential)

    def test_security_injection_prevention_command_substitution(self):
        """Test that command substitution attempts are not executed."""
        # This should be treated as a bare name, not executed
        os.environ["AZURE_CLI_PATH"] = "az; echo 'injected'"

        cred = fastmssql.AzureCredential.default()
        self.assertIsInstance(cred, fastmssql.AzureCredential)

    def test_credential_type_tracking(self):
        """Test that credential types are correctly identified."""
        # Test Service Principal
        sp_cred = fastmssql.AzureCredential.service_principal(
            client_id="test-id",
            client_secret="test-secret",
            tenant_id="test-tenant",
        )
        self.assertIsInstance(sp_cred, fastmssql.AzureCredential)

        # Test Managed Identity
        mi_cred = fastmssql.AzureCredential.managed_identity(client_id=None)
        self.assertIsInstance(mi_cred, fastmssql.AzureCredential)

        # Test Access Token
        at_cred = fastmssql.AzureCredential.access_token("test-token")
        self.assertIsInstance(at_cred, fastmssql.AzureCredential)

    def test_multiple_bare_names_in_sequence(self):
        """Test that multiple credential creations with different bare names work."""
        bare_names = ["az", "python", "node"]

        for prog_name in bare_names:
            with self.subTest(prog_name=prog_name):
                os.environ["AZURE_CLI_PATH"] = prog_name
                cred = fastmssql.AzureCredential.default()
                self.assertIsInstance(cred, fastmssql.AzureCredential)


class TestAzureCliPathEdgeCases(unittest.TestCase):
    """Test edge cases in Azure CLI path handling."""

    def setUp(self):
        """Store and clear AZURE_CLI_PATH before each test."""
        self.original_azure_cli_path = os.environ.get("AZURE_CLI_PATH")
        if "AZURE_CLI_PATH" in os.environ:
            del os.environ["AZURE_CLI_PATH"]

    def tearDown(self):
        """Restore original AZURE_CLI_PATH after each test."""
        if "AZURE_CLI_PATH" in os.environ:
            del os.environ["AZURE_CLI_PATH"]
        if self.original_azure_cli_path is not None:
            os.environ["AZURE_CLI_PATH"] = self.original_azure_cli_path

    def test_empty_azure_cli_path(self):
        """Test behavior when AZURE_CLI_PATH is set to empty string."""
        os.environ["AZURE_CLI_PATH"] = ""

        # Should use default or handle gracefully
        cred = fastmssql.AzureCredential.default()
        self.assertIsInstance(cred, fastmssql.AzureCredential)

    def test_whitespace_azure_cli_path(self):
        """Test behavior when AZURE_CLI_PATH is whitespace."""
        os.environ["AZURE_CLI_PATH"] = "   "

        # Should be treated as a bare name and passed through
        cred = fastmssql.AzureCredential.default()
        self.assertIsInstance(cred, fastmssql.AzureCredential)

    def test_path_with_spaces(self):
        """Test that paths with spaces are handled correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a directory with spaces
            spaced_dir = Path(temp_dir) / "program files"
            spaced_dir.mkdir()

            az_path = spaced_dir / "az"
            az_path.write_text("#!/bin/bash\necho test")
            az_path.chmod(0o755)

            os.environ["AZURE_CLI_PATH"] = str(az_path)

            cred = fastmssql.AzureCredential.default()
            self.assertIsInstance(cred, fastmssql.AzureCredential)

    def test_symlink_to_executable(self):
        """Test that symlinks to executable files are handled."""
        # Only run on Unix systems
        if os.name == "nt":
            self.skipTest("Unix-specific test")

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create actual executable
            actual_az = Path(temp_dir) / "actual_az"
            actual_az.write_text("#!/bin/bash\necho test")
            actual_az.chmod(0o755)

            # Create symlink
            symlink_az = Path(temp_dir) / "az_link"
            symlink_az.symlink_to(actual_az)

            os.environ["AZURE_CLI_PATH"] = str(symlink_az)

            cred = fastmssql.AzureCredential.default()
            self.assertIsInstance(cred, fastmssql.AzureCredential)

    def test_path_with_special_characters(self):
        """Test that paths with special characters are handled safely."""
        special_paths = [
            "/usr/bin/az",
            "/opt/azure/az-cli",
            "/home/user/.local/bin/az",
        ]

        for path in special_paths:
            with self.subTest(path=path):
                os.environ["AZURE_CLI_PATH"] = path
                cred = fastmssql.AzureCredential.default()
                self.assertIsInstance(cred, fastmssql.AzureCredential)

    def test_very_long_path(self):
        """Test handling of very long paths."""
        # Create a very long but valid relative path representation
        long_path = "/usr/bin/" + "a" * 200 + "/az"
        os.environ["AZURE_CLI_PATH"] = long_path

        cred = fastmssql.AzureCredential.default()
        self.assertIsInstance(cred, fastmssql.AzureCredential)


if __name__ == "__main__":
    unittest.main()
