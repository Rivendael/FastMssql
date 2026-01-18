"""
Test runner specifically for Azure authentication tests.
Runs both basic and advanced Azure authentication test suites.
"""

import unittest

from test_azure_auth_advanced import (
    TestAzureAuthenticationAsync,
    TestAzureAuthenticationErrorScenarios,
    TestAzureAuthenticationIntegrationPatterns,
    TestAzureCredentialEdgeCases,
    TestAzureCredentialValidation,
)

# Import test modules
from test_azure_authentication import (
    TestAzureAuthenticationIntegration,
    TestAzureAuthMockingScenarios,
    TestAzureCredentials,
    TestAzureCredentialTypes,
    TestParameterValidation,
    TestRealWorldScenarios,
)


def run_all_azure_tests(verbosity=2):
    """Run all Azure authentication tests."""
    print("🔐 Running FastMSSQL Azure Authentication Test Suite")
    print("=" * 60)

    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Basic tests
    basic_test_classes = [
        TestAzureCredentials,
        TestAzureCredentialTypes,
        TestAzureAuthenticationIntegration,
        TestParameterValidation,
        TestAzureAuthMockingScenarios,
        TestRealWorldScenarios,
    ]

    # Advanced tests
    advanced_test_classes = [
        TestAzureAuthenticationAsync,
        TestAzureAuthenticationErrorScenarios,
        TestAzureCredentialEdgeCases,
        TestAzureAuthenticationIntegrationPatterns,
        TestAzureCredentialValidation,
    ]

    all_test_classes = basic_test_classes + advanced_test_classes

    # Load all tests
    for test_class in all_test_classes:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)

    print(
        f"📊 Loaded {suite.countTestCases()} test cases from {len(all_test_classes)} test classes"
    )
    print()

    # Run tests
    runner = unittest.TextTestRunner(verbosity=verbosity)
    result = runner.run(suite)

    # Print summary
    print()
    print("🎯 Test Summary")
    print("=" * 30)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped) if hasattr(result, 'skipped') else 0}")

    if result.wasSuccessful():
        print("✅ All Azure authentication tests passed!")
    else:
        print("❌ Some tests failed. Check output above for details.")

        # Print failure details
        if result.failures:
            print("\n💥 Failures:")
            for test, traceback in result.failures:
                print(f"  - {test}: {traceback.split('AssertionError:')[-1].strip()}")

        if result.errors:
            print("\n🚨 Errors:")
            for test, traceback in result.errors:
                print(
                    f"  - {test}: {traceback.split('\\n')[-2] if '\\n' in traceback else traceback}"
                )

    return result.wasSuccessful()


def run_basic_azure_tests():
    """Run only the basic Azure authentication tests."""
    print("🔐 Running Basic Azure Authentication Tests")
    print("=" * 50)

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    basic_test_classes = [
        TestAzureCredentials,
        TestAzureCredentialTypes,
        TestAzureAuthenticationIntegration,
    ]

    for test_class in basic_test_classes:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return result.wasSuccessful()


def run_advanced_azure_tests():
    """Run only the advanced Azure authentication tests."""
    print("🔐 Running Advanced Azure Authentication Tests")
    print("=" * 50)

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    advanced_test_classes = [
        TestAzureAuthenticationAsync,
        TestAzureAuthenticationErrorScenarios,
        TestAzureCredentialEdgeCases,
        TestAzureAuthenticationIntegrationPatterns,
        TestAzureCredentialValidation,
    ]

    for test_class in advanced_test_classes:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return result.wasSuccessful()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Run Azure authentication tests for FastMSSQL"
    )
    parser.add_argument(
        "--suite",
        choices=["all", "basic", "advanced"],
        default="all",
        help="Which test suite to run",
    )
    parser.add_argument(
        "--verbose", "-v", action="count", default=2, help="Increase verbosity level"
    )

    args = parser.parse_args()

    # Run appropriate test suite
    if args.suite == "basic":
        success = run_basic_azure_tests()
    elif args.suite == "advanced":
        success = run_advanced_azure_tests()
    else:  # 'all'
        success = run_all_azure_tests(verbosity=args.verbose)

    # Exit with appropriate code
    exit(0 if success else 1)
