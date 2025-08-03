#!/bin/bash

# Build script for mssql-python-rust
# This script builds the Rust extension and installs it in development mode

set -e

echo "Building mssql-python-rust..."

# Check if maturin is installed
if ! command -v maturin &> /dev/null; then
    echo "Installing maturin..."
    pip install maturin
fi

# Build the Rust extension in development mode
echo "Building Rust extension..."
maturin develop --release

echo "Build completed successfully!"
echo ""
echo "You can now import the library:"
echo "  import mssql_python_rust"
echo ""
echo "Or run the examples:"
echo "  python examples/basic_usage.py"
