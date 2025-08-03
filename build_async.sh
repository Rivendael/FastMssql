#!/bin/bash

# Build script for mssql-python-rust with async support

echo "Building mssql-python-rust with async support..."

# Check if maturin is installed
if ! command -v maturin &> /dev/null; then
    echo "maturin not found. Installing..."
    pip install maturin
fi

# Clean previous builds
echo "Cleaning previous builds..."
rm -rf target/wheels/
rm -rf dist/

# Build in development mode
echo "Building in development mode..."
maturin develop

# Check if build was successful
if [ $? -eq 0 ]; then
    echo "Build successful!"
    echo ""
    echo "You can now run the examples:"
    echo "  python examples/basic_usage.py"
    echo "  python examples/async_usage.py"
    echo "  python examples/mixed_usage.py"
    echo ""
    echo "To test async functionality:"
    echo "  python -c \"import asyncio; import mssql_python_rust as mssql; print('Async support:', hasattr(mssql, 'connect_async'))\""
else
    echo "Build failed!"
    echo "Make sure you have:"
    echo "1. Rust toolchain installed"
    echo "2. Python development headers"
    echo "3. All dependencies in Cargo.toml"
    exit 1
fi
