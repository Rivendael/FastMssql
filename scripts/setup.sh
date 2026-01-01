#!/bin/bash

# Check for UV, which will be used to configure Python, the venv, and dependencies.
if ! command -v uv &>/dev/null; then
    echo "[ERROR] $(uv) is not installed or not in PATH. Please install $(uv) before continuing."
    exit 1
fi

# Check for Rust
if ! command -v rustc &>/dev/null; then
    echo "[ERROR] Rust is not installed or not in PATH. Please install Rust from https://rustup.rs before continuing."
    exit 1
fi

# Use UV to create and sync the virtual env.
uv sync

echo "Building Rust extension with maturin (release mode)..."
uv run maturin develop --release

echo "Running unittest_setup.sh for completeness..."
if [ -f "./scripts/unittest_setup.sh" ]; then
    bash ./scripts/unittest_setup.sh
else
    echo "[WARNING] unittest_setup.sh not found. Skipping unit test setup."
fi

echo "Setup complete."
echo ""
echo "To activate the virtual environment in the future, run:"
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    echo "  source .venv/Scripts/activate"
else
    echo "  source .venv/bin/activate"
fi
echo "Alternatively (Preferred) use \`uv run <entry.py>\` to run scripts within the virtual environment."
