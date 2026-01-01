
#!/usr/bin/env bash

# Check for Python
if ! command -v python &> /dev/null; then
    echo "[ERROR] Python is not installed or not in PATH. Please install Python before continuing."
    exit 1
fi

# Check for Rust
if ! command -v rustc &> /dev/null; then
    echo "[ERROR] Rust is not installed or not in PATH. Please install Rust from https://rustup.rs before continuing."
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
    echo "Creating Python virtual environment..."
    python -m venv .venv
else
    echo "Virtual environment already exists."
fi

# Activate virtual environment
echo "Activating virtual environment..."
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    # Windows (Git Bash/MSYS2)
    source .venv/Scripts/activate
else
    # Linux/macOS
    source .venv/bin/activate
fi

# Install Python requirements
echo "Installing Python dependencies from requirements.txt..."
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

# Build Rust extension with maturin
if ! command -v maturin &> /dev/null; then
    echo "[ERROR] maturin is not installed. Installing maturin..."
    python -m pip install maturin
fi

echo "Building Rust extension with maturin (release mode)..."
maturin develop --release

echo "Running unittest_setup.sh for completeness..."
if [ -f "unittest_setup.sh" ]; then
    bash unittest_setup.sh
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