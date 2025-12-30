# Getting Started: Developing FastMSSQL

This guide walks you through setting up your local development environment to build, test, and contribute to FastMSSQL—an async Python library for Microsoft SQL Server, built with Rust and PyO3.

## Prerequisites

You'll need to have the following tools installed:

- **Python** (3.10+) — Check with `python --version`
- **Rust** — Install from [https://rustup.rs](https://rustup.rs)
- **Docker** — For running Microsoft SQL Server locally. Install from [https://www.docker.com](https://www.docker.com)

## Quick Start: One-Command Setup

Run the setup script to configure everything automatically:

```bash
bash setup.sh
```

This will:
1. Check for Python and Rust
2. Create and activate a Python virtual environment
3. Install Python dependencies from `requirements.txt`
4. Install maturin (if needed)
5. Build the Rust extension in release mode
6. Run test database setup

**Done!** Your development environment is ready.

## Manual Setup (Step by Step)

If you prefer to set up manually or encounter issues, follow these steps:

### 1. Install System Dependencies

#### macOS & Linux
```bash
# Install Rust (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"

# Verify Rust is installed
rustc --version
```

#### Windows (Git Bash/MSYS2)
```bash
# Download and run the Rust installer from https://rustup.rs
# Or use winget:
winget install Rustlang.Rust.MSVC
```

### 2. Create a Python Virtual Environment

```bash
# Create the virtual environment
python -m venv .venv

# Activate it
# On macOS/Linux:
source .venv/bin/activate

# On Windows (Git Bash):
source .venv/Scripts/activate
```

### 3. Install Python Dependencies

```bash
# Upgrade pip
python -m pip install --upgrade pip

# Install requirements
pip install -r requirements.txt
```

### 4. Build the Rust Extension with Maturin

```bash
# Install maturin if you don't have it
pip install maturin

# Build in release mode for performance
maturin develop --release
```

**Note:** The first build may take a few minutes as Rust compiles all dependencies.

### 5. Set Up the Test Database

The library includes a test database setup script that pulls and runs the official Microsoft SQL Server Docker image:

```bash
bash unittest_setup.sh
```

This script will:
- Check that Docker is installed
- Pull the latest Microsoft SQL Server 2022 Express image
- Start a SQL Server container on port 1433
- Create a sample environment file with the connection string

#### What's Happening Under the Hood

The script runs the following Docker command:

```bash
docker run \
  -e "ACCEPT_EULA=Y" \
  -e "MSSQL_SA_PASSWORD=StrongPassword123!" \
  -p 1433:1433 \
  --name sqlserver \
  -d mcr.microsoft.com/mssql/server:2022-latest
```

This pulls the official Microsoft SQL Server image from `mcr.microsoft.com` and starts a containerized SQL Server instance with:
- **Port**: 1433 (standard SQL Server port, exposed on localhost)
- **SA Password**: `StrongPassword123!`
- **Edition**: Express (free, fully-featured for development)
- **Detached Mode**: `-d` runs it in the background

#### Manual Docker Setup (Alternative)

If you prefer to start the container manually:

```bash
# Pull the latest SQL Server 2022 image
docker pull mcr.microsoft.com/mssql/server:2022-latest

# Run the container
docker run \
  -e "ACCEPT_EULA=Y" \
  -e "MSSQL_SA_PASSWORD=StrongPassword123!" \
  -p 1433:1433 \
  --name sqlserver \
  -d mcr.microsoft.com/mssql/server:2022-latest

# Wait a few seconds for the container to start
sleep 10

# Verify it's running
docker ps | grep sqlserver
```

#### Verify SQL Server is Running

```bash
# Check the container is up
docker ps

# Test the connection (if you have sqlcmd installed)
sqlcmd -S localhost,1433 -U SA -P "StrongPassword123!" -Q "SELECT 1"
```

#### Stop or Remove the Container

```bash
# Stop the container
docker stop sqlserver

# Remove the container completely
docker rm sqlserver
```

### 6. Configure the Environment

Create a `.env` file in the project root with your SQL Server connection string:

```bash
# .env
FASTMSSQL_TEST_CONNECTION_STRING="Server=localhost,1433;Database=master;User Id=SA;Password=StrongPassword123!;TrustServerCertificate=yes"
```

Or if using different credentials, adjust the connection string accordingly.

## Running Tests

Once your environment is set up, run the test suite:

```bash
# Activate your virtual environment first
source .venv/bin/activate  # macOS/Linux
# or
source .venv/Scripts/activate  # Windows Git Bash

# Run all tests
pytest tests/

# Run a specific test file
pytest tests/test_basic.py

# Run tests with verbose output
pytest tests/ -v

# Run tests with coverage
pytest tests/ --cov=fastmssql
```

**Important:** Make sure your SQL Server Docker container is running before you run tests.

## Development Workflow

### Edit Rust Code and Rebuild

When you modify Rust code in `src/`, rebuild the extension:

```bash
maturin develop --release
```

This recompiles the Rust code and reinstalls the Python package in development mode.

### Run Benchmarks

The project includes performance benchmarks in the `benchmarks/` folder:

```bash
# Run a simple load test
python benchmarks/simple_load_test.py

# Run memory profiling
python benchmarks/memory_benchmark.py

# Run profiling
python benchmarks/profile_test.py
```

### Type Checking

The project includes a `fastmssql.pyi` stub file for type hints. Ensure your IDE recognizes it for better autocomplete and type checking.

## Project Structure

```
FastMssql/
├── python/              # Python package source
│   └── fastmssql/
├── src/                 # Rust source code
│   ├── lib.rs           # Main entry point
│   ├── pool_manager.rs  # Connection pooling
│   ├── connection.rs    # SQL Server connection logic
│   ├── batch.rs         # Batch operations
│   └── ...
├── tests/               # Python test suite
├── benchmarks/          # Performance benchmarks
├── Cargo.toml           # Rust project manifest
├── pyproject.toml       # Python project metadata
├── setup.sh             # Automated setup script
├── unittest_setup.sh    # Test database setup
└── requirements.txt     # Python dependencies
```

## Troubleshooting

### "Docker is not installed"

Install Docker from [https://www.docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop)

### "SQL Server failed to start"

Check Docker logs:
```bash
docker logs sqlserver
```

Common issues:
- Not enough disk space
- Port 1433 already in use by another application
- Insufficient system memory

### "maturin: command not found"

Ensure maturin is installed:
```bash
pip install maturin
```

### Tests fail with "connection refused"

Make sure:
1. SQL Server container is running: `docker ps | grep sqlserver`
2. `.env` file has the correct connection string
3. Container has fully started (may take 10-15 seconds)

Wait and retry:
```bash
sleep 10
pytest tests/
```

### Rebuild everything from scratch

```bash
# Clean Rust build artifacts
cargo clean

# Remove virtual environment
rm -rf .venv

# Run setup script again
bash setup.sh
```

## CI/CD Reference

The project uses GitHub Actions for continuous integration. The workflow in `.github/workflows/unittests.yml`:
- Tests against Python 3.10, 3.11, 3.12, 3.13, and 3.14
- Spins up a SQL Server 2022 Express container using the same image as above
- Installs dependencies and builds with maturin
- Runs the full test suite

You can mirror this locally by following the same steps in this guide.

## Next Steps

- **Read the [README.md](README.md)** for API documentation and usage examples
- **Explore the examples/** folder for sample applications
- **Check the test files** in `tests/` to understand how the library works
- **Review [Cargo.toml](Cargo.toml)** for Rust dependencies

Happy developing! 🚀
