# Getting Started: Developing FastMSSQL

This guide walks you through setting up your local development environment to
build, test, and contribute to FastMSSQL—an async Python library for Microsoft
SQL Server, built with Rust and PyO3.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Prerequisites: Quick Setup](#prerequisites-quick-setup)
- [Quick Start: One-Command Setup](#quick-start-one-command-setup)
- [Manual Setup (Step by Step)](#manual-setup-step-by-step)
- [Running Tests](#running-tests)
- [Development Workflow](#development-workflow)
- [Project Structure](#project-structure)
- [Troubleshooting](#troubleshooting)
- [CI/CD Reference](#cicd-reference)
- [Next Steps](#next-steps)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Prerequisites: Quick Setup

You'll need to have the following tools installed:

- [**uv**](https://docs.astral.sh/uv/): Project manager for Python that is built
    in Rust. Very fast, very capable. Manages Virtual Environments and Python
    versions for you.
- **Rust** — Install from [https://rustup.rs](https://rustup.rs)
- **Docker** — For running Microsoft SQL Server locally. Install from
    [https://www.docker.com](https://www.docker.com)

## Quick Start: One-Command Setup

From repo root, run the setup script to configure everything automatically:

```bash
./scripts/setup.sh
```

This will:

1. Check for UV and Rust
2. Run a `uv sync`, which will create a Python virtual environment and install
     dependancies.
3. Build the Rust extension in release mode
4. Run test database setup

**Done!** Your development environment is ready.

## Manual Setup (Step by Step)

If you prefer to set up manually or encounter issues, follow these steps:

### 1. Install System Dependencies

#### macOS & Linux

```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install Rust (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"

# Verify Rust is installed
rustc --version
```

You may also need to ensure that GCC is avialable. On a Debian system,

```bash
sudo apt-get install build-essentials
```

> You may need to search for your architecture.

#### Windows (Git Bash/MSYS2)

```pwsh
#Download and install the uv installer from
# Or use Invoke-RestMethod:
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# Download and run the Rust installer from https://rustup.rs
# Or use winget:
winget install Rustlang.Rust.MSVC
```

### 2. Configure Python environment

To prepare your python environment, run

```bash
uv sync
```

This will:

- Configure an isolated instance of Python
- Create a virtualenv as `.venv`
- Install packages from the `pyproject.toml` to that virtual environment

### 3. Build the Rust Extension with Maturin

```bash
# Build in release mode for performance, using the `uv` installed instance
uv run maturin develop --release
```

**Note:** The first build may take a few minutes as Rust compiles all
dependencies.

### 4. Set Up the Test Database

The library includes a test database setup script that pulls and runs the
official Microsoft SQL Server Docker image:

```bash
bash ./scripts/unittest_setup.sh
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

> ⚠️: Depending on your terminal, you may need to escape the `!` character: `\!`

This pulls the official Microsoft SQL Server image from `mcr.microsoft.com` and
starts a containerized SQL Server instance with:

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
docker run --rm \
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

> ⚠️: Depending on your terminal, you may need to escape the `!` character: `\!`

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
```

> ℹ️: If you used the command above, the container will auto-remove after being
> stopped.

### 5. Configure the Environment

Create a `.env` file in the project root, and populate it with the following
contents:

```bash
# .env
FASTMSSQL_TEST_CONNECTION_STRING="Server=localhost,1433;Database=master;User Id=SA;Password=StrongPassword123!;TrustServerCertificate=yes"
FAST_MSSQL_TEST_DB_USER="SA"
FAST_MSSQL_TEST_DB_PASSWORD="StrongPassword123!"
FAST_MSSQL_TEST_SERVER="localhost"
FAST_MSSQL_TEST_PORT="1433"
FAST_MSSQL_TEST_DATABASE="master"
```

Or if using different credentials, adjust the variables accordingly.

## Running Tests

Once your environment is set up, run the test suite:

```bash
# Run all tests
uv run pytest tests/

# Run a specific test file
uv run pytest tests/test_basic.py

# Run tests with verbose output
uv run pytest tests/ -v

# Run tests with coverage
uv run pytest tests/ --cov=fastmssql

# Run tests with multiple worker threads
uv run pytest tests/ -n {auto|#}
```

**Important:** Make sure your SQL Server Docker container is running before you
run tests.

> ℹ️: Please note that using multiple workers may interfere with test
> performance. If oyu experience problems, adjust your call accordingly.

## Development Workflow

### Edit Rust Code and Rebuild

When you modify Rust code in `src/`, rebuild the extension:

```bash
uv run maturin develop --release
```

This recompiles the Rust code and reinstalls the Python package in development
mode.

### Run Benchmarks

The project includes performance benchmarks in the `benchmarks/` folder:

```bash
# Run a simple load test
uv run benchmarks/simple_load_test.py

# Run memory profiling
uv run benchmarks/memory_benchmark.py

# Run profiling
uv run benchmarks/profile_test.py
```

### Type Checking

The project includes a `fastmssql.pyi` stub file for type hints. Ensure your IDE
recognizes it for better autocomplete and type checking.

## Project Structure

```bash
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

Install Docker using [Docker Desktop](https://www.docker.com/products/docker-desktop)

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
uv pip install maturin
```

### Tests fail with "connection refused"

Make sure:

1. SQL Server container is running: `docker ps | grep sqlserver`
2. `.env` file has the correct connection string
3. Container has fully started (may take 10-15 seconds)

Wait and retry:

```bash
sleep 10
uv run pytest tests/
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

The project uses GitHub Actions for continuous integration. The workflow in
`.github/workflows/unittests.yml`:

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
