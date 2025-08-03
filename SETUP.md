# Setup Instructions for mssql-python-rust

## Quick Start

1. **Install Rust** (if not already installed):
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   source $HOME/.cargo/env
   ```

2. **Install maturin**:
   ```bash
   pip install maturin
   ```

3. **Build the extension**:
   ```bash
   # On Windows
   build.bat
   
   # On Unix-like systems  
   chmod +x build.sh
   ./build.sh
   
   # Or manually
   maturin develop --release
   ```

4. **Test the installation**:
   ```python
   import mssql_python_rust
   print(mssql_python_rust.version())
   ```

5. **Run examples**:
   ```bash
   python examples/basic_usage.py
   ```

## Development Setup

1. **Install development dependencies**:
   ```bash
   pip install maturin pytest pytest-asyncio black ruff
   ```

2. **Build in development mode**:
   ```bash
   maturin develop
   ```

3. **Run tests**:
   ```bash
   python -m pytest tests/ -v
   ```

4. **Format code**:
   ```bash
   black python/
   ruff check python/
   ```

## Troubleshooting

### Common Issues

1. **"mssql_python_rust" module not found**
   - Make sure you've run `maturin develop` successfully
   - Check that there were no build errors

2. **Rust compiler not found**
   - Install Rust: https://rustup.rs/
   - Make sure `cargo` is in your PATH

3. **Maturin not found**
   - Install maturin: `pip install maturin`

4. **Build errors on Windows**
   - Install Microsoft Visual C++ Build Tools
   - Or install Visual Studio with C++ support

5. **Connection errors**
   - Make sure SQL Server is running
   - Check your connection string
   - Verify authentication method (Windows vs SQL Server auth)

### Getting Help

- Check the README.md for detailed documentation
- Look at examples/ directory for usage patterns
- Run tests to verify functionality: `python tests/test_basic.py`
