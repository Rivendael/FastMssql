@echo off
REM Build script for mssql-python-rust with async support (Windows)

echo Building mssql-python-rust with async support...

REM Check if maturin is installed
pip show maturin >nul 2>&1
if errorlevel 1 (
    echo maturin not found. Installing...
    pip install maturin
)

REM Clean previous builds
echo Cleaning previous builds...
if exist target\wheels rmdir /s /q target\wheels
if exist dist rmdir /s /q dist

REM Build in development mode
echo Building in development mode...
maturin develop

REM Check if build was successful
if %errorlevel% equ 0 (
    echo Build successful!
    echo.
    echo You can now run the examples:
    echo   python examples\basic_usage.py
    echo   python examples\async_usage.py  
    echo   python examples\mixed_usage.py
    echo.
    echo To test async functionality:
    echo   python -c "import asyncio; import mssql_python_rust as mssql; print('Async support:', hasattr(mssql, 'connect_async'))"
) else (
    echo Build failed!
    echo Make sure you have:
    echo 1. Rust toolchain installed
    echo 2. Python development headers
    echo 3. All dependencies in Cargo.toml
    exit /b 1
)
