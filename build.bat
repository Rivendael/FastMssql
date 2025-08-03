@echo off
REM Build script for mssql-python-rust on Windows
REM This script builds the Rust extension and installs it in development mode

echo Building mssql-python-rust...

REM Set Python to use 3.12 specifically
set PYTHON_EXE=C:\Users\River\AppData\Local\Programs\Python\Python312\python.exe

REM Check if Python 3.12 is available
if not exist "%PYTHON_EXE%" (
    echo Python 3.12 not found at %PYTHON_EXE%
    echo Please install Python 3.12 or update the path in this script
    exit /b 1
)

echo Using Python: %PYTHON_EXE%
%PYTHON_EXE% --version

REM Create/activate virtual environment with Python 3.12
if not exist ".venv" (
    echo Creating virtual environment with Python 3.12...
    %PYTHON_EXE% -m venv .venv
)

echo Activating virtual environment...
call .venv\Scripts\activate.bat

REM Check if maturin is installed
maturin --version >nul 2>&1
if errorlevel 1 (
    echo Installing maturin...
    pip install maturin
)

REM Set PyO3 to use forward compatibility if needed
set PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1

REM Build the Rust extension in development mode
echo Building Rust extension...
maturin develop --release

echo Build completed successfully!
echo.
echo You can now import the library:
echo   import mssql_python_rust
echo.
echo Or run the examples:
echo   python examples\basic_usage.py
