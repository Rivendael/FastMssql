
#!/usr/bin/env bash

echo "[INFO] You can use your own SQL Server connection string by placing it in the .env file as FASTMSSQL_TEST_CONNECTION_STRING."

# Activate virtual environment if it exists
if [ -d ".venv" ]; then
    echo "Activating virtual environment..."
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
        # Windows (Git Bash/MSYS2)
        source .venv/Scripts/activate
    else
        # Linux/macOS
        source .venv/bin/activate
    fi
else
    echo "[WARNING] Virtual environment not found. Installing globally."
fi

pip install -r requirements.txt

# Check for Docker
if ! command -v docker &> /dev/null; then
    echo "[ERROR] Docker is not installed or not in PATH. Please install Docker to run the test SQL Server container."
    echo "[INFO] You can still run tests by providing your own SQL Server and connection string in the .env file."
    exit 1
fi

docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=YourStrong@Password" -p 1433:1433 --name sqlserver -d mcr.microsoft.com/mssql/server:2022-latest
echo "FASTMSSQL_TEST_CONNECTION_STRING=\"Server=localhost,1433;Database=master;User Id=SA;Password=YourStrong@Password;TrustServerCertificate=yes\"" >> sample.env