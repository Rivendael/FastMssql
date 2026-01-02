#!/usr/bin/env bash

echo "[INFO] You can use your own SQL Server connection string by placing it in the .env file as FASTMSSQL_TEST_CONNECTION_STRING."

# Use UV to create and sync the virtual env.
uv sync

# Check for Docker
if ! command -v docker &>/dev/null; then
    echo "[ERROR] Docker is not installed or not in PATH. Please install Docker to run the test SQL Server container."
    echo "[INFO] You can still run tests by providing your own SQL Server and connection string in the .env file."
    exit 1
fi

docker run --rm \
    --env "ACCEPT_EULA=Y" \
    --env "MSSQL_SA_PASSWORD=YourStrong@Password" \
    --publish 1433:1433 \
    --name sqlserver \
    --detach mcr.microsoft.com/mssql/server:2022-latest

if [ ! -f .env ]; then
    cp sample.env .env
    echo "[INFO] Created .env file from sample.env"
fi
