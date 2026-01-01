#! /bin/bash
docker run --rm \
    --env "ACCEPT_EULA=Y" \
    --env "MSSQL_SA_PASSWORD=StrongPassword123!" \
    --publish 1433:1433 \
    --name sqlserver \
    --detach mcr.microsoft.com/mssql/server:2022-latest
