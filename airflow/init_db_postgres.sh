#!/bin/bash
set -e

# Optional: Log the env vars being used
echo "Using POSTGRES_USER=$POSTGRES_USER"
echo "Connecting to initial DB=$POSTGRES_DB"

# Run SQL to set up extra databases and users
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<-EOSQL
    -- Create DB for Airflow
    CREATE DATABASE airflow;

    -- Create extra database
    CREATE DATABASE PDCMmetastore;

    -- Grant privileges
    GRANT ALL PRIVILEGES ON DATABASE PDCMmetastore TO pdcm;
EOSQL
