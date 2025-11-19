-- Create database for MLflow
CREATE DATABASE mlflow;

-- Create database for Dagster
CREATE DATABASE dagster;

-- Create database for data warehouse
CREATE DATABASE data_warehouse;

-- Grant privileges (optional, since we're using the postgres superuser)
GRANT ALL PRIVILEGES ON DATABASE mlflow TO postgres;
GRANT ALL PRIVILEGES ON DATABASE dagster TO postgres;
GRANT ALL PRIVILEGES ON DATABASE data_warehouse TO postgres;