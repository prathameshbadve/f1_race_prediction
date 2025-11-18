-- Create database for MLflow
CREATE DATABASE mlflow;

-- Create database for Dagster
CREATE DATABASE dagster;

-- Grant privileges (optional, since we're using the postgres superuser)
GRANT ALL PRIVILEGES ON DATABASE mlflow TO postgres;
GRANT ALL PRIVILEGES ON DATABASE dagster TO postgres;