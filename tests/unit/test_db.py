"""
Tests for DatabaseResource and DatabaseClient

This file demonstrates:
1. Unit tests with mocked database
2. Integration tests with actual PostgreSQL
3. Dagster asset tests
4. CRUD operation tests
5. Batch operation tests
"""

import pytest
import pandas as pd
from unittest.mock import Mock
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from dagster_project.shared.resources import (
    DatabaseClient,
    DatabaseResource,
    DatabaseBackend,
    create_database_client,
    Base,
)


# =============================================================================
# Unit Tests (with mocking)
# =============================================================================


@pytest.mark.unit
class TestDatabaseClientUnit:
    """Unit tests for DatabaseClient using mocks."""

    @pytest.fixture
    def mock_engine(self, mocker):
        """Create a mocked SQLAlchemy engine."""
        mock = mocker.MagicMock()
        mocker.patch("sqlalchemy.create_engine", return_value=mock)
        return mock

    @pytest.fixture
    def db_client(self, mock_engine):
        """Create DatabaseClient with mocked engine."""
        return DatabaseClient(
            host="localhost",
            port=5432,
            database="test",
            user="test",
            password="test",
        )

    def test_client_initialization(self, db_client):
        """Test DatabaseClient initialization."""
        assert db_client.backend == DatabaseBackend.POSTGRESQL
        assert db_client.max_retries == 3
        assert db_client.connection_string.startswith("postgresql://")

    def test_connection_string_building(self):
        """Test connection string is built correctly."""
        client = DatabaseClient(
            host="testhost",
            port=5433,
            database="testdb",
            user="testuser",
            password="testpass",
        )

        expected = "postgresql://testuser:testpass@testhost:5433/testdb"
        assert client.connection_string == expected

    def test_supabase_backend(self):
        """Test Supabase backend configuration."""
        client = DatabaseClient(
            host="db.supabase.co",
            port=5432,
            database="postgres",
            user="postgres",
            password="password",
            backend=DatabaseBackend.SUPABASE,
        )

        assert client.backend == DatabaseBackend.SUPABASE
        assert "db.supabase.co" in client.connection_string

    def test_retry_mechanism_success_on_retry(self, db_client):
        """Test retry mechanism succeeds on second attempt."""
        mock_operation = Mock(
            side_effect=[OperationalError("test", None, None), "success"]
        )

        result = db_client._retry_with_backoff(mock_operation)

        assert result == "success"
        assert mock_operation.call_count == 2

    def test_retry_mechanism_all_fail(self, db_client):
        """Test retry mechanism fails after max attempts."""
        mock_operation = Mock(side_effect=OperationalError("test", None, None))

        with pytest.raises(OperationalError):
            db_client._retry_with_backoff(mock_operation)

        assert mock_operation.call_count == db_client.max_retries


# =============================================================================
# Integration Tests (requires PostgreSQL)
# =============================================================================


@pytest.mark.integration
class TestDatabaseClientIntegration:
    """Integration tests with actual PostgreSQL database."""

    @pytest.fixture(scope="class")
    def db_client_integration(self, test_env_vars):
        """Create DatabaseClient connected to test PostgreSQL."""
        client = create_database_client(
            host=test_env_vars["POSTGRES_HOST"],
            port=int(test_env_vars["POSTGRES_PORT"]),
            database=test_env_vars["POSTGRES_DB"],
            user=test_env_vars["POSTGRES_USER"],
            password=test_env_vars["POSTGRES_PASSWORD"],
        )

        # Create test tables
        Base.metadata.create_all(client.engine)

        yield client

        # Cleanup
        Base.metadata.drop_all(client.engine)
        client.close()

    @pytest.fixture
    def clean_tables(self, db_client_integration):
        """Clean all tables before each test."""
        yield

        # Cleanup after test
        with db_client_integration.get_session() as session:
            for table in reversed(Base.metadata.sorted_tables):
                session.execute(table.delete())

    def test_connection(self, db_client_integration):
        """Test database connection."""
        assert db_client_integration.test_connection()

    def test_table_exists(self, db_client_integration):
        """Test checking if table exists."""
        # Create a test table
        with db_client_integration.get_session() as session:
            session.execute(
                text("""
                CREATE TABLE IF NOT EXISTS test_table (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100)
                )
            """)
            )

        assert db_client_integration.table_exists("test_table")
        assert not db_client_integration.table_exists("nonexistent_table")

        # Cleanup
        with db_client_integration.get_session() as session:
            session.execute(text("DROP TABLE IF EXISTS test_table"))

    def test_get_table_columns(self, db_client_integration):
        """Test getting table columns."""
        # Create test table
        with db_client_integration.get_session() as session:
            session.execute(
                text("""
                CREATE TABLE IF NOT EXISTS test_columns (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    value INTEGER
                )
            """)
            )

        columns = db_client_integration.get_table_columns("test_columns")

        assert "id" in columns
        assert "name" in columns
        assert "value" in columns

        # Cleanup
        with db_client_integration.get_session() as session:
            session.execute(text("DROP TABLE IF EXISTS test_columns"))

    def test_insert_and_query(self, db_client_integration, clean_tables):
        """Test inserting and querying data."""
        # Create table
        with db_client_integration.get_session() as session:
            session.execute(
                text("""
                CREATE TABLE IF NOT EXISTS test_races (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    year INTEGER
                )
            """)
            )

        # Insert data
        data = {"name": "Bahrain GP", "year": 2024}
        result = db_client_integration.insert("test_races", data)

        # Query data
        results = db_client_integration.query("test_races", filters={"year": 2024})

        assert len(results) == 1
        assert results[0]["name"] == "Bahrain GP"
        assert results[0]["year"] == 2024

        # Cleanup
        with db_client_integration.get_session() as session:
            session.execute(text("DROP TABLE IF EXISTS test_races"))

    def test_bulk_insert(self, db_client_integration, clean_tables):
        """Test bulk insert operation."""
        # Create table
        with db_client_integration.get_session() as session:
            session.execute(
                text("""
                CREATE TABLE IF NOT EXISTS test_laps (
                    id SERIAL PRIMARY KEY,
                    driver VARCHAR(3),
                    lap_time FLOAT
                )
            """)
            )

        # Bulk insert
        data = [
            {"driver": "VER", "lap_time": 88.123},
            {"driver": "HAM", "lap_time": 88.456},
            {"driver": "LEC", "lap_time": 88.789},
        ]

        success = db_client_integration.bulk_insert("test_laps", data)
        assert success

        # Verify
        results = db_client_integration.query("test_laps")
        assert len(results) == 3

        # Cleanup
        with db_client_integration.get_session() as session:
            session.execute(text("DROP TABLE IF EXISTS test_laps"))

    def test_update(self, db_client_integration, clean_tables):
        """Test update operation."""
        # Create and populate table
        with db_client_integration.get_session() as session:
            session.execute(
                text("""
                CREATE TABLE IF NOT EXISTS test_drivers (
                    id SERIAL PRIMARY KEY,
                    code VARCHAR(3),
                    team VARCHAR(50)
                )
            """)
            )
            session.execute(
                text("""
                INSERT INTO test_drivers (code, team) VALUES 
                ('VER', 'Red Bull'),
                ('HAM', 'Mercedes')
            """)
            )

        # Update
        count = db_client_integration.update(
            "test_drivers", {"team": "Ferrari"}, {"code": "HAM"}
        )

        assert count == 1

        # Verify
        results = db_client_integration.query("test_drivers", filters={"code": "HAM"})
        assert results[0]["team"] == "Ferrari"

        # Cleanup
        with db_client_integration.get_session() as session:
            session.execute(text("DROP TABLE IF EXISTS test_drivers"))

    def test_delete(self, db_client_integration, clean_tables):
        """Test delete operation."""
        # Create and populate table
        with db_client_integration.get_session() as session:
            session.execute(
                text("""
                CREATE TABLE IF NOT EXISTS test_delete (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(50)
                )
            """)
            )
            session.execute(
                text("""
                INSERT INTO test_delete (name) VALUES 
                ('keep'),
                ('delete1'),
                ('delete2')
            """)
            )

        # Delete
        count = db_client_integration.delete("test_delete", {"name": "delete1"})

        assert count == 1

        # Verify
        results = db_client_integration.query("test_delete")
        assert len(results) == 2
        assert all(r["name"] != "delete1" for r in results)

        # Cleanup
        with db_client_integration.get_session() as session:
            session.execute(text("DROP TABLE IF EXISTS test_delete"))

    def test_upsert(self, db_client_integration, clean_tables):
        """Test upsert operation."""
        # Create table
        with db_client_integration.get_session() as session:
            session.execute(
                text("""
                CREATE TABLE IF NOT EXISTS test_upsert (
                    driver_code VARCHAR(3) PRIMARY KEY,
                    team VARCHAR(50),
                    points INTEGER
                )
            """)
            )

        # Initial insert
        data = [
            {"driver_code": "VER", "team": "Red Bull", "points": 100},
            {"driver_code": "HAM", "team": "Mercedes", "points": 80},
        ]

        db_client_integration.bulk_insert("test_upsert", data)

        # Upsert (update VER, insert LEC)
        upsert_data = [
            {"driver_code": "VER", "team": "Red Bull", "points": 125},  # Update
            {"driver_code": "LEC", "team": "Ferrari", "points": 75},  # Insert
        ]

        success = db_client_integration.upsert(
            "test_upsert",
            upsert_data,
            conflict_columns=["driver_code"],
            update_columns=["points"],
        )

        assert success

        # Verify
        results = db_client_integration.query("test_upsert", order_by="driver_code")
        assert len(results) == 3

        ver_result = next(r for r in results if r["driver_code"] == "VER")
        assert ver_result["points"] == 125  # Updated

        lec_result = next(r for r in results if r["driver_code"] == "LEC")
        assert lec_result["points"] == 75  # Inserted

        # Cleanup
        with db_client_integration.get_session() as session:
            session.execute(text("DROP TABLE IF EXISTS test_upsert"))

    def test_query_to_dataframe(self, db_client_integration, clean_tables):
        """Test querying data to DataFrame."""
        # Create and populate table
        with db_client_integration.get_session() as session:
            session.execute(
                text("""
                CREATE TABLE IF NOT EXISTS test_df (
                    id SERIAL PRIMARY KEY,
                    value INTEGER
                )
            """)
            )
            session.execute(
                text("""
                INSERT INTO test_df (value) VALUES (1), (2), (3), (4), (5)
            """)
            )

        # Query to DataFrame
        df = db_client_integration.query_to_dataframe("test_df")

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 5
        assert "value" in df.columns

        # Cleanup
        with db_client_integration.get_session() as session:
            session.execute(text("DROP TABLE IF EXISTS test_df"))

    def test_dataframe_to_sql(self, db_client_integration, clean_tables):
        """Test writing DataFrame to SQL."""
        # Create DataFrame
        df = pd.DataFrame({"driver": ["VER", "HAM", "LEC"], "points": [100, 80, 70]})

        # Write to SQL
        success = db_client_integration.dataframe_to_sql(
            df, "test_df_insert", if_exists="replace"
        )

        assert success

        # Verify
        result_df = db_client_integration.query_to_dataframe("test_df_insert")
        assert len(result_df) == 3

        pd.testing.assert_frame_equal(
            df.sort_values("driver").reset_index(drop=True),
            result_df.sort_values("driver").reset_index(drop=True),
        )

        # Cleanup
        with db_client_integration.get_session() as session:
            session.execute(text("DROP TABLE IF EXISTS test_df_insert"))

    def test_execute_raw_sql(self, db_client_integration, clean_tables):
        """Test executing raw SQL."""
        # Create and populate table
        with db_client_integration.get_session() as session:
            session.execute(
                text("""
                CREATE TABLE IF NOT EXISTS test_raw_sql (
                    id SERIAL PRIMARY KEY,
                    category VARCHAR(20),
                    value INTEGER
                )
            """)
            )
            session.execute(
                text("""
                INSERT INTO test_raw_sql (category, value) VALUES 
                ('A', 10),
                ('A', 20),
                ('B', 30),
                ('B', 40)
            """)
            )

        # Execute raw SQL with aggregation
        results = db_client_integration.execute_raw_sql(
            """
            SELECT category, SUM(value) as total
            FROM test_raw_sql
            GROUP BY category
            ORDER BY category
            """
        )

        assert len(results) == 2
        assert results[0]["category"] == "A"
        assert results[0]["total"] == 30
        assert results[1]["category"] == "B"
        assert results[1]["total"] == 70

        # Cleanup
        with db_client_integration.get_session() as session:
            session.execute(text("DROP TABLE IF EXISTS test_raw_sql"))


# =============================================================================
# Dagster Tests
# =============================================================================


@pytest.mark.dagster
class TestDatabaseResourceDagster:
    """Tests for DatabaseResource in Dagster context."""

    def test_database_resource_configuration(self):
        """Test DatabaseResource can be configured properly."""
        resource = DatabaseResource(
            host="localhost",
            port=5432,
            database="test",
            user="postgres",
            password="postgres",
            backend="postgresql",
        )

        assert resource.host == "localhost"
        assert resource.database == "test"
        assert resource.backend == "postgresql"

    def test_database_resource_get_client(self):
        """Test getting DatabaseClient from resource."""
        resource = DatabaseResource(
            host="localhost",
            port=5432,
            database="test",
            user="postgres",
            password="postgres",
        )

        client = resource.get_client()

        assert isinstance(client, DatabaseClient)
        assert client.backend == DatabaseBackend.POSTGRESQL


# =============================================================================
# Factory Function Tests
# =============================================================================


@pytest.mark.unit
def test_create_database_client_factory():
    """Test factory function creates client correctly."""
    client = create_database_client(
        host="localhost",
        database="test",
        user="postgres",
        password="postgres",
    )

    assert isinstance(client, DatabaseClient)
    assert client.backend == DatabaseBackend.POSTGRESQL


@pytest.mark.unit
def test_create_database_client_supabase():
    """Test factory function for Supabase."""
    client = create_database_client(
        host="db.supabase.co",
        database="postgres",
        user="postgres",
        password="password",
        backend=DatabaseBackend.SUPABASE,
    )

    assert isinstance(client, DatabaseClient)
    assert client.backend == DatabaseBackend.SUPABASE


# =============================================================================
# Performance Tests
# =============================================================================


@pytest.mark.integration
@pytest.mark.slow
class TestDatabasePerformance:
    """Performance tests for database operations."""

    def test_bulk_insert_performance(self, db_client_integration, benchmark_timer):
        """Test bulk insert performance with large dataset."""
        # Create table
        with db_client_integration.get_session() as session:
            session.execute(
                text("""
                CREATE TABLE IF NOT EXISTS test_performance (
                    id SERIAL PRIMARY KEY,
                    value INTEGER
                )
            """)
            )

        # Create large dataset
        data = [{"value": i} for i in range(10000)]

        # Time the bulk insert
        benchmark_timer.start("bulk_insert")
        success = db_client_integration.bulk_insert("test_performance", data)
        benchmark_timer.stop("bulk_insert")

        assert success
        duration = benchmark_timer.get_duration("bulk_insert")
        print(f"Bulk insert of 10,000 rows: {duration:.2f}s")

        # Should complete in reasonable time
        assert duration < 5.0, "Bulk insert took too long"

        # Cleanup
        with db_client_integration.get_session() as session:
            session.execute(text("DROP TABLE IF EXISTS test_performance"))


# =============================================================================
# Error Handling Tests
# =============================================================================


@pytest.mark.unit
class TestErrorHandling:
    """Tests for error handling."""

    def test_nonexistent_table(self, db_client_integration):
        """Test querying nonexistent table."""
        results = db_client_integration.query("nonexistent_table")
        assert results is None

    def test_invalid_column(self, db_client_integration):
        """Test querying with invalid column."""
        # Create table
        with db_client_integration.get_session() as session:
            session.execute(
                text("""
                CREATE TABLE IF NOT EXISTS test_error (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100)
                )
            """)
            )

        results = db_client_integration.query(
            "test_error", columns=["nonexistent_column"]
        )

        assert results is None

        # Cleanup
        with db_client_integration.get_session() as session:
            session.execute(text("DROP TABLE IF EXISTS test_error"))
