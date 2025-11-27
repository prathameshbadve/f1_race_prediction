"""
Tests for DatabaseResource and DatabaseClient

This file demonstrates:
1. Unit tests with mocked database
2. Integration tests with actual PostgreSQL
3. Dagster asset tests
4. CRUD operation tests
5. Batch operation tests
"""

# pylint: disable=protected-access, unused-argument

from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest
from sqlalchemy import Table
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

from dagster_project.shared.resources import DatabaseClient
from src.config.settings import DatabaseConfig


@pytest.mark.unit
class TestDatabaseClientInit:
    """Test Database Client initialization"""

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    def test_init_with_config(
        self, mock_create_engine, mock_sessionmaker, database_config_dict: dict
    ):
        """Tets initializing DatabaseClient with config"""

        config = DatabaseConfig(**database_config_dict)
        client = DatabaseClient(config=config)

        assert client.backend.value == "postgresql"
        assert (
            client.connection_string
            == "postgresql://testuser:testpass@localhost:5432/testdb"
        )
        assert client.max_retries == 3
        mock_create_engine.assert_called_once()
        mock_sessionmaker.assert_called_once()

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    def test_init_from_env(
        self,
        mock_create_engine,
        mock_sessionmaker,
        mock_env_vars: dict[str, str],
    ):
        """Tets initializing DatabaseClient from environment"""

        client = DatabaseClient.from_env()

        assert client.backend.value == "postgresql"
        assert (
            client.connection_string
            == "postgresql://testuser:testpass@localhost:5432/testdb"
        )
        assert client.retry_delay == 1.0
        mock_create_engine.assert_called_once()
        mock_sessionmaker.assert_called_once()


@pytest.mark.unit
class TestDatabaseClientSession:
    """Test DBClient session management"""

    def test_get_session_yields_session(self, db_client, mock_session):
        """Test that get_session yields a valid session object."""
        with db_client.get_session() as session:
            assert session is mock_session

    def test_get_session_creates_session_from_factory(
        self, db_client, mock_session_factory
    ):
        """Test that get_session calls the session factory."""
        with db_client.get_session() as _:
            pass

        mock_session_factory.assert_called_once()

    def test_get_session_commits_on_success(self, db_client, mock_session):
        """Test that session is committed when no exception occurs."""
        with db_client.get_session() as session:
            # Simulate some work
            session.execute("SELECT 1")

        mock_session.commit.assert_called_once()

    def test_get_session_closes_session_on_success(self, db_client, mock_session):
        """Test that session is closed after successful execution."""
        with db_client.get_session() as _:
            pass

        mock_session.close.assert_called_once()

    def test_get_session_on_exception(self, db_client, mock_session):
        """Test that session is rolled back when an exception occurs."""
        with pytest.raises(ValueError):
            with db_client.get_session() as _:
                raise ValueError("Test error")

        # Rollback
        mock_session.rollback.assert_called_once()
        # Closes
        mock_session.close.assert_called_once()
        # No commit
        mock_session.commit.assert_not_called()

    def test_get_session_logs_error_on_exception(self, db_client):
        """Test that errors are logged when an exception occurs."""
        error_message = "Database connection failed"

        with pytest.raises(RuntimeError):
            with db_client.get_session() as _:
                raise RuntimeError(error_message)

        db_client.logger.error.assert_called_once()
        # Verify the error message is included in the log
        call_args = db_client.logger.error.call_args
        assert "Session error" in call_args[0][0]
        assert error_message in str(call_args[0])

    def test_get_session_exception_during_commit(self, db_client, mock_session):
        """Test handling when commit itself raises an exception."""
        mock_session.commit.side_effect = SQLAlchemyError("Commit failed")

        with pytest.raises(SQLAlchemyError, match="Commit failed"):
            with db_client.get_session() as _:
                pass  # No error in the block, but commit will fail

        # Session should still be closed
        mock_session.close.assert_called_once()


@pytest.mark.unit
class TestDatabaseClientRetry:
    """Test DatabaseClient retry logic"""

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    @patch("time.sleep")
    def test_retry_success_on_second_attempt(
        self,
        mock_sleep,
        mock_sessionmaker,
        mock_create_engine,
        database_config_dict: dict,
    ):
        """Test successful retry after initial failure"""
        # Setup
        mock_session = Mock(spec=Session)
        mock_session_class = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_class
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        config = DatabaseConfig(**database_config_dict)
        client = DatabaseClient(config=config, max_retries=3, retry_delay=0.1)

        mock_operation = MagicMock()
        # First call fails, second succeeds
        mock_operation.side_effect = [OperationalError("Error", None, None), "Success"]

        result = client._retry_with_backoff(
            mock_operation,
        )

        assert result == "Success"
        assert mock_operation.call_count == 2
        mock_sleep.assert_called_once()

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    @patch("time.sleep")
    def test_retry_all_attempts_fail(
        self,
        mock_sleep,
        mock_sessionmaker,
        mock_create_engine,
        database_config_dict: dict,
    ):
        """Test when all retry attempts fail"""
        # Setup
        mock_session = Mock(spec=Session)
        mock_session_class = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_class
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        config = DatabaseConfig(**database_config_dict)
        client = DatabaseClient(config=config, max_retries=3, retry_delay=0.1)

        mock_operation = MagicMock()
        mock_operation.__name__ = "MockOperation"
        mock_operation.side_effect = [
            OperationalError("Persistent error", None, None),
            OperationalError("Persistent error", None, None),
            OperationalError("Persistent error", None, None),
        ]

        with pytest.raises(OperationalError):
            client._retry_with_backoff(
                mock_operation,
            )

        assert mock_operation.call_count == 3
        assert mock_sleep.call_count == 2

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    @patch("time.sleep")
    def test_retry_exponential_backoff(
        self, mock_sleep, mock_sessionmaker, mock_create_engine, database_config_dict
    ):
        """Test exponential backoff delay"""
        # Setup
        mock_session = Mock(spec=Session)
        mock_session_class = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_class
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        config = DatabaseConfig(**database_config_dict)
        client = DatabaseClient(config=config, max_retries=4, retry_delay=1.0)

        mock_operation = MagicMock()
        mock_operation.__name__ = "MockOperation"
        mock_operation.side_effect = OperationalError("Error", None, None)

        with pytest.raises(OperationalError):
            client._retry_with_backoff(
                mock_operation,
            )

        # Check exponential backoff: 1s, 2s, 4s
        expected_delays = [1.0, 2.0, 4.0]
        actual_delays = [call_args[0][0] for call_args in mock_sleep.call_args_list]
        assert actual_delays == expected_delays


@pytest.mark.unit
class TestDatabaseClientConnection:
    """Test DatabaseClient connection operations"""

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    def test_test_connection_success(
        self, mock_sessionmaker, mock_create_engine, database_config_dict
    ):
        """Test successful connection test"""
        # Setup
        mock_session = Mock(spec=Session)
        mock_session_class = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_class
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        config = DatabaseConfig(**database_config_dict)
        client = DatabaseClient(config=config)

        result = client.test_connection()

        assert result is True
        mock_session.execute.assert_called_once()

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    def test_test_connection_failure(
        self, mock_sessionmaker, mock_create_engine, database_config_dict
    ):
        """Test failed connection test"""
        config = DatabaseConfig(**database_config_dict)

        # Setup
        mock_session = Mock(spec=Session)
        mock_session.execute.side_effect = Exception("Connection failed")
        mock_session_class = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_class
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        client = DatabaseClient(config=config)

        result = client.test_connection()

        assert result is False


@pytest.mark.unit
class TestDatabaseClientRawSQL:
    """Test DatabaseClient raw SQL execution"""

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    def test_execute_raw_sql_with_fetch(
        self, mock_sessionmaker, mock_create_engine, database_config_dict
    ):
        """Test executing SQL with fetch"""
        config = DatabaseConfig(**database_config_dict)

        # Setup
        mock_result = MagicMock()
        mock_result.keys.return_value = ["id", "name"]
        mock_result.fetchall.return_value = [(1, "Test"), (2, "Test2")]

        mock_session = Mock(spec=Session)
        mock_session.execute.return_value = mock_result
        mock_session_class = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_class
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        client = DatabaseClient(config=config)

        results = client.execute_raw_sql("SELECT * FROM test", fetch=True)

        assert results is not None
        assert len(results) == 2
        assert results[0] == {"id": 1, "name": "Test"}
        assert results[1] == {"id": 2, "name": "Test2"}

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    def test_execute_raw_sql_without_fetch(
        self, mock_sessionmaker, mock_create_engine, database_config_dict
    ):
        """Test executing SQL without fetch"""
        config = DatabaseConfig(**database_config_dict)

        mock_session = Mock(spec=Session)
        mock_session_class = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_class
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        client = DatabaseClient(config=config)

        result = client.execute_raw_sql("INSERT INTO test VALUES (1)", fetch=False)

        assert result is None

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    def test_execute_raw_sql_with_params(
        self, mock_sessionmaker, mock_create_engine, database_config_dict
    ):
        """Test executing SQL with parameters"""
        config = DatabaseConfig(**database_config_dict)

        mock_result = MagicMock()
        mock_result.keys.return_value = ["id", "year"]
        mock_result.fetchall.return_value = [(1, 2024)]

        mock_session = Mock(spec=Session)
        mock_session.execute.return_value = mock_result
        mock_session_class = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_class
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        client = DatabaseClient(config=config)

        results = client.execute_raw_sql(
            "SELECT * FROM races WHERE year = :year",
            params={"year": 2024},
        )

        assert results is not None
        assert len(results) == 1


@pytest.mark.unit
class TestDatabaseClientInsert:
    """Test DatabaseClient insert operations"""

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    @patch("dagster_project.shared.resources.db_resource.Table")
    def test_insert_single_row(
        self,
        mock_table,
        mock_sessionmaker,
        mock_create_engine,
        database_config_dict,
    ):
        """Test inserting single row"""
        config = DatabaseConfig(**database_config_dict)

        mock_session = Mock(spec=Session)
        mock_session_class = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_class
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        client = DatabaseClient(config=config)

        data = {"name": "Bahrain Grand Prix", "year": 2024}
        result = client.insert("races", data)

        assert result is None  # Returns None when return_ids=False
        mock_session.execute.assert_called_once()

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    @patch("dagster_project.shared.resources.db_resource.Table")
    def test_insert_multiple_rows(
        self,
        mock_table,
        mock_sessionmaker,
        mock_create_engine,
        database_config_dict: dict,
    ):
        """Test inserting multiple rows"""
        config = DatabaseConfig(**database_config_dict)

        mock_session = Mock(spec=Session)
        mock_session_class = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_class
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        client = DatabaseClient(config=config)

        data = [
            {"name": "Bahrain", "year": 2024},
            {"name": "Saudi Arabia", "year": 2024},
        ]
        result = client.insert("races", data)

        assert result is None
        mock_session.execute.assert_called_once()


@pytest.mark.unit
class TestDatabaseClientBulkInsert:
    """Test DatabaseClient bulk insert operations"""

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    @patch("dagster_project.shared.resources.db_resource.Table")
    def test_bulk_insert_success(
        self,
        mock_table,
        mock_sessionmaker,
        mock_create_engine,
        database_config_dict: dict,
    ):
        """Test successful bulk insert"""
        config = DatabaseConfig(**database_config_dict)

        mock_session = Mock(spec=Session)
        mock_session_class = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_class
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        client = DatabaseClient(config=config)

        data = [
            {"lap": 1, "time": 90.123},
            {"lap": 2, "time": 89.456},
            {"lap": 3, "time": 88.789},
        ]
        result = client.bulk_insert("lap_times", data)

        assert result is True
        mock_session.execute.assert_called_once()

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    def test_bulk_insert_empty_data(
        self,
        mock_sessionmaker,
        mock_create_engine,
        database_config_dict: dict,
    ):
        """Test bulk insert with empty data"""
        config = DatabaseConfig(**database_config_dict)
        client = DatabaseClient(config=config)

        result = client.bulk_insert("lap_times", [])

        assert result is False


@pytest.mark.unit
class TestDatabaseClientUpsert:
    """Test DatabaseClient upsert operations"""

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    @patch("dagster_project.shared.resources.db_resource.Table")
    def test_upsert_single_row(
        self,
        mock_table,
        mock_sessionmaker,
        mock_create_engine,
        database_config_dict: dict,
    ):
        """Test upserting single row"""
        config = DatabaseConfig(**database_config_dict)

        mock_session = Mock(spec=Session)
        mock_session_class = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_class
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        # Mock table
        mock_table_instance = MagicMock(spec=Table)
        mock_table_instance.columns = [
            MagicMock(name="driver_id"),
            MagicMock(name="team"),
        ]
        mock_table.return_value = mock_table_instance

        client = DatabaseClient(config=config)

        data = {"driver_id": "VER", "team": "Red Bull Racing"}
        result = client.upsert(
            "drivers",
            data,
            conflict_columns=["driver_id"],
            update_columns=["team"],
        )

        assert result is True
        mock_session.execute.assert_called_once()

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    @patch("dagster_project.shared.resources.db_resource.Table")
    def test_upsert_multiple_rows(
        self, mock_table, mock_sessionmaker, mock_create_engine, database_config_dict
    ):
        """Test upserting multiple rows"""
        config = DatabaseConfig(**database_config_dict)

        mock_session = Mock(spec=Session)
        mock_session_class = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_class
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        # Create mock columns with proper name attributes
        mock_col_driver_id = MagicMock()
        mock_col_driver_id.name = "driver_id"
        mock_col_driver_id.key = "driver_id"

        mock_col_team = MagicMock()
        mock_col_team.name = "team"
        mock_col_team.key = "team"

        mock_col_points = MagicMock()
        mock_col_points.name = "points"
        mock_col_points.key = "points"

        mock_table_instance = MagicMock(spec=Table)
        mock_table_instance.columns = [
            mock_col_driver_id,
            mock_col_team,
            mock_col_points,
        ]

        # Also set up c accessor which SQLAlchemy uses for column access
        mock_table_instance.c = MagicMock()
        mock_table_instance.c.driver_id = mock_col_driver_id
        mock_table_instance.c.team = mock_col_team
        mock_table_instance.c.points = mock_col_points

        mock_table.return_value = mock_table_instance

        client = DatabaseClient(config=config)

        data = [
            {"driver_id": "VER", "team": "Red Bull", "points": "25"},
            {"driver_id": "HAM", "team": "Mercedes", "points": "18"},
        ]
        result = client.upsert(
            "drivers",
            data,
            conflict_columns=["driver_id"],
        )

        assert result is True


@pytest.mark.unit
class TestDatabaseClientQuery:
    """Test DatabaseClient query operations"""

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    @patch("dagster_project.shared.resources.db_resource.Table")
    def test_query_all_columns(
        self,
        mock_table,
        mock_sessionmaker,
        mock_create_engine,
        database_config_dict: dict,
    ):
        """Test querying all columns"""
        config = DatabaseConfig(**database_config_dict)

        mock_result = MagicMock()
        mock_result.keys.return_value = ["id", "name", "year"]
        mock_result.fetchall.return_value = [(1, "Bahrain", 2024)]

        mock_session = Mock(spec=Session)
        mock_session.execute.return_value = mock_result
        mock_session_class = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_class
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        client = DatabaseClient(config=config)

        results = client.query("races")

        assert results is not None
        assert len(results) == 1
        assert results[0] == {"id": 1, "name": "Bahrain", "year": 2024}

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    @patch("dagster_project.shared.resources.db_resource.Table")
    def test_query_specific_columns(
        self,
        mock_table,
        mock_sessionmaker,
        mock_create_engine,
        database_config_dict: dict,
    ):
        """Test querying specific columns"""
        config = DatabaseConfig(**database_config_dict)

        mock_result = MagicMock()
        mock_result.keys.return_value = ["name", "year"]
        mock_result.fetchall.return_value = [("Bahrain", 2024)]

        mock_session = Mock(spec=Session)
        mock_session.execute.return_value = mock_result
        mock_session_class = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_class
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        client = DatabaseClient(config=config)

        results = client.query("races", columns=["name", "year"])

        assert results is not None
        assert results[0] == {"name": "Bahrain", "year": 2024}

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    @patch("dagster_project.shared.resources.db_resource.Table")
    def test_query_with_filters(
        self,
        mock_table,
        mock_sessionmaker,
        mock_create_engine,
        database_config_dict: dict,
    ):
        """Test querying with filters"""
        config = DatabaseConfig(**database_config_dict)

        mock_result = MagicMock()
        mock_result.keys.return_value = ["name"]
        mock_result.fetchall.return_value = [("Bahrain",)]

        mock_session = Mock(spec=Session)
        mock_session.execute.return_value = mock_result
        mock_session_class = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_class
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        client = DatabaseClient(config=config)

        results = client.query("races", filters={"year": 2024})

        assert results is not None

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    @patch("dagster_project.shared.resources.db_resource.Table")
    def test_query_with_limit(
        self,
        mock_table,
        mock_sessionmaker,
        mock_create_engine,
        database_config_dict: dict,
    ):
        """Test querying with limit"""
        config = DatabaseConfig(**database_config_dict)

        mock_result = MagicMock()
        mock_result.keys.return_value = ["name"]
        mock_result.fetchall.return_value = [("Bahrain",), ("Saudi Arabia",)]

        mock_session = Mock(spec=Session)
        mock_session.execute.return_value = mock_result
        mock_session_class = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_class
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        client = DatabaseClient(config=config)

        results = client.query("races", limit=2)

        assert results is not None
        assert len(results) == 2


@pytest.mark.unit
class TestDatabaseClientDataFrame:
    """Test DatabaseClient DataFrame operations"""

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    @patch("dagster_project.shared.resources.db_resource.Table")
    def test_query_to_dataframe(
        self,
        mock_table,
        mock_sessionmaker,
        mock_create_engine,
        database_config_dict: dict,
    ):
        """Test querying to DataFrame"""
        config = DatabaseConfig(**database_config_dict)

        mock_result = MagicMock()
        mock_result.keys.return_value = ["name", "year"]
        mock_result.fetchall.return_value = [("Bahrain", 2024), ("Saudi Arabia", 2024)]

        mock_session = Mock(spec=Session)
        mock_session.execute.return_value = mock_result
        mock_session_class = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_class
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        client = DatabaseClient(config=config)

        df = client.query_to_dataframe("races")

        assert df is not None
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert list(df.columns) == ["name", "year"]

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    def test_create_table_from_dataframe(
        self,
        mock_sessionmaker,
        mock_create_engine,
        database_config_dict: dict,
        sample_schedule_df: pd.DataFrame,
    ):
        """Test creating table from DataFrame"""
        config = DatabaseConfig(**database_config_dict)

        # Mock the to_sql method
        with patch.object(pd.DataFrame, "to_sql") as mock_to_sql:
            client = DatabaseClient(config=config)

            result = client.create_table_from_dataframe(
                "test_table", sample_schedule_df
            )

            assert result is True
            mock_to_sql.assert_called_once()

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    def test_dataframe_to_sql(
        self,
        mock_sessionmaker,
        mock_create_engine,
        database_config_dict: dict,
        sample_schedule_df: pd.DataFrame,
    ):
        """Test writing DataFrame to SQL"""
        config = DatabaseConfig(**database_config_dict)

        with patch.object(pd.DataFrame, "to_sql") as mock_to_sql:
            client = DatabaseClient(config=config)

            result = client.dataframe_to_sql(sample_schedule_df, "races")

            assert result is True
            mock_to_sql.assert_called_once()
            call_kwargs = mock_to_sql.call_args[1]
            assert call_kwargs["if_exists"] == "append"
            assert call_kwargs["chunksize"] == 1000


@pytest.mark.unit
class TestDatabaseClientUpdate:
    """Test DatabaseClient update operations"""

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    @patch("dagster_project.shared.resources.db_resource.Table")
    def test_update_rows(
        self,
        mock_table,
        mock_sessionmaker,
        mock_create_engine,
        database_config_dict: dict,
    ):
        """Test updating rows"""
        config = DatabaseConfig(**database_config_dict)

        mock_result = MagicMock()
        mock_result.rowcount = 1

        mock_session = Mock(spec=Session)
        mock_session.execute.return_value = mock_result
        mock_session_class = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_class
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        client = DatabaseClient(config=config)

        count = client.update("drivers", {"team": "Ferrari"}, {"driver_id": "SAI"})

        assert count == 1
        mock_session.execute.assert_called_once()


@pytest.mark.unit
class TestDatabaseClientDelete:
    """Test DatabaseClient delete operations"""

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    @patch("dagster_project.shared.resources.db_resource.Table")
    def test_delete_rows(
        self,
        mock_table,
        mock_sessionmaker,
        mock_create_engine,
        database_config_dict: dict,
    ):
        """Test deleting rows"""
        config = DatabaseConfig(**database_config_dict)

        mock_result = MagicMock()
        mock_result.rowcount = 2

        mock_session = Mock(spec=Session)
        mock_session.execute.return_value = mock_result
        mock_session_class = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_class
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        client = DatabaseClient(config=config)

        count = client.delete("lap_times", {"session_id": "old_session"})

        assert count == 2
        mock_session.execute.assert_called_once()


@pytest.mark.unit
class TestDatabaseClientUtility:
    """Test DatabaseClient utility methods"""

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    @patch("dagster_project.shared.resources.db_resource.inspect")
    def test_table_exists_true(
        self,
        mock_inspect,
        mock_sessionmaker,
        mock_create_engine,
        database_config_dict: dict,
    ):
        """Test checking if table exists (true)"""
        config = DatabaseConfig(**database_config_dict)

        mock_inspector = MagicMock()
        mock_inspector.get_table_names.return_value = ["races", "drivers", "laps"]
        mock_inspect.return_value = mock_inspector

        client = DatabaseClient(config=config)

        result = client.table_exists("races")

        assert result is True

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    @patch("dagster_project.shared.resources.db_resource.inspect")
    def test_table_exists_false(
        self,
        mock_inspect,
        mock_sessionmaker,
        mock_create_engine,
        database_config_dict: dict,
    ):
        """Test checking if table exists (false)"""
        config = DatabaseConfig(**database_config_dict)

        mock_inspector = MagicMock()
        mock_inspector.get_table_names.return_value = ["races", "drivers"]
        mock_inspect.return_value = mock_inspector

        client = DatabaseClient(config=config)

        result = client.table_exists("nonexistent")

        assert result is False

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    @patch("dagster_project.shared.resources.db_resource.inspect")
    def test_get_table_columns(
        self,
        mock_inspect,
        mock_sessionmaker,
        mock_create_engine,
        database_config_dict: dict,
    ):
        """Test getting table columns"""
        config = DatabaseConfig(**database_config_dict)

        mock_inspector = MagicMock()
        mock_inspector.get_columns.return_value = [
            {"name": "id", "type": "INTEGER"},
            {"name": "name", "type": "VARCHAR"},
            {"name": "year", "type": "INTEGER"},
        ]
        mock_inspect.return_value = mock_inspector

        client = DatabaseClient(config=config)

        columns = client.get_table_columns("races")

        assert columns == ["id", "name", "year"]


@pytest.mark.unit
class TestDatabaseClientClose:
    """Test DatabaseClient close operation"""

    @patch("dagster_project.shared.resources.db_resource.create_engine")
    @patch("dagster_project.shared.resources.db_resource.sessionmaker")
    def test_close(self, mock_sessionmaker, mock_create_engine, database_config_dict):
        """Test closing database connections"""
        config = DatabaseConfig(**database_config_dict)

        mock_session = Mock(spec=Session)
        mock_session_class = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_class
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        client = DatabaseClient(config=config)
        client.close()

        mock_engine.dispose.assert_called_once()
