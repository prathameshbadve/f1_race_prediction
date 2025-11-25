"""
Database Resource for PostgreSQL/Supabase operations.

This module provides:
1. DatabaseClient - Standalone client for database operations
2. DatabaseResource - Dagster-aware resource wrapper
3. Support for both development (PostgreSQL) and production (Supabase)
4. Generic CRUD operations with batch support
"""

import time
from contextlib import contextmanager
from enum import Enum
from typing import Any, Dict, Generator, List, Optional, Union
from urllib.parse import quote_plus

import pandas as pd
from dagster import ConfigurableResource
from sqlalchemy import (
    MetaData,
    Table,
    create_engine,
    inspect,
    text,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import (
    OperationalError,
    SQLAlchemyError,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Session,
    sessionmaker,
)
from sqlalchemy.pool import QueuePool

from src.config.logging import get_logger
from src.config.settings import DatabaseConfig

# =============================================================================
# Database Configuration
# =============================================================================


class DatabaseBackend(Enum):
    """Supported database backends."""

    POSTGRESQL = "postgresql"
    SUPABASE = "supabase"


# SQLAlchemy Base for ORM models
class Base(DeclarativeBase):
    """Base class for SQLAlchemy ORM models."""


# =============================================================================
# Database Client
# =============================================================================


class DatabaseClient:
    """
    Standalone client for database operations.

    Works with both PostgreSQL and Supabase with the same interface.
    Can be used independently in Streamlit, notebooks, or other applications.
    """

    def __init__(
        self,
        config: DatabaseConfig,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_timeout: int = 30,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        echo: bool = False,
    ):
        """
        Initialize DatabaseClient.

        Args:
            connection_string: Full connection string (overrides individual params)
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
            backend: Database backend (POSTGRESQL or SUPABASE)
            pool_size: Connection pool size
            max_overflow: Max overflow connections
            pool_timeout: Pool timeout in seconds
            max_retries: Maximum number of retry attempts
            retry_delay: Initial delay between retries (exponential backoff)
            echo: Echo SQL statements (for debugging)
        """

        self.backend = DatabaseBackend(config.backend)
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.logger = get_logger("resources.database")

        # Build connection string
        if config.connection_string:
            self.connection_string = config.connection_string
        else:
            if self.backend == DatabaseBackend.SUPABASE.value:
                # ToDo: Check the connection string format for
                # Supabase and update accordingly
                encoded_user = quote_plus(config.user) if config.user else ""
                encoded_password = (
                    quote_plus(config.password) if config.password else ""
                )
                self.connection_string = f"postgresql://{encoded_user}:{encoded_password}@{config.host}:{config.port}/{config.database}"  # pylint: disable=line-too-long
            else:
                # URL-encode credentials to handle special characters
                encoded_user = quote_plus(config.user) if config.user else ""
                encoded_password = (
                    quote_plus(config.password) if config.password else ""
                )
                self.connection_string = f"postgresql://{encoded_user}:{encoded_password}@{config.host}:{config.port}/{config.database}"  # pylint: disable=line-too-long

        # Create engine with connection pooling
        self.engine = create_engine(
            self.connection_string,
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_pre_ping=True,  # Verify connections before using
            echo=echo,
        )

        # Create session factory
        self.session_local = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine,
        )

        # Metadata for table reflection
        self.metadata = MetaData()

        self.logger.info(
            "DatabaseClient initialized (%s) with pool_size=%d",
            self.backend.value,
            pool_size,
        )

    @classmethod
    def from_env(cls):
        """Factory method to return object configured as per env variables"""

        return cls(config=DatabaseConfig.from_env())

    @classmethod
    def from_custom_config(cls, custom_config: DatabaseConfig):
        """
        Factory method to return a class object configured
        as per the provided configuration
        """

        return cls(custom_config)

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Get a database session with automatic cleanup.

        Yields:
            SQLAlchemy Session

        Example:
            >>> with client.get_session() as session:
            ...     result = session.execute(text("SELECT * FROM races"))
        """

        session = self.session_local()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error("Session error: %s", str(e))
            raise
        finally:
            session.close()

    def _retry_with_backoff(self, operation, *args, **kwargs):
        """
        Execute database operation with exponential backoff retry logic.

        Args:
            operation: Function to execute
            *args: Positional arguments for the operation
            **kwargs: Keyword arguments for the operation

        Returns:
            Result of the operation

        Raises:
            Exception: If all retry attempts fail
        """

        last_exception = None

        for attempt in range(self.max_retries):
            try:
                return operation(*args, **kwargs)
            except (OperationalError, SQLAlchemyError) as e:
                last_exception = e
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2**attempt)
                    self.logger.warning(
                        "Attempt %d/%d failed: %s. Retrying in %ds...",
                        attempt + 1,
                        self.max_retries,
                        str(e),
                        delay,
                    )
                    time.sleep(delay)
                else:
                    self.logger.error(
                        "All %d attempts failed for operation: %s",
                        self.max_retries,
                        operation.__name__,
                    )

        raise last_exception

    def test_connection(self) -> bool:
        """
        Test database connection.

        Returns:
            True if connection successful, False otherwise
        """

        try:
            with self.get_session() as session:
                session.execute(text("SELECT 1"))
            self.logger.info("Database connection test successful")
            return True

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Database connection test failed: %s", str(e))
            return False

    def execute_raw_sql(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        fetch: bool = True,
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Execute raw SQL query.

        Args:
            query: SQL query string
            params: Query parameters
            fetch: Whether to fetch results

        Returns:
            List of results as dictionaries if fetch=True, None otherwise

        Example:
            >>> results = client.execute_raw_sql(
            ...     "SELECT * FROM races WHERE year = :year",
            ...     params={"year": 2024}
            ... )
        """

        try:

            def _execute():
                with self.get_session() as session:
                    result = session.execute(text(query), params or {})

                    # Convert to list of dicts
                    if fetch:
                        columns = result.keys()
                        return [dict(zip(columns, row)) for row in result.fetchall()]
                    return None

            return self._retry_with_backoff(_execute)

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to execute SQL: %s", str(e))
            return None

    def insert(
        self,
        table_name: str,
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
        return_ids: bool = False,
    ) -> Optional[Union[int, List[int]]]:
        """
        Insert data into a table.

        Args:
            table_name: Name of the table
            data: Dictionary or list of dictionaries to insert
            return_ids: Whether to return inserted IDs

        Returns:
            Inserted ID(s) if return_ids=True, None otherwise

        Example:
            >>> client.insert("races", {
            ...     "name": "Bahrain Grand Prix",
            ...     "year": 2024,
            ...     "round": 1
            ... })
        """

        try:
            # Ensure data is a list
            if isinstance(data, dict):
                data = [data]

            def _insert():
                with self.get_session() as session:
                    # Reflect table
                    table = Table(table_name, self.metadata, autoload_with=self.engine)

                    # Insert data
                    result = session.execute(table.insert(), data)

                    if return_ids:
                        return result.inserted_primary_key

                    return None

            return self._retry_with_backoff(_insert)

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to insert data: %s", str(e))
            return None

    def bulk_insert(
        self,
        table_name: str,
        data: List[Dict[str, Any]],
    ) -> bool:
        """
        Bulk insert data into a table (optimized for large datasets).

        Args:
            table_name: Name of the table
            data: List of dictionaries to insert

        Returns:
            True if successful, False otherwise

        Example:
            >>> data = [
            ...     {"lap": 1, "time": 90.123},
            ...     {"lap": 2, "time": 89.456},
            ... ]
            >>> client.bulk_insert("lap_times", data)
        """

        if not data:
            self.logger.warning("No data to insert")
            return False

        try:

            def _bulk_insert():
                with self.get_session() as session:
                    # Reflect table
                    table = Table(table_name, self.metadata, autoload_with=self.engine)

                    # Bulk insert
                    session.execute(table.insert(), data)

                    self.logger.info(
                        "Bulk inserted %d rows into %s",
                        len(data),
                        table_name,
                    )

                return True

            return self._retry_with_backoff(_bulk_insert)

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to bulk insert data: %s", str(e))
            return False

    def upsert(
        self,
        table_name: str,
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None,
    ) -> bool:
        """
        Insert or update data (upsert) using ON CONFLICT.

        Args:
            table_name: Name of the table
            data: Dictionary or list of dictionaries
            conflict_columns: Columns that define uniqueness
            update_columns: Columns to update on conflict (all non-conflict if None)

        Returns:
            True if successful, False otherwise

        Example:
            >>> client.upsert(
            ...     "drivers",
            ...     {"driver_id": "VER", "name": "Max Verstappen", "team": "Red Bull"},
            ...     conflict_columns=["driver_id"],
            ...     update_columns=["team"]
            ... )
        """

        try:
            # Ensure data is a list
            if isinstance(data, dict):
                data = [data]

            def _upsert():
                with self.get_session() as session:
                    # Reflect table
                    table = Table(table_name, self.metadata, autoload_with=self.engine)

                    # Build ON CONFLICT clause
                    stmt = insert(table).values(data)

                    # Determine which columns to update
                    if update_columns:
                        update_dict = {
                            col: stmt.excluded[col] for col in update_columns
                        }
                    else:
                        # Update all columns except conflict columns
                        update_dict = {
                            col.name: stmt.excluded[col.name]
                            for col in table.columns
                            if col.name not in conflict_columns
                        }

                    stmt = stmt.on_conflict_do_update(
                        index_elements=conflict_columns, set_=update_dict
                    )

                    session.execute(stmt)
                    self.logger.info("Upserted %d rows into %s", len(data), table_name)

                return True

            return self._retry_with_backoff(_upsert)

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to upsert data: %s", str(e))
            return False

    def query(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[str] = None,
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Query data from a table.

        Args:
            table_name: Name of the table
            columns: Columns to select (all if None)
            filters: WHERE conditions as dict
            limit: Maximum number of rows
            offset: Number of rows to skip
            order_by: Column to order by

        Returns:
            List of dictionaries or None if error

        Example:
            >>> results = client.query(
            ...     "races",
            ...     columns=["name", "year"],
            ...     filters={"year": 2024},
            ...     order_by="round",
            ...     limit=10
            ... )
        """

        try:

            def _query():
                with self.get_session() as session:
                    # Reflect table
                    table = Table(table_name, self.metadata, autoload_with=self.engine)

                    # Build query
                    if columns:
                        cols = [table.c[col] for col in columns]
                        query = table.select().with_only_columns(*cols)
                    else:
                        query = table.select()

                    # Apply filters
                    if filters:
                        for column, value in filters.items():
                            query = query.where(table.c[column] == value)

                    # Apply ordering
                    if order_by:
                        query = query.order_by(table.c[order_by])

                    # Apply pagination
                    if limit:
                        query = query.limit(limit)
                    if offset:
                        query = query.offset(offset)

                    # Execute
                    result = session.execute(query)
                    columns_names = result.keys()
                    return [dict(zip(columns_names, row)) for row in result.fetchall()]

            return self._retry_with_backoff(_query)

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to query data: %s", str(e))
            return None

    def query_to_dataframe(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Query data and return as pandas DataFrame.

        Args:
            table_name: Name of the table
            columns: Columns to select
            filters: WHERE conditions
            limit: Maximum number of rows

        Returns:
            pandas DataFrame or None

        Example:
            >>> df = client.query_to_dataframe(
            ...     "lap_times",
            ...     filters={"session_id": "bahrain_2024_race"}
            ... )
        """

        results = self.query(table_name, columns, filters, limit)

        if results is None:
            return None

        return pd.DataFrame(results)

    def update(
        self,
        table_name: str,
        data: Dict[str, Any],
        filters: Dict[str, Any],
    ) -> int:
        """
        Update rows in a table.

        Args:
            table_name: Name of the table
            data: Columns to update with new values
            filters: WHERE conditions

        Returns:
            Number of rows updated

        Example:
            >>> count = client.update(
            ...     "drivers",
            ...     {"team": "Ferrari"},
            ...     {"driver_id": "SAI"}
            ... )
        """

        try:

            def _update():
                with self.get_session() as session:
                    # Reflect table
                    table = Table(table_name, self.metadata, autoload_with=self.engine)

                    # Build update statement
                    stmt = table.update().values(**data)

                    # Apply filters
                    for column, value in filters.items():
                        stmt = stmt.where(table.c[column] == value)

                    result = session.execute(stmt)
                    count = result.rowcount

                    self.logger.info("Updated %d rows in %s", count, table_name)
                    return count

            return self._retry_with_backoff(_update)

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to update data: %s", str(e))
            return 0

    def delete(
        self,
        table_name: str,
        filters: Dict[str, Any],
    ) -> int:
        """
        Delete rows from a table.

        Args:
            table_name: Name of the table
            filters: WHERE conditions

        Returns:
            Number of rows deleted

        Example:
            >>> count = client.delete(
            ...     "lap_times",
            ...     {"session_id": "old_session"}
            ... )
        """

        try:

            def _delete():
                with self.get_session() as session:
                    # Reflect table
                    table = Table(table_name, self.metadata, autoload_with=self.engine)

                    # Build delete statement
                    stmt = table.delete()

                    # Apply filters
                    for column, value in filters.items():
                        stmt = stmt.where(table.c[column] == value)

                    result = session.execute(stmt)
                    count = result.rowcount

                    self.logger.info("Deleted %d rows from %s", count, table_name)
                    return count

            return self._retry_with_backoff(_delete)

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to delete data: %s", str(e))
            return 0

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists.

        Args:
            table_name: Name of the table

        Returns:
            True if table exists, False otherwise
        """

        try:
            inspector = inspect(self.engine)
            return table_name in inspector.get_table_names()

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to check table existence: %s", str(e))
            return False

    def get_table_columns(self, table_name: str) -> Optional[List[str]]:
        """
        Get column names for a table.

        Args:
            table_name: Name of the table

        Returns:
            List of column names or None
        """

        try:
            inspector = inspect(self.engine)
            columns = inspector.get_columns(table_name)
            return [col["name"] for col in columns]

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to get table columns: %s", str(e))
            return None

    def create_table_from_dataframe(
        self,
        table_name: str,
        df: pd.DataFrame,
        if_exists: str = "fail",
    ) -> bool:
        """
        Create table from pandas DataFrame schema.

        Args:
            table_name: Name of the table to create
            df: DataFrame with schema
            if_exists: 'fail', 'replace', or 'append'

        Returns:
            True if successful, False otherwise
        """

        try:
            df.head(0).to_sql(table_name, self.engine, if_exists=if_exists, index=False)
            self.logger.info("Created table %s from DataFrame schema", table_name)
            return True

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to create table: %s", str(e))
            return False

    def dataframe_to_sql(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
        chunksize: int = 1000,
    ) -> bool:
        """
        Write DataFrame to SQL table (optimized for large datasets).

        Args:
            df: DataFrame to write
            table_name: Target table name
            if_exists: 'fail', 'replace', or 'append'
            chunksize: Number of rows per batch

        Returns:
            True if successful, False otherwise
        """

        try:
            df.to_sql(
                table_name,
                self.engine,
                if_exists=if_exists,
                index=False,
                chunksize=chunksize,
                method="multi",  # Multi-row INSERT
            )
            self.logger.info("Wrote %d rows to %s", len(df), table_name)
            return True

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to write DataFrame to SQL: %s", str(e))
            return False

    def close(self):
        """Close database engine and connections."""

        self.engine.dispose()
        self.logger.info("Database connections closed")


# =============================================================================
# Dagster Resource
# =============================================================================


class DatabaseResource(ConfigurableResource):
    """
    Dagster resource wrapper for DatabaseClient.

    This resource can be configured in Dagster and used across assets, ops, and jobs.
    """

    def get_client(self) -> DatabaseClient:
        """
        Get a DatabaseClient instance with the configured settings.

        Returns:
            Configured DatabaseClient instance
        """

        return DatabaseClient.from_env()

    @classmethod
    def from_env(cls):
        """Factory for generating DatabaseResource from environment variables."""

        return cls(config=DatabaseConfig.from_env())
