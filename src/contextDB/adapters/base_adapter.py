"""Base adapter interface for all database adapters."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from contextlib import asynccontextmanager


class BaseDatabaseAdapter(ABC):
    """
    Abstract base class defining the interface for all database adapters.

    Each database adapter (Redshift, PostgreSQL, Aurora, MySQL) implements this interface
    to provide a consistent API for database operations while allowing database-specific optimizations.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize adapter with database configuration.

        Args:
            config: Database configuration dictionary with keys:
                   - host: Database host
                   - port: Database port
                   - database: Database name
                   - user: Username
                   - password: Password
                   - schema: Default schema (optional)
        """
        self.config = config
        self._connection_pool = None

    @abstractmethod
    async def connect(self) -> None:
        """
        Establish database connection pool.

        Should create and store connection pool in self._connection_pool
        """
        pass

    @abstractmethod
    @asynccontextmanager
    async def get_connection(self):
        """
        Get a database connection from the pool (context manager).

        Usage:
            async with adapter.get_connection() as conn:
                result = await conn.fetch("SELECT * FROM table")

        Yields:
            Database connection object
        """
        pass

    @abstractmethod
    async def execute_query(self, sql: str) -> List[Dict[str, Any]]:
        """
        Execute SQL query and return results as list of dictionaries.

        Args:
            sql: SQL query to execute

        Returns:
            List of row dictionaries
        """
        pass

    @abstractmethod
    async def get_schemas(self) -> List[str]:
        """
        Get list of all user-defined schemas (excludes system schemas).

        Returns:
            List of schema names
        """
        pass

    @abstractmethod
    async def get_tables(self, schema: str) -> List[Dict[str, Any]]:
        """
        Get list of tables in a schema with metadata.

        Args:
            schema: Schema name

        Returns:
            List of dictionaries with table metadata (name, type, size, etc.)
        """
        pass

    @abstractmethod
    async def get_table_metadata(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """
        Get column-level metadata for a specific table.

        Args:
            schema: Schema name
            table: Table name

        Returns:
            List of dictionaries with column metadata (name, type, nullable, etc.)
        """
        pass

    @abstractmethod
    def supports_feature(self, feature: str) -> bool:
        """
        Check if database supports a specific feature.

        Args:
            feature: Feature name (e.g., 'window_functions', 'cte', 'materialized_views')

        Returns:
            True if feature is supported, False otherwise
        """
        pass

    @property
    @abstractmethod
    def db_type(self) -> str:
        """
        Return database type identifier.

        Returns:
            Database type string (e.g., 'redshift', 'postgres', 'aurora', 'mysql')
        """
        pass

    @property
    @abstractmethod
    def system_views(self) -> Dict[str, str]:
        """
        Return mapping of feature names to database-specific system views.

        Returns:
            Dictionary mapping feature -> system view name
            Example: {'query_performance': 'stl_query', 'table_stats': 'svv_table_info'}
        """
        pass

    async def close(self) -> None:
        """
        Close database connection pool.

        Default implementation - can be overridden by subclasses.
        """
        if self._connection_pool:
            await self._connection_pool.close()
            self._connection_pool = None
