"""Redshift-specific database adapter."""

import asyncpg
import logging
from typing import Any, Dict, List
from contextlib import asynccontextmanager

from .base_adapter import BaseDatabaseAdapter

logger = logging.getLogger(__name__)


class RedshiftAdapter(BaseDatabaseAdapter):
    """
    Database adapter for Amazon Redshift.

    Provides Redshift-specific optimizations and access to Redshift system tables
    (STL, SVV tables) for performance monitoring and metadata discovery.
    """

    @property
    def db_type(self) -> str:
        """Return database type identifier."""
        return "redshift"

    @property
    def system_views(self) -> Dict[str, str]:
        """Return Redshift-specific system views mapping."""
        return {
            'query_performance': 'stl_query',
            'wlm_stats': 'stl_wlm_query',
            'table_info': 'svv_table_info',
            'table_stats': 'stv_tbl_perm',
            'disk_usage': 'stv_partitions'
        }

    def supports_feature(self, feature: str) -> bool:
        """Check if Redshift supports a specific feature."""
        supported_features = {
            'window_functions': True,
            'cte': True,
            'materialized_views': True,
            'distribution_keys': True,  # Redshift-specific
            'sort_keys': True,  # Redshift-specific
            'column_encoding': True,  # Redshift-specific
            'vacuum_analyze': True,  # Redshift-specific
            'wlm': True,  # Workload Management - Redshift-specific
        }
        return supported_features.get(feature, False)

    async def connect(self) -> None:
        """Establish Redshift connection pool."""
        try:
            logger.info("Creating Redshift connection pool...")
            pool_config = {k: v for k, v in self.config.items() if k != 'schema'}
            self._connection_pool = await asyncpg.create_pool(
                **pool_config,
                min_size=1,
                max_size=10
            )
            logger.info("Redshift connection pool created successfully")
        except Exception as e:
            logger.error(f"Failed to create Redshift connection pool: {e}")
            raise

    @asynccontextmanager
    async def get_connection(self):
        """Get a connection from the Redshift pool."""
        if self._connection_pool is None:
            await self.connect()

        try:
            async with self._connection_pool.acquire() as conn:
                yield conn
        except Exception as e:
            logger.error(f"Redshift connection error: {e}")
            raise

    async def execute_query(self, sql: str) -> List[Dict[str, Any]]:
        """Execute SQL query on Redshift and return results."""
        async with self.get_connection() as conn:
            try:
                rows = await conn.fetch(sql)
                return [dict(row) for row in rows]
            except Exception as e:
                logger.error(f"Query execution error: {e}")
                raise

    async def get_schemas(self) -> List[str]:
        """Get list of user-defined schemas in Redshift."""
        sql = """
            SELECT nspname AS schema_name
            FROM pg_namespace
            WHERE nspname NOT LIKE 'pg_%'
                AND nspname != 'information_schema'
                AND nspname != 'catalog_history'
            ORDER BY schema_name
        """
        async with self.get_connection() as conn:
            rows = await conn.fetch(sql)
            return [row['schema_name'] for row in rows]

    async def get_tables(self, schema: str) -> List[Dict[str, Any]]:
        """
        Get list of tables in a Redshift schema.

        This implementation uses information_schema for compatibility and to avoid
        permission issues with svv_table_info.
        """
        # Use information_schema for compatibility across all databases
        sql = f"""
            SELECT
                table_name,
                table_type
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
                AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        async with self.get_connection() as conn:
            rows = await conn.fetch(sql)
            return [dict(row) for row in rows]

    async def get_table_metadata(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """
        Get column-level metadata for a Redshift table.

        Includes Redshift-specific attributes like distribution keys and sort keys.
        """
        sql = f"""
            SELECT
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                c.ordinal_position
            FROM information_schema.columns c
            WHERE c.table_schema = '{schema}'
                AND c.table_name = '{table}'
            ORDER BY c.ordinal_position
        """
        async with self.get_connection() as conn:
            rows = await conn.fetch(sql)
            return [dict(row) for row in rows]
