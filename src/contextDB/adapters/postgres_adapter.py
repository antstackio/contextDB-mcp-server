"""PostgreSQL database adapter."""

import asyncpg
import logging
from typing import Any, Dict, List
from contextlib import asynccontextmanager

from .base_adapter import BaseDatabaseAdapter

logger = logging.getLogger(__name__)


class PostgresAdapter(BaseDatabaseAdapter):
    """
    Database adapter for PostgreSQL.

    Provides generic PostgreSQL support using standard pg_stat_* views
    and information_schema for metadata discovery.
    """

    @property
    def db_type(self) -> str:
        """Return database type identifier."""
        return "postgres"

    @property
    def system_views(self) -> Dict[str, str]:
        """Return PostgreSQL system views mapping."""
        return {
            'query_performance': 'pg_stat_statements',
            'table_stats': 'pg_stat_user_tables',
            'index_usage': 'pg_stat_user_indexes',
            'table_sizes': 'pg_class',
            'database_stats': 'pg_stat_database'
        }

    def supports_feature(self, feature: str) -> bool:
        """Check if PostgreSQL supports a specific feature."""
        supported_features = {
            'window_functions': True,
            'cte': True,
            'materialized_views': True,
            'full_text_search': True,
            'json_support': True,
            'partitioning': True,
            'inheritance': True,
            'extensions': True,
            # Redshift-specific features not supported
            'distribution_keys': False,
            'sort_keys': False,
            'wlm': False,
        }
        return supported_features.get(feature, False)

    async def connect(self) -> None:
        """Establish PostgreSQL connection pool."""
        try:
            logger.info("Creating PostgreSQL connection pool...")
            pool_config = {k: v for k, v in self.config.items() if k != 'schema'}
            self._connection_pool = await asyncpg.create_pool(
                **pool_config,
                min_size=1,
                max_size=10
            )
            logger.info("PostgreSQL connection pool created successfully")
        except Exception as e:
            logger.error(f"Failed to create PostgreSQL connection pool: {e}")
            raise

    @asynccontextmanager
    async def get_connection(self):
        """Get a connection from the PostgreSQL pool."""
        if self._connection_pool is None:
            await self.connect()

        try:
            async with self._connection_pool.acquire() as conn:
                yield conn
        except Exception as e:
            logger.error(f"PostgreSQL connection error: {e}")
            raise

    async def execute_query(self, sql: str) -> List[Dict[str, Any]]:
        """Execute SQL query on PostgreSQL and return results."""
        async with self.get_connection() as conn:
            try:
                rows = await conn.fetch(sql)
                return [dict(row) for row in rows]
            except Exception as e:
                logger.error(f"Query execution error: {e}")
                raise

    async def get_schemas(self) -> List[str]:
        """Get list of user-defined schemas in PostgreSQL."""
        sql = """
            SELECT nspname AS schema_name
            FROM pg_namespace
            WHERE nspname NOT LIKE 'pg_%'
                AND nspname != 'information_schema'
            ORDER BY schema_name
        """
        async with self.get_connection() as conn:
            rows = await conn.fetch(sql)
            return [row['schema_name'] for row in rows]

    async def get_tables(self, schema: str) -> List[Dict[str, Any]]:
        """
        Get list of tables in a PostgreSQL schema with metadata.

        Returns table information including sizes and row counts from pg_stat views.
        """
        sql = f"""
            SELECT
                t.table_name,
                t.table_type,
                COALESCE(pg_size_pretty(pg_total_relation_size(
                    quote_ident(t.table_schema) || '.' || quote_ident(t.table_name)
                )), 'N/A') as size,
                COALESCE(s.n_live_tup, 0) as estimated_rows
            FROM information_schema.tables t
            LEFT JOIN pg_stat_user_tables s
                ON s.schemaname = t.table_schema
                AND s.relname = t.table_name
            WHERE t.table_schema = '{schema}'
                AND t.table_type = 'BASE TABLE'
            ORDER BY t.table_name
        """
        async with self.get_connection() as conn:
            try:
                rows = await conn.fetch(sql)
                return [dict(row) for row in rows]
            except Exception as e:
                # Fallback to simpler query if pg_stat not available
                logger.warning(f"Falling back to basic table list: {e}")
                fallback_sql = f"""
                    SELECT
                        table_name,
                        table_type
                    FROM information_schema.tables
                    WHERE table_schema = '{schema}'
                        AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """
                rows = await conn.fetch(fallback_sql)
                return [dict(row) for row in rows]

    async def get_table_metadata(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get column-level metadata for a PostgreSQL table."""
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
