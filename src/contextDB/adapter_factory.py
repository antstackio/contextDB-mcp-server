"""Factory for creating database adapters dynamically."""

import logging

from .adapters.base_adapter import BaseDatabaseAdapter
from .adapters.redshift_adapter import RedshiftAdapter
from .adapters.postgres_adapter import PostgresAdapter
from .adapters.aurora_adapter import AuroraAdapter

logger = logging.getLogger(__name__)


def create_adapter(database_type: str, config: dict) -> BaseDatabaseAdapter:
    """
    Create appropriate database adapter based on configuration.

    Args:
        database_type: Database type (redshift|postgres|aurora)
        config: Configuration dictionary with connection details
                (host, port, database, user, password, schema)

    Returns:
        Database adapter instance (RedshiftAdapter, PostgresAdapter, or AuroraAdapter)

    Raises:
        ValueError: If database type is not supported

    Example:
        config = {
            'host': 'localhost',
            'port': 5439,
            'database': 'mydb',
            'user': 'admin',
            'password': 'secret',
            'schema': 'public'
        }
        adapter = create_adapter('redshift', config)
    """
    db_type = database_type.lower()

    # Create appropriate adapter
    if db_type == 'redshift':
        logger.info("Creating Redshift adapter")
        return RedshiftAdapter(config)
    elif db_type in ('postgres', 'postgresql'):
        logger.info("Creating PostgreSQL adapter")
        return PostgresAdapter(config)
    elif db_type == 'aurora':
        logger.info("Creating Aurora PostgreSQL adapter")
        return AuroraAdapter(config)
    else:
        raise ValueError(
            f"Unsupported database type: {db_type}. "
            f"Supported types: redshift, postgres, aurora"
        )
