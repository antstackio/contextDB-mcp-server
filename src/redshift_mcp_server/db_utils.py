import os
import asyncpg
from contextlib import asynccontextmanager
import logging

logger = logging.getLogger(__name__)

def get_database_config():
    """Get database configuration from environment variables."""
    return {
        "host": os.getenv("REDSHIFT_HOST", "localhost"),
        "port": int(os.getenv("REDSHIFT_PORT", "5439")),
        "database": os.getenv("REDSHIFT_DATABASE", "dev"),
        "user": os.getenv("REDSHIFT_USER", "admin"),
        "password": os.getenv("REDSHIFT_PASSWORD", ""),
        "schema": os.getenv("REDSHIFT_SCHEMA", "public"),
        
    }

DATABASE_CONFIG = get_database_config()

connection_pool = None

@asynccontextmanager
async def get_db_connection():
    """Get a database connection from the pool."""
    global connection_pool
    try:
        if connection_pool is None:
            logger.info("Creating connection pool...")
            pool_config = {k: v for k, v in DATABASE_CONFIG.items() if k != 'schema'}
            connection_pool = await asyncpg.create_pool(
                **pool_config,
                ssl=False,
                min_size=1,
                max_size=10
            )
        
        async with connection_pool.acquire() as conn:
            yield conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise