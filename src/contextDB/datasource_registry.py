"""
Datasource Registry - Manages multiple database connections for MCP server.

This module provides a centralized registry for managing multiple datasource connections,
enabling AI assistants to work with multiple databases simultaneously.
"""

import json
import os
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from .adapter_factory import create_adapter
from .adapters.base_adapter import BaseDatabaseAdapter

logger = logging.getLogger(__name__)


@dataclass
class DatasourceConfig:
    """Configuration for a single datasource."""
    id: str
    type: str
    name: str
    description: str
    connection: Dict[str, Any]
    enabled: bool
    metadata: Optional[Dict[str, Any]] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DatasourceConfig':
        """Create DatasourceConfig from dictionary."""
        return cls(
            id=data['id'],
            type=data['type'],
            name=data['name'],
            description=data.get('description', ''),
            connection=data['connection'],
            enabled=data.get('enabled', True),
            metadata=data.get('metadata')
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary (for serialization)."""
        return {
            'id': self.id,
            'type': self.type,
            'name': self.name,
            'description': self.description,
            'connection': self.connection,
            'enabled': self.enabled,
            'metadata': self.metadata
        }


class DatasourceRegistry:
    """
    Central registry for managing multiple datasource connections.

    Responsibilities:
    - Load datasource configurations from JSON
    - Create and cache database adapters
    - Provide connection management

    Usage:
        registry = DatasourceRegistry('datasources.json')
        adapter = registry.get_adapter('analytics_warehouse')
        async with adapter.get_connection() as conn:
            results = await conn.fetch("SELECT * FROM table")
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize datasource registry.

        Args:
            config_path: Path to datasources.json file.
                        If None, looks for DATASOURCES_CONFIG env var,
                        then falls back to 'datasources.json' in current dir.
        """
        self.datasources: Dict[str, DatasourceConfig] = {}
        self.adapters: Dict[str, BaseDatabaseAdapter] = {}

        # Determine config path
        if config_path is None:
            config_path = os.getenv('DATASOURCES_CONFIG', 'datasources.json')

        self.config_path = config_path

        # Load configuration
        if os.path.exists(config_path):
            self.load_config(config_path)
            logger.info(f"Loaded {len(self.datasources)} datasources from {config_path}")
        else:
            raise FileNotFoundError(
                f"Datasource config not found: {config_path}\n"
                f"Please create a datasources.json file. See datasources.example.json for template."
            )

    def load_config(self, config_path: str):
        """
        Load datasource configurations from JSON file.

        Args:
            config_path: Path to datasources.json

        Raises:
            ValueError: If config format is invalid
            FileNotFoundError: If config file doesn't exist
        """
        with open(config_path, 'r') as f:
            config_data = json.load(f)

        if 'datasources' not in config_data:
            raise ValueError("Config must contain 'datasources' key")

        for ds_data in config_data['datasources']:
            # Validate required fields
            required = ['id', 'type', 'name', 'connection']
            for field in required:
                if field not in ds_data:
                    raise ValueError(f"Datasource missing required field: {field}")

            # Substitute environment variables in passwords
            connection = ds_data['connection'].copy()
            if 'password' in connection:
                password = connection['password']
                if password.startswith('${') and password.endswith('}'):
                    env_var = password[2:-1]
                    connection['password'] = os.getenv(env_var, '')

            ds_data['connection'] = connection

            # Create datasource config
            ds_config = DatasourceConfig.from_dict(ds_data)

            # Only load enabled datasources
            if ds_config.enabled:
                self.datasources[ds_config.id] = ds_config
                logger.info(f"Registered datasource: {ds_config.id} ({ds_config.type})")

    def register_datasource(self, config: DatasourceConfig):
        """
        Manually register a datasource.

        Args:
            config: DatasourceConfig instance
        """
        self.datasources[config.id] = config
        logger.info(f"Registered datasource: {config.id}")

    def get_datasource(self, datasource_id: str) -> DatasourceConfig:
        """
        Get datasource configuration by ID.

        Args:
            datasource_id: Datasource identifier

        Returns:
            DatasourceConfig object

        Raises:
            KeyError: If datasource_id not found
        """
        if datasource_id not in self.datasources:
            available = ', '.join(self.datasources.keys())
            raise KeyError(
                f"Datasource '{datasource_id}' not found. "
                f"Available datasources: {available}"
            )

        return self.datasources[datasource_id]

    def get_adapter(self, datasource_id: str) -> BaseDatabaseAdapter:
        """
        Get database adapter for a datasource.

        Creates adapter on first access and caches it for reuse.

        Args:
            datasource_id: Datasource identifier

        Returns:
            BaseDatabaseAdapter instance (RedshiftAdapter, PostgresAdapter, etc.)

        Raises:
            KeyError: If datasource_id not found
        """
        # Return cached adapter if exists
        if datasource_id in self.adapters:
            return self.adapters[datasource_id]

        # Get datasource config
        ds_config = self.get_datasource(datasource_id)

        # Create new adapter
        adapter = create_adapter(
            database_type=ds_config.type,
            config=ds_config.connection
        )

        # Cache adapter
        self.adapters[datasource_id] = adapter
        logger.info(f"Created adapter for datasource: {datasource_id}")

        return adapter

    def list_all(self) -> List[DatasourceConfig]:
        """
        Get list of all registered datasources.

        Returns:
            List of DatasourceConfig objects
        """
        return list(self.datasources.values())

    def has_type(self, db_type: str) -> bool:
        """
        Check if any datasource of given type exists.

        Args:
            db_type: Database type (e.g., 'redshift', 'postgres', 'aurora')

        Returns:
            True if at least one datasource of this type exists
        """
        return any(ds.type == db_type for ds in self.datasources.values())

    def get_by_type(self, db_type: str) -> List[DatasourceConfig]:
        """
        Get all datasources of a specific type.

        Args:
            db_type: Database type to filter by

        Returns:
            List of datasources matching the type
        """
        return [ds for ds in self.datasources.values() if ds.type == db_type]

    async def close_all(self):
        """Close all adapter connections."""
        for datasource_id, adapter in self.adapters.items():
            try:
                await adapter.close()
                logger.info(f"Closed adapter: {datasource_id}")
            except Exception as e:
                logger.error(f"Error closing adapter {datasource_id}: {e}")

        self.adapters.clear()

    def __repr__(self) -> str:
        return f"DatasourceRegistry(datasources={len(self.datasources)})"
