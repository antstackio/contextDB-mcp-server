#!/usr/bin/env python3
"""
FastMCP Redshift Server - A Model Context Protocol server for Redshift database operations.
"""


from typing import Any, Optional
from contextlib import asynccontextmanager
import logging
import asyncpg
import json
import asyncio
import os

from mcp.server.fastmcp import FastMCP

# Import db_utils
from src.redshift_mcp_server.db_utils import get_db_connection, DATABASE_CONFIG

# Import tool functions
from src.redshift_mcp_server.tools.execute_sql import execute_sql
from src.redshift_mcp_server.tools.discover_schema_metadata import discover_schema_metadata
from src.redshift_mcp_server.tools.find_table_dependencies import find_table_dependencies
from src.redshift_mcp_server.tools.list_schemas import list_schemas as list_schemas_from_tool

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastMCP server
mcp = FastMCP("Redshift Database Server")

# Decorate and register tool functions
@mcp.tool()
async def execute_sql_tool(sql: str) -> str:
    """Execute a SQL Query on the Redshift cluster."""
    return await execute_sql(sql)

@mcp.tool()
async def discover_schema_metadata_tool(schema: str) -> str:
    """Discover metadata for a database schema."""
    return await discover_schema_metadata(schema)


@mcp.tool()
async def find_table_dependencies_tool(schema: str, table: str) -> str:
    """Find dependencies for a specific table."""
    return await find_table_dependencies(schema, table)

@mcp.tool()
async def list_schemas_tool() -> str:
    """List all schemas in the Redshift database."""
    return await list_schemas_from_tool()

@mcp.tool()
async def list_tables_in_schema_tool(schema: str) -> str:
    """List all tables in a specific schema."""
    return await list_tables(schema)

# Resource handlers
@mcp.resource("rs://schemas")
async def list_schemas() -> str:
    """List all schemas in the Redshift database."""
    sql = """
        SELECT nspname AS schema_name
        FROM pg_namespace
        WHERE nspname NOT LIKE 'pg_%'
            AND nspname != 'information_schema'
            AND nspname != 'catalog_history'
        ORDER BY schema_name
    """
    
    async with get_db_connection() as conn:
        try:
            rows = await conn.fetch(sql)
            schemas = [row['schema_name'] for row in rows]
            return "\n".join(schemas)
        except Exception as e:
            return f"Error listing schemas: {str(e)}"

@mcp.resource("rs://{schema}/tables")
async def list_tables(schema: str) -> str:
    """List all tables in a specific schema."""
    sql = f"""
        SELECT table_name 
        FROM information_schema.tables
        WHERE table_schema = '{schema}'
        ORDER BY table_name
    """
    
    async with get_db_connection() as conn:
        try:
            rows = await conn.fetch(sql)
            tables = [row['table_name'] for row in rows]
            return "\n".join(tables)
        except Exception as e:
            return f"Error listing tables for schema '{schema}': {str(e)}"

@mcp.resource("rs://{schema}/{table}/ddl")
async def get_table_ddl(schema: str, table: str) -> str:
    """Get the DDL for a specific table."""
    sql = f"""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = '{schema}' AND table_name = '{table}'
        ORDER BY ordinal_position
    """
    
    async with get_db_connection() as conn:
        try:
            rows = await conn.fetch(sql)
            if not rows:
                return f"No DDL found for {schema}.{table}"
            
            ddl_parts = [f"-- DDL for table {schema}.{table}"]
            ddl_parts.append(f"CREATE TABLE {schema}.{table} (")
            
            column_definitions = []
            for row in rows:
                nullable = "NULL" if row['is_nullable'] == 'YES' else "NOT NULL"
                default = f" DEFAULT {row['column_default']}" if row['column_default'] else ""
                column_definitions.append(f"    {row['column_name']} {row['data_type']} {nullable}{default}")
            
            ddl_parts.append(",\n".join(column_definitions))
            ddl_parts.append(");")
            
            return "\n".join(ddl_parts)
            
        except Exception as e:
            return f"Error getting DDL for {schema}.{table}: {str(e)}"

if __name__ == "__main__":
    # For STDIO transport (required for settings.json integration)
    mcp.run()