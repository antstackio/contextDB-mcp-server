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

from fastmcp import FastMCP

# Import db_utils
from src.redshift_mcp_server.db_utils import get_db_connection, DATABASE_CONFIG

# Import tool functions
from src.redshift_mcp_server.tools.execute_sql import execute_sql
from src.redshift_mcp_server.tools.discover_schema_metadata import discover_schema_metadata
from src.redshift_mcp_server.tools.find_table_dependencies import find_table_dependencies
from src.redshift_mcp_server.tools.list_schemas import list_schemas as list_schemas_from_tool
from src.redshift_mcp_server.tools.list_tables import list_tables
from src.redshift_mcp_server.tools.table_profile import get_table_profile
from src.redshift_mcp_server.tools.query_performance import analyze_query_performance
from src.redshift_mcp_server.tools.data_distribution import analyze_data_distribution
from src.redshift_mcp_server.tools.business_metrics import get_business_metrics
from src.redshift_mcp_server.tools.data_quality import check_data_quality

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
    """Execute a SELECT query on the Redshift cluster. Only SELECT statements allowed for safety. Returns results in CSV format."""
    return await execute_sql(sql)

@mcp.tool()
async def discover_schema_metadata_tool(schema: str) -> str:
    """Get comprehensive schema metadata including tables, columns, data types, and Redshift-specific constraints (dist keys, sort keys)."""
    return await discover_schema_metadata(schema)


@mcp.tool()
async def find_table_dependencies_tool(schema: str, table: str) -> str:
    """Map table relationships and foreign key dependencies to understand data lineage and table interconnections."""
    return await find_table_dependencies(schema, table)

@mcp.tool()
async def list_schemas_tool() -> str:
    """List all user-defined schemas in the Redshift database. Excludes system schemas like pg_catalog."""
    return await list_schemas_from_tool()

@mcp.tool()
async def list_tables_tool(schema: str = 'public') -> str:
    """Get comprehensive table listing with Redshift-specific metrics: sizes, row counts, distribution styles, sort keys, and maintenance needs (VACUUM/ANALYZE)."""
    return await list_tables(schema)

@mcp.tool()
async def get_table_profile_tool(schema: str, table: str) -> str:
    """Generate detailed table profile with column analysis, data types, row counts, and data quality metrics for business intelligence."""
    return await get_table_profile(schema, table)

@mcp.tool()
async def analyze_query_performance_tool(limit: int = 10) -> str:
    """Analyze recent query performance using Redshift system tables. Identifies slow queries, queue times, and provides optimization insights."""
    return await analyze_query_performance(limit)

@mcp.tool()
async def analyze_data_distribution_tool(schema: str, table: str, column: str | None = None) -> str:
    """Analyze data distribution across Redshift nodes. Identifies data skew issues that impact query performance and provides optimization recommendations."""
    return await analyze_data_distribution(schema, table, column)

@mcp.tool()
async def get_business_metrics_tool(schema: str = 'public', days: int = 30) -> str:
    """Generate comprehensive business intelligence dashboard with data usage patterns, query trends, table health metrics, and ROI insights."""
    return await get_business_metrics(schema, days)

@mcp.tool()
async def check_data_quality_tool(schema: str, table: str) -> str:
    """Perform comprehensive data quality assessment including completeness, null analysis, and data integrity scores for business decision making."""
    return await check_data_quality(schema, table)

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
    print("Starting MCP Server with HTTP transport on http://127.0.0.1:8000")
    # Use fastmcp's run method to start HTTP server
    mcp.run(transport="http", host="127.0.0.1", port=8000)
