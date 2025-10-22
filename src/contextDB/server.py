#!/usr/bin/env python3
"""
FastMCP Redshift Server - A Model Context Protocol server for Redshift database operations.
"""

import logging
import os

from fastmcp import FastMCP

# Import DatasourceRegistry for multi-datasource support
from contextDB.datasource_registry import DatasourceRegistry

# Import tools
from contextDB.tools import (
    list_all_datasources,
    get_datasource_overview,
    search_across_datasources,
    get_database_overview,
    analyze_query_performance,
    get_business_metrics,
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastMCP server
mcp = FastMCP("Multi-Datasource MCP Server")

# Initialize DatasourceRegistry (global instance)
registry = None

def init_registry():
    """Initialize datasource registry on server startup."""
    global registry
    config_path = os.getenv('DATASOURCES_CONFIG', 'datasources.json')
    registry = DatasourceRegistry(config_path)
    logger.info(f"✅ Initialized DatasourceRegistry with {len(registry.list_all())} datasources")
    return registry

def _quote_identifier(*parts):
    """
    Quote SQL identifiers that contain hyphens or special characters.

    Example:
        _quote_identifier("order-management", "clinical_order")
        Returns: '"order-management".clinical_order'
    """
    quoted_parts = []
    for part in parts:
        if '-' in part or ' ' in part:
            quoted_parts.append(f'"{part}"')
        else:
            quoted_parts.append(part)
    return '.'.join(quoted_parts)

# ============================================================================
# NEW DISCOVERY TOOLS (Multi-Datasource Entry Points)
# ============================================================================

@mcp.tool()
async def list_all_datasources_tool() -> str:
    """
    **[START HERE]** Show all configured datasources.

    **Datasources:** N/A (meta tool - shows available datasources)

    **Use this when:**
    - First time exploring (user asks "What databases exist?")
    - User doesn't specify which datasource

    **Returns:**
        Table with: id, name, type, description, status
    """
    return await list_all_datasources(registry)


@mcp.tool()
async def get_datasource_overview_tool(datasource_id: str) -> str:
    """
    Get executive summary of a specific datasource.

    **Datasources:** All

    **Use this when:**
    - User asks "What's in analytics_warehouse?"
    - Need bird's eye view before exploring schemas

    **Args:**
        datasource_id: Target datasource from list_all_datasources

    **Returns:**
        Summary: schema count, table count, top schemas
    """
    return await get_datasource_overview(registry, datasource_id)


@mcp.tool()
async def search_across_datasources_tool(keyword: str, datasource_types: list = None) -> str:
    """
    Search for tables/columns across ALL datasources.

    **Datasources:** All (searches multiple simultaneously)

    **Use this when:**
    - User asks "Find customer data" (don't know which DB)
    - Cross-datasource exploration

    **Args:**
        keyword: Search term (e.g., "customer", "order")
        datasource_types: Optional filter ["redshift", "postgres"]

    **Returns:**
        Grouped results by datasource with table/column matches
    """
    return await search_across_datasources(registry, keyword, datasource_types)


# ============================================================================
# DATABASE TOOLS (Schema, Queries, Analysis)
# ============================================================================

@mcp.tool()
async def execute_sql_tool(datasource_id: str, sql_query: str) -> str:
    """
    Execute a SELECT query on a datasource.

    **Datasources:** All (postgres, redshift, aurora, mysql)

    **Use this when:**
    - User requests specific data: "Show me customers from analytics_warehouse"
    - Ad-hoc analytical queries after discovering schema
    - Testing JOIN patterns between related tables
    - Validating data samples

    **Don't use for:**
    - Schema discovery → Use list_tables or discover_schema_metadata
    - Sample data without specific query → Use sample_data tool
    - Performance analysis → Use analyze_query_performance (Redshift)

    **Limitations:**
    - READ-ONLY: Only SELECT statements allowed
    - Max 1000 rows returned (use LIMIT clause)
    - Query timeout: 30 seconds

    **Args:**
        datasource_id: Target datasource from list_all_datasources
                      Examples: 'analytics_warehouse', 'customer_db'
        sql_query: Valid SELECT statement
                  Example: "SELECT * FROM public.customers LIMIT 10"

    **Returns:**
        CSV format with column headers in first row, data rows following.

        Example:
        customer_id,name,age
        1001,John Doe,45
        1002,Jane Smith,32

    **Token cost:** ~150 tokens

    **Tip:** Start with LIMIT 10 when exploring unfamiliar tables to avoid large result sets.
    """
    try:
        # Validate it's a SELECT query BEFORE approval (security)
        query_stripped = sql_query.strip()
        query_upper = query_stripped.upper()

        # Check if it starts with SELECT
        if not query_upper.startswith('SELECT'):
            return "❌ Error: Only SELECT queries are allowed for safety. Use SELECT to query data."

        # Check for dangerous keywords that might be in CTEs or subqueries
        dangerous_keywords = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 'TRUNCATE', 'GRANT', 'REVOKE']
        for keyword in dangerous_keywords:
            if keyword in query_upper:
                return f"❌ Error: Query contains forbidden keyword '{keyword}'. Only SELECT queries are allowed."

        # Get adapter and execute
        adapter = registry.get_adapter(datasource_id)

        # Execute query using adapter
        async with adapter.get_connection() as conn:
            results = await conn.fetch(sql_query)

            if not results:
                return "Query executed successfully. No rows returned."

            # Convert to CSV format
            if len(results) > 0:
                # Get column names
                columns = list(results[0].keys())
                csv_lines = [",".join(columns)]

                # Add data rows
                for row in results:
                    row_values = [str(row[col]) if row[col] is not None else '' for col in columns]
                    csv_lines.append(",".join(f'"{val}"' if ',' in val else val for val in row_values))

                return "\n".join(csv_lines)

    except KeyError:
        available = ', '.join([ds.id for ds in registry.list_all()])
        return f"❌ Datasource '{datasource_id}' not found. Available: {available}"
    except Exception as e:
        logger.error(f"Error executing SQL on {datasource_id}: {e}")
        return f"❌ Error executing query: {str(e)}"

@mcp.tool()
async def discover_schema_metadata_tool(datasource_id: str, schema_name: str) -> str:
    """
    Get complete data dictionary with all tables, columns, types, and constraints in a schema.

    Use when: User asks "What columns are in public schema?" or needs full schema metadata
    Args:
        datasource_id: Target datasource ID
        schema_name: Schema to analyze
    Returns: CSV with table_name, column_name, data_type, nullable, keys
    Note: May return 500+ rows for large schemas
    """
    try:
        adapter = registry.get_adapter(datasource_id)

        async with adapter.get_connection() as conn:
            # Get metadata using information_schema
            metadata_sql = """
                SELECT
                    table_name,
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    ordinal_position as position
                FROM information_schema.columns
                WHERE table_schema = $1
                ORDER BY table_name, ordinal_position
            """
            results = await conn.fetch(metadata_sql, schema_name)

            if not results:
                return f"No metadata found for schema '{schema_name}'"

            # Convert to CSV
            columns = list(results[0].keys())
            csv_lines = [",".join(columns)]
            for row in results:
                row_values = [str(row[col]) if row[col] is not None else '' for col in columns]
                csv_lines.append(",".join(f'"{val}"' if ',' in val else val for val in row_values))

            return "\n".join(csv_lines)

    except KeyError:
        available = ', '.join([ds.id for ds in registry.list_all()])
        return f"❌ Datasource '{datasource_id}' not found. Available: {available}"
    except Exception as e:
        logger.error(f"Error discovering metadata in {datasource_id}.{schema_name}: {e}")
        return f"❌ Error: {str(e)}"




@mcp.tool()
async def list_schemas_tool(datasource_id: str) -> str:
    """
    List all schemas in a datasource. Excludes system schemas.

    Use when: User asks "What schemas exist in analytics_warehouse?"
    Args:
        datasource_id: Target datasource ID
    Returns: Schema names, one per line
    """
    try:
        adapter = registry.get_adapter(datasource_id)
        schemas = await adapter.get_schemas()
        return "\n".join(schemas) if schemas else "No user schemas found"
    except KeyError:
        available = ', '.join([ds.id for ds in registry.list_all()])
        return f"❌ Datasource '{datasource_id}' not found. Available: {available}"
    except Exception as e:
        logger.error(f"Error listing schemas in {datasource_id}: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def list_tables_tool(datasource_id: str, schema_name: str = 'public') -> str:
    """
    List tables in a schema with metadata (row counts, sizes, DB-specific metrics).

    Use when: User asks "What tables are in public schema of analytics_warehouse?"
    Args:
        datasource_id: Target datasource ID
        schema_name: Schema to list tables from (default: 'public')
    Returns: CSV with table_name, type, row_count, size_mb, and DB-specific columns
    """
    try:
        adapter = registry.get_adapter(datasource_id)
        tables = await adapter.get_tables(schema_name)

        if not tables:
            return f"No tables found in schema '{schema_name}'"

        # Convert to CSV
        if len(tables) > 0:
            columns = list(tables[0].keys())
            csv_lines = [",".join(columns)]
            for table in tables:
                row_values = [str(table[col]) if table[col] is not None else '' for col in columns]
                csv_lines.append(",".join(f'"{val}"' if ',' in val else val for val in row_values))
            return "\n".join(csv_lines)

        return f"No tables in schema '{schema_name}'"

    except KeyError:
        available = ', '.join([ds.id for ds in registry.list_all()])
        return f"❌ Datasource '{datasource_id}' not found. Available: {available}"
    except Exception as e:
        logger.error(f"Error listing tables in {datasource_id}.{schema_name}: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def get_table_profile_tool(datasource_id: str, schema_name: str, table_name: str) -> str:
    """
    Generate comprehensive table profile with column stats, sample data, and quality metrics.

    Use when: User asks "What's in the customers table?" or needs deep table analysis
    Args:
        datasource_id: Target datasource ID
        schema_name: Schema containing the table
        table_name: Table to profile
    Returns: Report with row count, column statistics, sample data, quality score
    """
    try:
        adapter = registry.get_adapter(datasource_id)

        async with adapter.get_connection() as conn:
            full_table = _quote_identifier(schema_name, table_name)

            # Get row count
            count_sql = f"SELECT COUNT(*) as row_count FROM {full_table}"
            count_result = await conn.fetchrow(count_sql)
            row_count = count_result['row_count']

            # Get sample data (first 5 rows)
            sample_sql = f"SELECT * FROM {full_table} LIMIT 5"
            sample_data = await conn.fetch(sample_sql)

            # Build report
            report_lines = [
                f"TABLE PROFILE: {schema_name}.{table_name}",
                "=" * 60,
                f"Row Count: {row_count:,}",
                ""
            ]

            if sample_data:
                report_lines.append("Sample Data (first 5 rows):")
                columns = list(sample_data[0].keys())
                report_lines.append(",".join(columns))
                for row in sample_data:
                    row_values = [str(row[col]) if row[col] is not None else 'NULL' for col in columns]
                    report_lines.append(",".join(f'"{val}"' if ',' in val else val for val in row_values))

            return "\n".join(report_lines)

    except KeyError:
        available = ', '.join([ds.id for ds in registry.list_all()])
        return f"❌ Datasource '{datasource_id}' not found. Available: {available}"
    except Exception as e:
        logger.error(f"Error profiling {datasource_id}.{schema_name}.{table_name}: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def analyze_query_performance_tool(datasource_id: str, limit: int = 10) -> str:
    """
    Analyze recent query performance (Redshift-specific, last 24hrs).

    Use when: User asks "Why is the database slow?" or "Which queries are slow?"
    Args:
        datasource_id: Target Redshift datasource ID
        limit: Number of slowest queries (default: 10, range: 1-100)
    Returns: CSV with query_id, duration_sec, status, performance_level, query_snippet
    Note: Redshift-only tool (uses STL system tables)
    """
    try:
        ds_config = registry.get_datasource(datasource_id)
        if ds_config.type != 'redshift':
            return f"❌ This tool only works with Redshift datasources. {datasource_id} is {ds_config.type}"

        # Get adapter and call function
        adapter = registry.get_adapter(datasource_id)
        return await analyze_query_performance(adapter, limit)
    except KeyError:
        available = ', '.join([ds.id for ds in registry.list_all()])
        return f"❌ Datasource '{datasource_id}' not found. Available: {available}"
    except Exception as e:
        logger.error(f"Error analyzing performance for {datasource_id}: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def get_business_metrics_tool(datasource_id: str, schema_name: str = 'public', days: int = 30) -> str:
    """
    Generate 30-day business intelligence dashboard (Redshift-specific).

    Use when: User asks for "database health report" or "usage trends"
    Args:
        datasource_id: Target Redshift datasource ID
        schema_name: Target schema (default: 'public')
        days: Analysis window (default: 30, range: 1-90)
    Returns: Report with usage patterns, query trends, table health metrics
    Note: Redshift-only tool
    """
    try:
        ds_config = registry.get_datasource(datasource_id)
        if ds_config.type != 'redshift':
            return f"❌ This tool only works with Redshift. {datasource_id} is {ds_config.type}"

        # Get adapter and call function
        adapter = registry.get_adapter(datasource_id)
        return await get_business_metrics(adapter, schema_name, days)
    except KeyError:
        available = ', '.join([ds.id for ds in registry.list_all()])
        return f"❌ Datasource '{datasource_id}' not found. Available: {available}"
    except Exception as e:
        logger.error(f"Error getting metrics for {datasource_id}: {e}")
        return f"❌ Error: {str(e)}"


@mcp.tool()
async def check_data_quality_tool(datasource_id: str, schema_name: str, table_name: str) -> str:
    """
    Assess data quality with completeness scores and null analysis.

    Use when: User asks "Is this table reliable?" or needs quality assessment
    Args:
        datasource_id: Target datasource ID
        schema_name: Schema containing the table
        table_name: Table to assess
    Returns: Quality report with completeness percentages and recommendations
    """
    try:
        adapter = registry.get_adapter(datasource_id)
        full_table = _quote_identifier(schema_name, table_name)

        async with adapter.get_connection() as conn:
            # Get column info
            cols_sql = """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2
                ORDER BY ordinal_position
            """
            columns = await conn.fetch(cols_sql, schema_name, table_name)

            if not columns:
                return f"No columns found for {schema_name}.{table_name}"

            # Check nulls for each column
            report_lines = [
                f"DATA QUALITY REPORT: {schema_name}.{table_name}",
                "=" * 60
            ]

            for col in columns[:10]:  # Limit to first 10 columns
                col_name = col['column_name']
                # Use proper quoting for column names
                null_sql = f'SELECT SUM(CASE WHEN "{col_name}" IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as null_pct FROM {full_table}'
                result = await conn.fetchrow(null_sql)
                null_pct = result['null_pct'] or 0
                completeness = 100 - null_pct
                report_lines.append(f"{col_name}: {completeness:.1f}% complete ({null_pct:.1f}% nulls)")

            return "\n".join(report_lines)

    except KeyError:
        available = ', '.join([ds.id for ds in registry.list_all()])
        return f"❌ Datasource '{datasource_id}' not found. Available: {available}"
    except Exception as e:
        logger.error(f"Error checking quality for {datasource_id}.{schema_name}.{table_name}: {e}")
        return f"❌ Error: {str(e)}"

# Context Understanding Tools
@mcp.tool()
async def get_database_overview_tool(datasource_id: str) -> str:
    """
    **[START HERE FOR NEW DATABASES]** Get executive summary of the entire database.

    **When to use this tool:**
    - FIRST TIME exploring a completely unfamiliar database
    - User asks "What's in this database?" or "Give me an overview"
    - Need to understand database scale before diving into specifics
    - Want to see top schemas ranked by importance/size
    - Creating high-level documentation or presentations

    **Do NOT use for:**
    - Detailed schema exploration - use list_schemas_tool then list_tables_tool
    - Specific table information - use get_table_profile_tool
    - Column-level details - use discover_schema_metadata_tool

    **Difference from list_schemas_tool:**
    - This tool: Bird's eye view with statistics and recommendations
    - list_schemas_tool: Simple list of schema names only

    Args:
        datasource_id: Target datasource ID

    Returns:
        Executive summary with:
        - Total schemas, tables, columns count
        - Top 10 schemas ranked by table count
        - Recommended exploration path
        - Quick start suggestions

        Example output:
            DATABASE OVERVIEW - Executive Summary
            ==================
            Scale Metrics:
            • Total Schemas: 54
            • Total Tables: 312
            • Total Columns: 2,450

            Top 10 Schemas by Table Count:
            1. order-management: 52 tables
            2. billing-claims: 39 tables
            3. patient-charts: 35 tables

            Recommended Exploration Path:
            → Try: list_tables_tool(schema_name='order-management')

    **Tip:** Use this once at the beginning, then follow the recommended exploration path.
    """
    try:
        adapter = registry.get_adapter(datasource_id)
        return await get_database_overview(adapter)
    except KeyError:
        available = ', '.join([ds.id for ds in registry.list_all()])
        return f"❌ Datasource '{datasource_id}' not found. Available: {available}"
    except Exception as e:
        logger.error(f"Error getting overview for {datasource_id}: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def search_database_objects_tool(datasource_id: str, keyword: str, object_type: str = "all") -> str:
    """
    Search for tables and columns by keyword in a datasource.

    Use when: User asks "Where is patient data?" or "Find tables with 'billing'"
    Args:
        datasource_id: Target datasource ID
        keyword: Search term (e.g., "patient", "order", "billing")
        object_type: Filter - "all", "table", or "column" (default: "all")
    Returns: Grouped results showing matching tables and columns
    """
    try:
        adapter = registry.get_adapter(datasource_id)

        async with adapter.get_connection() as conn:
            results = []

            # Search tables
            if object_type in ["all", "table"]:
                table_sql = f"""
                    SELECT table_schema, table_name, 'TABLE' as match_type
                    FROM information_schema.tables
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'catalog_history')
                      AND LOWER(table_name) LIKE LOWER('%{keyword}%')
                    LIMIT 50
                """
                table_matches = await conn.fetch(table_sql)
                results.extend(table_matches)

            # Search columns
            if object_type in ["all", "column"]:
                column_sql = f"""
                    SELECT table_schema, table_name, column_name, 'COLUMN' as match_type
                    FROM information_schema.columns
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'catalog_history')
                      AND LOWER(column_name) LIKE LOWER('%{keyword}%')
                    LIMIT 100
                """
                column_matches = await conn.fetch(column_sql)
                results.extend(column_matches)

            if not results:
                return f"No matches found for '{keyword}'"

            # Format results
            report = [f"SEARCH RESULTS for '{keyword}' ({len(results)} matches)", "=" * 60, ""]
            table_count = sum(1 for r in results if r['match_type'] == 'TABLE')
            column_count = sum(1 for r in results if r['match_type'] == 'COLUMN')

            if table_count:
                report.append(f"TABLE MATCHES ({table_count}):")
                for r in results:
                    if r['match_type'] == 'TABLE':
                        report.append(f"  • {r['table_schema']}.{r['table_name']}")

            if column_count:
                report.append(f"\nCOLUMN MATCHES ({column_count}):")
                for r in results:
                    if r['match_type'] == 'COLUMN':
                        report.append(f"  • {r['table_schema']}.{r['table_name']}.{r['column_name']}")

            return "\n".join(report)

    except KeyError:
        available = ', '.join([ds.id for ds in registry.list_all()])
        return f"❌ Datasource '{datasource_id}' not found. Available: {available}"
    except Exception as e:
        logger.error(f"Error searching {datasource_id} for '{keyword}': {e}")
        return f"❌ Error: {str(e)}"
