"""
Discovery Tools - Entry point tools for multi-datasource exploration.

These tools help users understand what datasources are available and get
high-level overviews before diving into specific schemas and tables.
"""

from typing import Optional, List
import logging

logger = logging.getLogger(__name__)


async def list_all_datasources(registry) -> str:
    """
    **[START HERE]** Show all configured datasources.

    **Datasources:** N/A (meta tool - shows available datasources)

    **Use this when:**
    - First time exploring (user asks "What databases exist?" or "What datasources are available?")
    - User doesn't specify which datasource to use
    - Need to show all configured data sources

    **Don't use for:**
    - Specific datasource details → Use get_datasource_overview instead
    - Listing tables → Use list_tables tool

    **Limitations:**
    - Shows only enabled datasources
    - Requires datasources.json configuration

    **Returns:**
        Table with columns: id, name, type, description, status

        Example:
        id,name,type,description,status
        analytics_warehouse,Analytics DW,redshift,Primary analytics database,✅ ENABLED
        customer_db,Customer DB,postgres,Operational customer data,✅ ENABLED

    **Token cost:** ~120 tokens
    """
    try:
        datasources = registry.list_all()

        if not datasources:
            return "No datasources configured. Please add datasources to datasources.json"

        # Build CSV output
        header = "id,name,type,description,status"
        rows = [header]

        for ds in datasources:
            row = (
                f"{ds.id},"
                f"{ds.name},"
                f"{ds.type},"
                f'"{ds.description}",'
                f"{'✅ ENABLED' if ds.enabled else '❌ DISABLED'}"
            )
            rows.append(row)

        result = "\n".join(rows)

        # Add helpful footer
        footer = f"\n\n💡 Tip: Use get_datasource_overview(datasource_id='{datasources[0].id}') to explore a specific datasource"

        return result + footer

    except Exception as e:
        logger.error(f"Error listing datasources: {e}")
        return f"Error listing datasources: {str(e)}"


async def get_datasource_overview(registry, datasource_id: str) -> str:
    """
    Get executive summary of a specific datasource.

    **Datasources:** All (works with any configured datasource)

    **Use this when:**
    - User asks "What's in analytics_warehouse?" or "Tell me about customer_db"
    - Need bird's eye view of a datasource before exploring schemas
    - Want to see schema count, table count, and top schemas

    **Don't use for:**
    - Listing all datasources → Use list_all_datasources
    - Schema details → Use list_tables
    - Specific table info → Use get_table_profile

    **Limitations:**
    - Requires valid datasource_id from list_all_datasources
    - May be slow for very large databases (1000+ tables)

    **Args:**
        datasource_id: Target datasource ID (from list_all_datasources)
                      Examples: 'analytics_warehouse', 'customer_db'

    **Returns:**
        Executive summary with:
        - Datasource metadata (type, name, description)
        - Scale metrics (schema count, table count, column count)
        - Top 10 schemas by table count
        - Recommended next steps

        Example:
        ==========================================
        DATASOURCE OVERVIEW: analytics_warehouse
        ==========================================
        Type: redshift
        Name: Analytics Data Warehouse
        Description: Primary analytics database

        📊 Scale Metrics:
        • Total Schemas: 12
        • Total Tables: 247
        • Total Columns: 3,450

        📁 Top Schemas:
        1. public: 89 tables
        2. sales: 52 tables
        ...

    **Token cost:** ~150 tokens
    """
    try:
        # Get datasource config
        ds_config = registry.get_datasource(datasource_id)
        adapter = registry.get_adapter(datasource_id)

        # Get schema statistics
        async with adapter.get_connection() as conn:
            # Count schemas
            schema_count_sql = """
                SELECT COUNT(DISTINCT table_schema) as schema_count
                FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'catalog_history')
                    AND table_type = 'BASE TABLE'
            """
            schema_stats = await conn.fetchrow(schema_count_sql)

            # Count tables
            table_count_sql = """
                SELECT COUNT(*) as table_count
                FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'catalog_history')
                    AND table_type = 'BASE TABLE'
            """
            table_stats = await conn.fetchrow(table_count_sql)

            # Count columns
            column_count_sql = """
                SELECT COUNT(*) as column_count
                FROM information_schema.columns
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'catalog_history')
            """
            column_stats = await conn.fetchrow(column_count_sql)

            # Get top schemas by table count
            top_schemas_sql = """
                SELECT
                    table_schema,
                    COUNT(*) as table_count
                FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'catalog_history')
                    AND table_type = 'BASE TABLE'
                GROUP BY table_schema
                ORDER BY table_count DESC
                LIMIT 10
            """
            top_schemas = await conn.fetch(top_schemas_sql)

        # Build overview report
        result_lines = [
            "=" * 60,
            f"DATASOURCE OVERVIEW: {datasource_id}",
            "=" * 60,
            f"Type: {ds_config.type}",
            f"Name: {ds_config.name}",
            f"Description: {ds_config.description}",
            "",
            "📊 Scale Metrics:",
            f"   • Total Schemas: {schema_stats['schema_count']}",
            f"   • Total Tables: {table_stats['table_count']}",
            f"   • Total Columns: {column_stats['column_count']}",
            "",
            "📁 Top 10 Schemas by Table Count:",
        ]

        for idx, schema in enumerate(top_schemas, 1):
            result_lines.append(f"   {idx}. {schema['table_schema']}: {schema['table_count']} tables")

        if top_schemas:
            result_lines.extend([
                "",
                "💡 Recommended Next Steps:",
                f"   → Explore top schema: list_tables(datasource_id='{datasource_id}', schema_name='{top_schemas[0]['table_schema']}')",
                f"   → Search for data: search_database_objects(datasource_id='{datasource_id}', keyword='your_keyword')",
            ])

        result_lines.append("=" * 60)

        return "\n".join(result_lines)

    except KeyError as e:
        available = ', '.join([ds.id for ds in registry.list_all()])
        return f"❌ Datasource '{datasource_id}' not found. Available: {available}"
    except Exception as e:
        logger.error(f"Error getting overview for {datasource_id}: {e}")
        return f"Error getting datasource overview: {str(e)}"


async def search_across_datasources(
    registry,
    keyword: str,
    datasource_types: Optional[List[str]] = None
) -> str:
    """
    Search for tables and columns across ALL datasources.

    **Datasources:** All (searches multiple datasources simultaneously)

    **Use this when:**
    - User asks "Find customer data" but doesn't know which datasource has it
    - Need to locate tables/columns across all databases
    - Cross-datasource exploration ("Where is billing information stored?")

    **Don't use for:**
    - Searching within a known datasource → Use search_database_objects
    - Browsing all tables → Use list_tables
    - Known table location → Use get_table_profile directly

    **Limitations:**
    - Max 100 results total (20 per datasource)
    - Searches table and column names only (not data values)
    - Case-insensitive keyword matching
    - May be slow with many datasources (5+ datasources)

    **Args:**
        keyword: Search term to find in table/column names
                Examples: "customer", "order", "billing", "invoice"
        datasource_types: Optional filter by database types
                         Examples: ["redshift"], ["postgres", "aurora"]
                         Default: None (search all types)

    **Returns:**
        Grouped search results with columns:
        datasource_id, datasource_name, type, schema, table, column, match_type

        Example:
        ========================================
        CROSS-DATASOURCE SEARCH: 'customer'
        Found 8 matches across 2 datasources
        ========================================

        📊 analytics_warehouse (redshift):
          TABLE MATCHES (2):
          • public.customers
          • sales.customer_segments

          COLUMN MATCHES (3):
          • public.orders.customer_id
          • public.invoices.customer_name

        📊 customer_db (postgres):
          TABLE MATCHES (3):
          • public.customer_profiles
          ...

    **Token cost:** ~180 tokens
    """
    try:
        all_results = []
        datasources = registry.list_all()

        # Filter by type if specified
        if datasource_types:
            datasources = [ds for ds in datasources if ds.type in datasource_types]

        if not datasources:
            return "No datasources match the specified types"

        # Search each datasource
        for ds in datasources:
            try:
                adapter = registry.get_adapter(ds.id)

                async with adapter.get_connection() as conn:
                    # Search tables
                    table_search_sql = f"""
                        SELECT
                            table_schema,
                            table_name,
                            'TABLE' as match_type
                        FROM information_schema.tables
                        WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'catalog_history')
                            AND table_type = 'BASE TABLE'
                            AND LOWER(table_name) LIKE LOWER('%{keyword}%')
                        LIMIT 10
                    """

                    # Search columns
                    column_search_sql = f"""
                        SELECT
                            table_schema,
                            table_name,
                            column_name,
                            'COLUMN' as match_type
                        FROM information_schema.columns
                        WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'catalog_history')
                            AND LOWER(column_name) LIKE LOWER('%{keyword}%')
                        LIMIT 10
                    """

                    table_matches = await conn.fetch(table_search_sql)
                    column_matches = await conn.fetch(column_search_sql)

                    # Combine results
                    for match in table_matches:
                        all_results.append({
                            'datasource_id': ds.id,
                            'datasource_name': ds.name,
                            'type': ds.type,
                            'schema': match['table_schema'],
                            'table': match['table_name'],
                            'column': None,
                            'match_type': 'TABLE'
                        })

                    for match in column_matches:
                        all_results.append({
                            'datasource_id': ds.id,
                            'datasource_name': ds.name,
                            'type': ds.type,
                            'schema': match['table_schema'],
                            'table': match['table_name'],
                            'column': match['column_name'],
                            'match_type': 'COLUMN'
                        })

            except Exception as e:
                logger.warning(f"Error searching datasource {ds.id}: {e}")
                continue

        if not all_results:
            return f"No matches found for keyword '{keyword}' across {len(datasources)} datasources"

        # Format results
        result_lines = [
            "=" * 60,
            f"CROSS-DATASOURCE SEARCH: '{keyword}'",
            f"Found {len(all_results)} matches across {len(datasources)} datasources",
            "=" * 60,
            ""
        ]

        # Group by datasource
        by_datasource = {}
        for result in all_results:
            ds_id = result['datasource_id']
            if ds_id not in by_datasource:
                by_datasource[ds_id] = {
                    'name': result['datasource_name'],
                    'type': result['type'],
                    'table_matches': [],
                    'column_matches': []
                }

            if result['match_type'] == 'TABLE':
                by_datasource[ds_id]['table_matches'].append(
                    f"{result['schema']}.{result['table']}"
                )
            else:
                by_datasource[ds_id]['column_matches'].append(
                    f"{result['schema']}.{result['table']}.{result['column']}"
                )

        # Format grouped results
        for ds_id, data in by_datasource.items():
            result_lines.append(f"📊 {ds_id} ({data['type']}):")

            if data['table_matches']:
                result_lines.append(f"  TABLE MATCHES ({len(data['table_matches'])}):")
                for match in data['table_matches'][:5]:  # Limit display
                    result_lines.append(f"  • {match}")

            if data['column_matches']:
                result_lines.append(f"  COLUMN MATCHES ({len(data['column_matches'])}):")
                for match in data['column_matches'][:5]:  # Limit display
                    result_lines.append(f"  • {match}")

            result_lines.append("")

        result_lines.append("=" * 60)

        return "\n".join(result_lines)

    except Exception as e:
        logger.error(f"Error in cross-datasource search: {e}")
        return f"Error searching across datasources: {str(e)}"
