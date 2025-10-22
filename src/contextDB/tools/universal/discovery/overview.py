async def get_database_overview(adapter) -> str:
    """
    Get executive summary of the entire database including schemas, tables, and data scale.

    Args:
        adapter: Database adapter instance

    Returns:
        Comprehensive overview with schema count, table count, total size,
        top schemas by size, and recommended starting points for exploration
    """
    async with adapter.get_connection() as conn:
        try:
            # Get schema statistics
            schema_stats_sql = """
                SELECT
                    COUNT(DISTINCT table_schema) as schema_count,
                    COUNT(*) as total_tables
                FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'catalog_history')
                    AND table_type = 'BASE TABLE'
            """

            stats = await conn.fetchrow(schema_stats_sql)

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

            # Get total column count
            column_count_sql = """
                SELECT COUNT(*) as total_columns
                FROM information_schema.columns
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'catalog_history')
            """

            column_stats = await conn.fetchrow(column_count_sql)

            # Build overview report
            result_lines = [
                "=" * 60,
                "DATABASE OVERVIEW - Executive Summary",
                "=" * 60,
                "",
                "📊 **Scale Metrics:**",
                f"   • Total Schemas: {stats['schema_count']}",
                f"   • Total Tables: {stats['total_tables']}",
                f"   • Total Columns: {column_stats['total_columns']}",
                "",
                "📁 **Top 10 Schemas by Table Count:**",
            ]

            for idx, schema in enumerate(top_schemas, 1):
                result_lines.append(f"   {idx}. {schema['table_schema']}: {schema['table_count']} tables")

            result_lines.extend([
                "",
                "🎯 **Recommended Exploration Path:**",
                "   1. Start with largest schemas (listed above)",
                "   2. Use list_tables_tool() to see tables in a schema",
                "   3. Use discover_schema_metadata_tool() for column details",
                "   4. Use get_table_profile_tool() for specific table deep-dive",
                "",
                "💡 **Quick Start:**",
                f"   → Try: list_tables_tool(schema_name='{top_schemas[0]['table_schema']}')"
                if top_schemas else "   → Use list_schemas_tool() to see available schemas",
                "",
                "=" * 60
            ])

            return "\n".join(result_lines)

        except Exception as e:
            return f"Error generating database overview: {str(e)}"
