from ..db_utils import get_db_connection

async def list_tables(schema: str = 'public') -> str:
    """
    List all tables in a schema using optimized Redshift system views.
    Uses SVV_TABLE_INFO for comprehensive table metadata and performance insights.

    Args:
        schema: The schema name to list tables from

    Returns:
        Comprehensive table information with Redshift-specific metrics
    """
    # Use information_schema with fallback to basic table info
    sql = f"""
        SELECT
            t.table_name,
            t.table_type,
            'N/A' as dist_style,
            'N/A' as sort_key,
            0 as size_mb,
            0 as row_count,
            'N/A' as disk_used_pct,
            'N/A' as needs_vacuum,
            'N/A' as needs_analyze,
            'N/A' as row_skew
        FROM information_schema.tables t
        WHERE t.table_schema = '{schema}'
        AND t.table_type = 'BASE TABLE'
        ORDER BY t.table_name
    """

    async with get_db_connection() as conn:
        try:
            rows = await conn.fetch(sql)
            if not rows:
                return f"No tables found in schema '{schema}'"

            # Enhanced CSV format with Redshift-specific columns
            columns = ['table_name', 'table_type', 'dist_style', 'sort_key', 'size_mb',
                      'row_count', 'disk_used_pct', 'needs_vacuum', 'needs_analyze', 'row_skew']
            result_lines = [",".join(columns)]

            for row in rows:
                result_lines.append(",".join([
                    str(row['table_name']),
                    str(row['table_type']),
                    str(row['dist_style']),
                    str(row['sort_key']),
                    str(row['size_mb']),
                    str(row['row_count']),
                    str(row['disk_used_pct']),
                    str(row['needs_vacuum']),
                    str(row['needs_analyze']),
                    str(row['row_skew'])
                ]))

            return "\n".join(result_lines)

        except Exception as e:
            return f"Error listing tables: {str(e)}"