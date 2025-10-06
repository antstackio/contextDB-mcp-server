from ..db_utils import get_db_connection

async def get_table_profile(schema: str, table: str) -> str:
    """
    Get comprehensive table profile including statistics, column details, and data quality metrics.

    Args:
        schema: The schema name
        table: The table name

    Returns:
        Detailed table profile information
    """
    profile_sql = f"""
        SELECT
            c.column_name,
            c.data_type,
            c.is_nullable,
            c.column_default,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale,
            CASE
                WHEN c.data_type IN ('character varying', 'varchar', 'text', 'char') THEN 'string'
                WHEN c.data_type IN ('integer', 'bigint', 'smallint', 'numeric', 'decimal', 'real', 'double precision') THEN 'numeric'
                WHEN c.data_type IN ('date', 'timestamp', 'timestamp with time zone') THEN 'datetime'
                WHEN c.data_type = 'boolean' THEN 'boolean'
                ELSE 'other'
            END as column_category
        FROM information_schema.columns c
        WHERE c.table_schema = '{schema}'
        AND c.table_name = '{table}'
        ORDER BY c.ordinal_position
    """

    async with get_db_connection() as conn:
        try:
            rows = await conn.fetch(profile_sql)
            if not rows:
                return f"Table {schema}.{table} not found"

            # Get basic table info
            table_info_sql = f"""
                SELECT
                    COUNT(*) as row_count,
                    COUNT(DISTINCT 1) as distinct_count
                FROM {schema}.{table}
                LIMIT 1
            """

            try:
                table_info = await conn.fetchrow(table_info_sql)
                row_count = table_info['row_count'] if table_info else 0
            except:
                row_count = "Unknown"

            # Format results
            result_lines = [
                f"Table Profile: {schema}.{table}",
                f"Row Count: {row_count}",
                "",
                "Columns:",
                "column_name,data_type,is_nullable,column_default,max_length,precision,scale,category"
            ]

            for row in rows:
                result_lines.append(",".join([
                    str(row['column_name']),
                    str(row['data_type']),
                    str(row['is_nullable']),
                    str(row['column_default']) if row['column_default'] else 'NULL',
                    str(row['character_maximum_length']) if row['character_maximum_length'] else 'NULL',
                    str(row['numeric_precision']) if row['numeric_precision'] else 'NULL',
                    str(row['numeric_scale']) if row['numeric_scale'] else 'NULL',
                    str(row['column_category'])
                ]))

            return "\n".join(result_lines)

        except Exception as e:
            return f"Error generating table profile: {str(e)}"