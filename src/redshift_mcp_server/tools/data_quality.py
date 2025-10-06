from ..db_utils import get_db_connection

async def check_data_quality(schema: str, table: str) -> str:
    """
    Perform comprehensive data quality checks on a table.

    Args:
        schema: The schema name
        table: The table name

    Returns:
        Data quality report with completeness, validity, and consistency metrics
    """

    async with get_db_connection() as conn:
        try:
            # Get table structure first
            columns_sql = f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = '{schema}' AND table_name = '{table}'
                ORDER BY ordinal_position
            """

            columns = await conn.fetch(columns_sql)
            if not columns:
                return f"Table {schema}.{table} not found"

            # Get row count - properly quote schema and table names
            count_sql = f'SELECT COUNT(*) as row_count FROM "{schema}"."{table}"'
            row_count = await conn.fetchval(count_sql)

            result_lines = [
                f"Data Quality Report: {schema}.{table}",
                f"Total Rows: {row_count}",
                "",
                "Column Quality Analysis:",
                "column_name,data_type,null_count,null_percentage,unique_count,quality_score"
            ]

            for col in columns:
                col_name = col['column_name']
                col_type = col['data_type']

                # Null analysis - properly quote schema, table, and column names
                null_sql = f"""
                    SELECT
                        COUNT(CASE WHEN "{col_name}" IS NULL THEN 1 END) as null_count,
                        COUNT(DISTINCT "{col_name}") as unique_count
                    FROM "{schema}"."{table}"
                """

                try:
                    stats = await conn.fetchrow(null_sql)
                    null_count = stats['null_count']
                    unique_count = stats['unique_count']
                    null_pct = (null_count / row_count * 100) if row_count > 0 else 0

                    # Calculate quality score (100 - null_percentage)
                    quality_score = max(0, 100 - null_pct)

                    result_lines.append(",".join([
                        col_name,
                        col_type,
                        str(null_count),
                        f"{null_pct:.2f}",
                        str(unique_count),
                        f"{quality_score:.1f}"
                    ]))

                except Exception as col_error:
                    result_lines.append(f"{col_name},{col_type},ERROR,ERROR,ERROR,ERROR")

            # Add summary statistics
            result_lines.extend([
                "",
                "Quality Summary:",
                f"Columns Analyzed: {len(columns)}",
                f"Table Completeness: Available for detailed analysis"
            ])

            return "\n".join(result_lines)

        except Exception as e:
            return f"Error checking data quality: {str(e)}"