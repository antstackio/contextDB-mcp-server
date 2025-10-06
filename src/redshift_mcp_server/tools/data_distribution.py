from ..db_utils import get_db_connection

async def analyze_data_distribution(schema: str, table: str, column: str | None = None) -> str:
    """
    Analyze data distribution and skew for tables and columns.

    Args:
        schema: The schema name
        table: The table name
        column: Optional specific column to analyze

    Returns:
        Data distribution analysis including skew and node distribution
    """
    if column:
        # Analyze specific column distribution - properly quote all identifiers
        column_sql = f"""
            SELECT
                '{column}' as column_name,
                COUNT(*) as total_rows,
                COUNT(DISTINCT "{column}") as distinct_values,
                COUNT(CASE WHEN "{column}" IS NULL THEN 1 END) as null_count,
                ROUND(COUNT(DISTINCT "{column}") * 100.0 / COUNT(*), 2) as cardinality_percent
            FROM "{schema}"."{table}"
        """
    else:
        # Analyze table distribution across nodes - properly quote identifiers
        column_sql = f"""
            SELECT
                slice as node_slice,
                COUNT(*) as row_count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percent_of_total
            FROM "{schema}"."{table}"
            GROUP BY slice
            ORDER BY slice
        """

    async with get_db_connection() as conn:
        try:
            rows = await conn.fetch(column_sql)
            if not rows:
                return f"No distribution data found for {schema}.{table}"

            if column:
                # Format column analysis
                row = rows[0]
                result_lines = [
                    f"Column Distribution Analysis: {schema}.{table}.{column}",
                    f"Total Rows: {row['total_rows']}",
                    f"Distinct Values: {row['distinct_values']}",
                    f"Null Count: {row['null_count']}",
                    f"Cardinality: {row['cardinality_percent']}%"
                ]
            else:
                # Format table distribution
                result_lines = [
                    f"Table Distribution Analysis: {schema}.{table}",
                    "node_slice,row_count,percent_of_total"
                ]
                for row in rows:
                    result_lines.append(f"{row['node_slice']},{row['row_count']},{row['percent_of_total']}")

            return "\n".join(result_lines)

        except Exception as e:
            return f"Error analyzing data distribution: {str(e)}"