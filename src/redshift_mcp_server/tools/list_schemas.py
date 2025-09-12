from ..db_utils import get_db_connection

async def list_schemas() -> str:
    """
    Lists all schemas in the Redshift database.
    """
    sql = "SELECT nspname as schema_name FROM pg_namespace WHERE nspname NOT LIKE 'pg_%' AND nspname != 'information_schema';"
    async with get_db_connection() as conn:
        try:
            rows = await conn.fetch(sql)
            if not rows:
                return "No schemas found."
            
            # Get column names
            columns = list(rows[0].keys())
            
            # Format as CSV
            result_lines = [",".join(columns)]
            for row in rows:
                result_lines.append(",".join(str(row[col]) for col in columns))
            
            return "\n".join(result_lines)
            
        except Exception as e:
            return f"Error listing schemas: {str(e)}"