from ..db_utils import get_db_connection

async def discover_schema_metadata(schema: str) -> str:
    """
    Extract comprehensive schema metadata including tables, columns, and types.
    
    Args:
        schema: The schema name to analyze
        
    Returns:
        Schema metadata in CSV format
    """
    sql = f"""
        SELECT table_name, column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
        ORDER BY table_name, ordinal_position
    """
    
    async with get_db_connection() as conn:
        try:
            rows = await conn.fetch(sql)
            if not rows:
                return f"No metadata found for schema '{schema}'"
            
            # Format as CSV
            columns = ['table_name', 'column_name', 'data_type', 'is_nullable', 'column_default']
            result_lines = [",".join(columns)]
            
            for row in rows:
                result_lines.append(",".join(str(row[col]) if row[col] is not None else 'NULL' for col in columns))
            
            return "\n".join(result_lines)
            
        except Exception as e:
            return f"Error discovering schema metadata: {str(e)}"