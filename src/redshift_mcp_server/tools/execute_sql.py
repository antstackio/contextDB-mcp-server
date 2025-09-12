
from ..db_utils import get_db_connection

async def execute_sql(sql: str) -> str:
    """
    Execute a SQL Query on the Redshift cluster.
    
    Args:
        sql: The SQL query to execute
        
    Returns:
        Query results as CSV format or success message
    """
    print(f"Executing SQL: {sql}")
    async with get_db_connection() as conn:
        try:
            # Guardrail: Only allow SELECT statements
            if not sql.upper().strip().startswith('SELECT'):
                return "Error: Only SELECT statements are allowed for execution."
            
            # For SELECT queries that return data
            rows = await conn.fetch(sql)
            if not rows:
                return "Query executed successfully but returned no data."
            
            # Get column names
            columns = list(rows[0].keys())
            
            # Format as CSV
            result_lines = [",".join(columns)]
            for row in rows:
                result_lines.append(",".join(str(row[col]) for col in columns))
            
            return "\n".join(result_lines)
            
        except Exception as e:
            return f"Error executing query: {str(e)}"