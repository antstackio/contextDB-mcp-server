from ..db_utils import get_db_connection

async def find_table_dependencies(schema: str, table: str) -> str:
    """
    Map table relationships and dependencies including foreign keys.
    
    Args:
        schema: The schema name
        table: The table name
        
    Returns:
        Table dependencies and relationships
    """
    # Note: Redshift has limited support for pg_constraint, adapting for Redshift
    sql = f"""
        SELECT 
            t1.table_name as referencing_table,
            t1.column_name as referencing_column,
            t2.table_name as referenced_table,
            t2.column_name as referenced_column
        FROM information_schema.key_column_usage t1
        JOIN information_schema.referential_constraints rc ON t1.constraint_name = rc.constraint_name
        JOIN information_schema.key_column_usage t2 ON rc.unique_constraint_name = t2.constraint_name
        WHERE t1.table_schema = '{schema}' 
        AND (t1.table_name = '{table}' OR t2.table_name = '{table}')
        ORDER BY t1.table_name, t1.column_name
    """
    
    async with get_db_connection() as conn:
        try:
            rows = await conn.fetch(sql)
            if not rows:
                return f"No dependencies found for table {schema}.{table}"
            
            # Format as CSV
            columns = ['referencing_table', 'referencing_column', 'referenced_table', 'referenced_column']
            result_lines = [",".join(columns)]
            
            for row in rows:
                result_lines.append(",".join(str(row[col]) for col in columns))
            
            return "\n".join(result_lines)
            
        except Exception as e:
            return f"Error finding table dependencies: {str(e)}"