from ..db_utils import get_db_connection

async def discover_schema_metadata(schema: str) -> str:
    """
    Extract comprehensive schema metadata using Redshift-optimized system views.
    Uses pg_catalog views for better performance than information_schema.

    Args:
        schema: The schema name to analyze

    Returns:
        Schema metadata with column details, data types, and constraints
    """
    # Use pg_catalog for better performance in Redshift
    sql = f"""
        SELECT
            c.table_name,
            c.column_name,
            c.data_type,
            c.is_nullable,
            c.column_default,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale,
            c.ordinal_position,
            CASE
                WHEN pk.column_name IS NOT NULL THEN 'YES'
                ELSE 'NO'
            END as is_primary_key,
            CASE
                WHEN dk.column_name IS NOT NULL THEN dk.distkey_type
                ELSE 'NO'
            END as is_distkey,
            CASE
                WHEN sk.column_name IS NOT NULL THEN 'YES'
                ELSE 'NO'
            END as is_sortkey
        FROM information_schema.columns c
        LEFT JOIN (
            SELECT
                schemaname,
                tablename,
                column_name,
                'YES' as distkey_type
            FROM pg_catalog.pg_constraint pc
            JOIN pg_catalog.pg_class pt ON pc.conrelid = pt.oid
            JOIN pg_catalog.pg_namespace pn ON pt.relnamespace = pn.oid
            JOIN information_schema.columns ic ON pn.nspname = ic.table_schema
                AND pt.relname = ic.table_name
            WHERE pc.contype = 'd' AND pn.nspname = '{schema}'
        ) dk ON c.table_schema = dk.schemaname
            AND c.table_name = dk.tablename
            AND c.column_name = dk.column_name
        LEFT JOIN (
            SELECT
                schemaname,
                tablename,
                column_name
            FROM pg_catalog.pg_constraint pc
            JOIN pg_catalog.pg_class pt ON pc.conrelid = pt.oid
            JOIN pg_catalog.pg_namespace pn ON pt.relnamespace = pn.oid
            JOIN information_schema.columns ic ON pn.nspname = ic.table_schema
                AND pt.relname = ic.table_name
            WHERE pc.contype = 'p' AND pn.nspname = '{schema}'
        ) pk ON c.table_schema = pk.schemaname
            AND c.table_name = pk.tablename
            AND c.column_name = pk.column_name
        LEFT JOIN (
            SELECT
                schemaname,
                tablename,
                column_name
            FROM pg_catalog.pg_constraint pc
            JOIN pg_catalog.pg_class pt ON pc.conrelid = pt.oid
            JOIN pg_catalog.pg_namespace pn ON pt.relnamespace = pn.oid
            JOIN information_schema.columns ic ON pn.nspname = ic.table_schema
                AND pt.relname = ic.table_name
            WHERE pc.contype = 's' AND pn.nspname = '{schema}'
        ) sk ON c.table_schema = sk.schemaname
            AND c.table_name = sk.tablename
            AND c.column_name = sk.column_name
        WHERE c.table_schema = '{schema}'
        ORDER BY c.table_name, c.ordinal_position
    """

    async with get_db_connection() as conn:
        try:
            rows = await conn.fetch(sql)
            if not rows:
                return f"No metadata found for schema '{schema}'"

            # Enhanced CSV format with Redshift-specific information
            columns = [
                'table_name', 'column_name', 'data_type', 'is_nullable', 'column_default',
                'max_length', 'precision', 'scale', 'position', 'is_primary_key',
                'is_distkey', 'is_sortkey'
            ]
            result_lines = [",".join(columns)]

            for row in rows:
                result_lines.append(",".join([
                    str(row['table_name']),
                    str(row['column_name']),
                    str(row['data_type']),
                    str(row['is_nullable']),
                    str(row['column_default']) if row['column_default'] else 'NULL',
                    str(row['character_maximum_length']) if row['character_maximum_length'] else 'NULL',
                    str(row['numeric_precision']) if row['numeric_precision'] else 'NULL',
                    str(row['numeric_scale']) if row['numeric_scale'] else 'NULL',
                    str(row['ordinal_position']),
                    str(row['is_primary_key']),
                    str(row['is_distkey']),
                    str(row['is_sortkey'])
                ]))

            return "\n".join(result_lines)

        except Exception as e:
            # Fallback to simpler query if the complex one fails
            fallback_sql = f"""
                SELECT
                    table_name,
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_schema = '{schema}'
                ORDER BY table_name, ordinal_position
            """

            try:
                rows = await conn.fetch(fallback_sql)
                if not rows:
                    return f"No metadata found for schema '{schema}'"

                columns = ['table_name', 'column_name', 'data_type', 'is_nullable', 'column_default']
                result_lines = [",".join(columns)]

                for row in rows:
                    result_lines.append(",".join([
                        str(row['table_name']),
                        str(row['column_name']),
                        str(row['data_type']),
                        str(row['is_nullable']),
                        str(row['column_default']) if row['column_default'] else 'NULL'
                    ]))

                return "\n".join(result_lines)

            except Exception as fallback_error:
                return f"Error discovering schema metadata: {str(e)} | Fallback error: {str(fallback_error)}"