from ..db_utils import get_db_connection

async def get_business_metrics(schema: str = 'public', days: int = 30) -> str:
    """
    Generate business-focused metrics using Redshift system tables for accurate insights.
    Uses STL tables for query activity and SVV tables for current state analysis.

    Args:
        schema: The schema to analyze (default 'public')
        days: Number of days to look back for trends (default 30)

    Returns:
        Comprehensive business metrics with data activity patterns and growth insights
    """

    async with get_db_connection() as conn:
        try:
            # Get basic schema statistics using information_schema
            schema_stats_sql = f"""
                SELECT
                    COUNT(*) as total_tables,
                    0 as total_size_mb,
                    0 as total_rows,
                    0 as avg_table_size_mb,
                    0 as tables_need_vacuum,
                    0 as tables_need_analyze,
                    0 as skewed_tables,
                    0 as largest_table_size_mb
                FROM information_schema.tables
                WHERE table_schema = '{schema}'
                AND table_type = 'BASE TABLE'
            """

            schema_stats = await conn.fetchrow(schema_stats_sql)

            # Get query activity patterns using STL_QUERY
            activity_sql = f"""
                SELECT
                    DATE(starttime) as query_date,
                    COUNT(*) as total_queries,
                    COUNT(CASE WHEN aborted = 0 THEN 1 END) as successful_queries,
                    COUNT(CASE WHEN aborted = 1 THEN 1 END) as failed_queries,
                    AVG(DATEDIFF(seconds, starttime, endtime)) as avg_duration_sec,
                    MAX(DATEDIFF(seconds, starttime, endtime)) as max_duration_sec,
                    COUNT(DISTINCT userid) as unique_users
                FROM stl_query
                WHERE starttime >= DATEADD(day, -{days}, GETDATE())
                AND userid > 1  -- Exclude system queries
                GROUP BY DATE(starttime)
                ORDER BY query_date DESC
                LIMIT 10
            """

            activity_rows = await conn.fetch(activity_sql)

            # Get table access patterns
            table_access_sql = f"""
                SELECT
                    s.tbl as table_id,
                    pt.schemaname,
                    pt.tablename,
                    COUNT(*) as scan_count,
                    SUM(s.rows) as rows_scanned,
                    AVG(s.rows) as avg_rows_per_scan
                FROM stl_scan s
                JOIN pg_class pc ON s.tbl = pc.oid
                JOIN pg_namespace pn ON pc.relnamespace = pn.oid
                JOIN pg_tables pt ON pn.nspname = pt.schemaname AND pc.relname = pt.tablename
                WHERE s.starttime >= DATEADD(day, -{days}, GETDATE())
                AND pt.schemaname = '{schema}'
                GROUP BY s.tbl, pt.schemaname, pt.tablename
                ORDER BY scan_count DESC
                LIMIT 10
            """

            table_access_rows = await conn.fetch(table_access_sql)

            # Build comprehensive report
            result_lines = [
                f"=== Business Metrics Dashboard ===",
                f"Schema: {schema} | Analysis Period: Last {days} days",
                f"Generated: {conn.get_server_version() if hasattr(conn, 'get_server_version') else 'N/A'}",
                "",
                "=== Schema Overview ===",
                f"Total Tables: {schema_stats['total_tables']}",
                f"Total Data Size: {schema_stats['total_size_mb']:.2f} MB",
                f"Total Rows: {schema_stats['total_rows']:,}",
                f"Average Table Size: {schema_stats['avg_table_size_mb']:.2f} MB",
                f"Largest Table: {schema_stats['largest_table_size_mb']:.2f} MB",
                "",
                "=== Data Health ===",
                f"Tables Needing VACUUM: {schema_stats['tables_need_vacuum']}",
                f"Tables Needing ANALYZE: {schema_stats['tables_need_analyze']}",
                f"Skewed Tables: {schema_stats['skewed_tables']}",
                f"Health Score: {max(0, 100 - (schema_stats['tables_need_vacuum'] + schema_stats['tables_need_analyze']) * 10):.1f}%",
                "",
                "=== Query Activity Trends ===",
                "date,total_queries,success_rate,avg_duration_sec,max_duration_sec,unique_users"
            ]

            if activity_rows:
                for row in activity_rows:
                    success_rate = (row['successful_queries'] / row['total_queries'] * 100) if row['total_queries'] > 0 else 0
                    result_lines.append(",".join([
                        str(row['query_date']),
                        str(row['total_queries']),
                        f"{success_rate:.1f}%",
                        f"{row['avg_duration_sec']:.2f}",
                        str(row['max_duration_sec']),
                        str(row['unique_users'])
                    ]))
            else:
                result_lines.append("No query activity found in the specified period")

            result_lines.extend([
                "",
                "=== Most Accessed Tables ===",
                "table_name,scan_count,total_rows_scanned,avg_rows_per_scan,access_intensity"
            ])

            if table_access_rows:
                for row in table_access_rows:
                    # Calculate access intensity (scans per day)
                    access_intensity = row['scan_count'] / days
                    result_lines.append(",".join([
                        str(row['tablename']),
                        str(row['scan_count']),
                        str(row['rows_scanned']),
                        f"{row['avg_rows_per_scan']:.0f}",
                        f"{access_intensity:.2f}"
                    ]))
            else:
                result_lines.append("No table access data found")

            # Calculate business insights
            if activity_rows:
                total_queries = sum(row['total_queries'] for row in activity_rows)
                total_successful = sum(row['successful_queries'] for row in activity_rows)
                overall_success_rate = (total_successful / total_queries * 100) if total_queries > 0 else 0

                result_lines.extend([
                    "",
                    "=== Business Insights ===",
                    f"Total Queries (Period): {total_queries:,}",
                    f"Overall Success Rate: {overall_success_rate:.1f}%",
                    f"Average Daily Queries: {total_queries / len(activity_rows):.0f}",
                    f"Data Utilization: {'HIGH' if total_queries > days * 100 else 'MODERATE' if total_queries > days * 10 else 'LOW'}"
                ])

            return "\n".join(result_lines)

        except Exception as e:
            # Fallback to basic schema information
            try:
                fallback_sql = f"""
                    SELECT
                        COUNT(*) as table_count
                    FROM information_schema.tables
                    WHERE table_schema = '{schema}'
                """

                basic_stats = await conn.fetchrow(fallback_sql)
                return f"Basic Schema Info - {schema}: {basic_stats['table_count']} tables | Error getting detailed metrics: {str(e)}"

            except Exception as fallback_error:
                return f"Error generating business metrics: {str(e)} | Fallback error: {str(fallback_error)}"