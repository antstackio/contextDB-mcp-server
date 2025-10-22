async def analyze_query_performance(adapter, limit: int = 10) -> str:
    """
    Analyze recent query performance using database-specific system tables.

    Currently only supports Redshift (uses STL_QUERY with additional metrics).

    Args:
        adapter: Database adapter instance (must be Redshift)
        limit: Number of queries to analyze (default 10)

    Returns:
        Query performance analysis with execution times, resource usage, and optimization hints

    Raises:
        ValueError: If adapter is not Redshift type
    """
    # Check if adapter is Redshift
    if adapter.db_type != 'redshift':
        return f"❌ This tool only supports Redshift. Current database type: {adapter.db_type}"
    # Optimized query using STL_QUERY with additional performance metrics
    performance_sql = f"""
        SELECT
            q.query,
            q.userid,
            q.starttime,
            q.endtime,
            DATEDIFF(seconds, q.starttime, q.endtime) as duration_seconds,
            q.aborted,
            CASE WHEN q.aborted = 0 THEN 'SUCCESS' ELSE 'ABORTED' END as status,
            wlm.queue_start_time,
            wlm.queue_end_time,
            DATEDIFF(ms, wlm.queue_start_time, wlm.queue_end_time) as queue_time_ms,
            wlm.exec_time as exec_time_ms,
            wlm.service_class,
            CASE
                WHEN DATEDIFF(seconds, q.starttime, q.endtime) > 300 THEN 'SLOW'
                WHEN DATEDIFF(seconds, q.starttime, q.endtime) > 60 THEN 'MODERATE'
                ELSE 'FAST'
            END as performance_category,
            LEFT(REGEXP_REPLACE(q.querytxt, '[\\r\\n\\t]+', ' ', 'g'), 100) as query_snippet
        FROM stl_query q
        LEFT JOIN stl_wlm_query wlm ON q.query = wlm.query
        WHERE q.starttime >= DATEADD(hour, -24, GETDATE())
        AND q.userid > 1  -- Exclude system queries
        AND q.querytxt NOT LIKE 'SELECT%pg_%'
        AND q.querytxt NOT LIKE '%information_schema%'
        AND q.querytxt NOT LIKE '%stl_%'
        AND q.querytxt NOT LIKE '%svv_%'
        ORDER BY duration_seconds DESC
        LIMIT {limit}
    """

    async with adapter.get_connection() as conn:
        try:
            rows = await conn.fetch(performance_sql)
            if not rows:
                return "No recent queries found"

            # Enhanced CSV format with performance insights
            columns = [
                'query_id', 'user_id', 'start_time', 'duration_sec', 'status',
                'queue_time_ms', 'exec_time_ms', 'service_class', 'performance_level', 'query_snippet'
            ]
            result_lines = [",".join(columns)]

            total_duration = 0
            slow_queries = 0

            for row in rows:
                duration = row['duration_seconds']
                total_duration += duration
                if duration > 60:  # Consider >60s as slow
                    slow_queries += 1

                # Clean up query snippet for CSV
                query_snippet = str(row['query_snippet']).replace(',', ';').replace('\n', ' ')

                result_lines.append(",".join([
                    str(row['query']),
                    str(row['userid']),
                    str(row['starttime']),
                    str(duration),
                    str(row['status']),
                    str(row['queue_time_ms']) if row['queue_time_ms'] else '0',
                    str(row['exec_time_ms']) if row['exec_time_ms'] else '0',
                    str(row['service_class']) if row['service_class'] else 'default',
                    str(row['performance_category']),
                    f'"{query_snippet}"'
                ]))

            # Add performance summary
            avg_duration = total_duration / len(rows) if rows else 0
            result_lines.extend([
                "",
                f"=== Performance Summary ===",
                f"Total Queries Analyzed: {len(rows)}",
                f"Average Duration: {avg_duration:.2f} seconds",
                f"Slow Queries (>60s): {slow_queries}",
                f"Performance Issues: {slow_queries / len(rows) * 100:.1f}% of queries"
            ])

            return "\n".join(result_lines)

        except Exception as e:
            # Fallback to simpler query
            try:
                fallback_sql = f"""
                    SELECT
                        query,
                        userid,
                        starttime,
                        endtime,
                        DATEDIFF(seconds, starttime, endtime) as duration_seconds,
                        aborted,
                        LEFT(querytxt, 100) as query_snippet
                    FROM stl_query
                    WHERE starttime >= DATEADD(day, -1, GETDATE())
                    AND userid > 1
                    ORDER BY duration_seconds DESC
                    LIMIT {limit}
                """

                rows = await conn.fetch(fallback_sql)
                if not rows:
                    return "No queries found"

                columns = ['query_id', 'user_id', 'start_time', 'end_time', 'duration_sec', 'aborted', 'query_snippet']
                result_lines = [",".join(columns)]

                for row in rows:
                    query_snippet = str(row['query_snippet']).replace(',', ';')
                    result_lines.append(",".join([
                        str(row['query']),
                        str(row['userid']),
                        str(row['starttime']),
                        str(row['endtime']),
                        str(row['duration_seconds']),
                        str(row['aborted']),
                        f'"{query_snippet}"'
                    ]))

                return "\n".join(result_lines)

            except Exception as fallback_error:
                return f"Error analyzing query performance: {str(e)} | Fallback error: {str(fallback_error)}"