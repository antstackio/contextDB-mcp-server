  Best Practices from Research

  1. Description Structure (Anthropic's Recommendations)

  ✅ DO:
  - Start with the action verb (Execute, Analyze, Generate, Map, List)
  - Explain WHAT it does in the first sentence
  - Specify output format explicitly (CSV, JSON, summary)
  - Mention key constraints (e.g., "Only SELECT allowed")
  - Include when to use it (use cases/scenarios)

  ❌ DON'T:
  - Use vague terms like "Get data" or "Process information"
  - Mix multiple concerns in one tool
  - Assume context the AI doesn't have

  2. Parameter Naming (Critical for Determinism)

  Your Current Code:
  async def find_table_dependencies_tool(schema: str, table: str) -> str:

  ✅ Good: schema, table are unambiguous⚠️ Could Improve: Add parameter descriptions in docstring

  Better Pattern:
  async def find_table_dependencies_tool(
      schema_name: str,  # More explicit than just "schema"
      table_name: str
  ) -> str:
      """
      Map table relationships and foreign key dependencies.
      
      Use this when you need to understand:
      - Which tables reference this table (downstream dependencies)
      - Which tables this table depends on (upstream dependencies)
      - Foreign key relationships and data lineage
      
      Args:
          schema_name: The database schema containing the table (e.g., 'public', 'order-management')
          table_name: The specific table name to analyze (e.g., 'clinical_order')
      
      Returns:
          CSV format with columns: referenced_table, referencing_table, constraint_name, columns
      """

  ---
  Analysis of Your Current Tools

  What You're Doing Well:

  1. ✅ Action verbs in descriptions: "Execute", "Analyze", "Generate", "Map"
  2. ✅ Specify output format: "Returns results in CSV format"
  3. ✅ Mention constraints: "Only SELECT statements allowed for safety"
  4. ✅ Type hints: All parameters have type annotations

  Where You Can Improve:

  Issue #1: Overlapping Tool Purposes

  # Current: 3 tools that might confuse the agent
  list_schemas_tool()           # When to use?
  discover_schema_metadata_tool(schema)  # vs this?
  list_tables_tool(schema)      # vs this?

  The Problem: An AI agent asked "What tables exist in the order-management schema?" could reasonably pick any of these three tools.

  Solution: Make tool purposes mutually exclusive in descriptions:

  @mcp.tool()
  async def list_schemas_tool() -> str:
      """
      **START HERE**: List all database schemas to discover high-level data organization.
      
      Use this FIRST when exploring an unfamiliar database to see available business domains.
      Do NOT use this to see tables - use list_tables_tool instead.
      
      Returns: CSV with schema names (e.g., 'public', 'billing-claims', 'order-management')
      """

  @mcp.tool()
  async def list_tables_tool(schema_name: str = 'public') -> str:
      """
      List all tables in a schema with operational metrics (sizes, row counts, maintenance status).
      
      Use this AFTER list_schemas_tool to explore tables within a specific schema.
      Use this when you need to know: table names, sizes, distribution styles, or maintenance needs.
      Do NOT use for column details - use discover_schema_metadata_tool instead.
      
      Args:
          schema_name: Target schema from list_schemas_tool output (default: 'public')
      
      Returns: CSV with table_name, size_mb, row_count, dist_style, needs_vacuum, etc.
      """

  @mcp.tool()
  async def discover_schema_metadata_tool(schema_name: str) -> str:
      """
      Get complete data dictionary: ALL tables, columns, data types, and constraints in a schema.
      
      Use this when you need: column names, data types, primary keys, distribution keys, or complete schema documentation.
      This returns DETAILED column-level metadata (can be large for schemas with many tables).
      
      Args:
          schema_name: Target schema from list_schemas_tool output
      
      Returns: CSV with 579+ rows containing table_name, column_name, data_type, is_nullable, 
               column_default, is_primary_key, is_distkey, is_sortkey
      """

  ---
  Issue #2: Missing Usage Hints

  # Current
  async def analyze_query_performance_tool(limit: int = 10) -> str:
      """Analyze recent query performance using Redshift system tables. Identifies slow queries, queue times, and provides optimization insights."""

  The Problem: AI doesn't know WHEN this is useful.

  Better:
  async def analyze_query_performance_tool(limit: int = 10) -> str:
      """
      Analyze recent query performance to identify slow or problematic queries.
      
      Use this when:
      - User asks "Why is the database slow?"
      - Investigating query optimization opportunities
      - Need to see recent query execution times and resource usage
      
      Analyzes queries from last 24 hours in Redshift STL system tables.
      
      Args:
          limit: Number of slowest queries to return (default: 10, max recommended: 50)
      
      Returns: CSV with query_id, duration_sec, queue_time_ms, status, performance_level, query_snippet
               Plus summary statistics: avg duration, slow query count, performance issue percentage
      """

  ---
  Issue #3: Vague Output Descriptions

  # Current
  """Generate detailed table profile with column analysis, data types, row counts, and data quality metrics for business intelligence."""

  The Problem: "Detailed table profile" is vague - AI doesn't know exactly what it will get.

  Better:
  """
  Generate comprehensive table profile for business intelligence and data analysis.

  Returns specific metrics:
  - Column-level statistics (count, nulls, distinct values)
  - Data type distribution
  - Actual row count vs estimated
  - Sample values from each column
  - Data quality scores (completeness percentage)

  Use this when you need to understand table contents before writing queries or building reports.

  Args:
      schema_name: Database schema name
      table_name: Specific table to profile

  Returns: Formatted report with sections for table summary, column statistics, and sample data
  """

  ---
  Concrete Improvements for Determinism

  1. Add "When to Use" Section

  This is the single most important improvement:

  """
  [What it does - 1 sentence]

  **When to use this tool:**
  - [Scenario 1]
  - [Scenario 2]
  - [Scenario 3]

  **Do NOT use for:**
  - [Alternative tool to use instead]

  [Technical details]

  Args: ...
  Returns: ...
  """

  2. Specify Exact Output Format

  # Instead of: "Returns results in CSV format"
  # Write:
  """
  Returns: CSV format with columns: table_name, column_name, data_type, is_nullable
  Example output:
      table_name,column_name,data_type,is_nullable
      users,id,bigint,NO
      users,email,varchar,YES
  """

  3. Add Parameter Constraints

  async def analyze_query_performance_tool(limit: int = 10) -> str:
      """
      ...
      Args:
          limit: Number of queries to analyze (1-100, default: 10)
                - Use 10 for quick overview
                - Use 20-50 for detailed analysis
                - Values > 100 may be slow
      """

  4. Create Tool "Workflows"

  Help the AI understand tool sequences:

  @mcp.tool()
  async def list_schemas_tool() -> str:
      """
      **STEP 1 of database exploration**: List all schemas to see data organization.
      
      Typical workflow:
      1. Use this tool to see all schemas
      2. Then use list_tables_tool(schema_name) to see tables in a schema
      3. Then use discover_schema_metadata_tool(schema_name) for detailed columns
      
      Returns: Simple list of schema names
      """

  ---