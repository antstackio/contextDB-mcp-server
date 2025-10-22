# contextDB MCP Server


## Features

- **Multi-Database Support**: Connect to Redshift, PostgreSQL, Aurora, and MySQL databases
- **Metadata Discovery**: Explore schemas, tables, columns, and relationships
- **Data Profiling**: Analyze table profiles, data quality, and distribution
- **Query Execution**: Execute SELECT queries with user approval
- **Business Metrics**: Generate 30-day business intelligence dashboards (Redshift)
- **Performance Analysis**: Analyze query performance and identify slow queries (Redshift)
- **Cross-Database Search**: Search for tables and columns across multiple datasources

## Installation

```bash
# 1. Clone the repository
git clone https://github.com/antstackio/contextDB-mcp-server.git
cd contextDB

# 2. Install with uv tool
uv tool install .

# 3. Add uv's bin directory to PATH (one-time setup)
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

This makes the `contextDB` command available globally.


## Configuration

### 1. Create Datasources Configuration

Copy the example configuration and update with your database credentials:

```bash
cp datasources.example.json datasources.json
```

Edit `datasources.json`:



### 2. Configure MCP Client

#### Claude Code

Create `.settings.json` in your project root:
```json
{
  "mcpServers": {
    "redshift": {
      "command": "contextDB",
      "env": {
        "DATASOURCES_CONFIG": "/path/to/your/datasources.json"
      }
    }
  }
}
```

#### Gemini CLI

Create `.gemini/settings.json` in your project root:
```json
{
  "mcpServers": {
    "redshift": {
      "command": "contextDB",
      "env": {
        "DATASOURCES_CONFIG": "/path/to/your/datasources.json"
      }
    }
  }
}
```

#### Cursor

Open Command Palette and select "Add new global MCP server". This opens the MCP settings file where you can add:
```json
{
  "mcpServers": {
    "redshift": {
      "command": "contextDB",
      "env": {
        "DATASOURCES_CONFIG": "/path/to/your/datasources.json"
      }
    }
  }
}
```



## Usage

### Discovery Tools

1. **List all datasources**
```
list_all_datasources_tool()
```

2. **Get datasource overview**
```
get_datasource_overview_tool(datasource_id="analytics_warehouse")
```

3. **Search across all datasources**
```
search_across_datasources_tool(keyword="customer")
```

### Database Exploration Tools

4. **Get database overview**
```
get_database_overview_tool(datasource_id="analytics_warehouse")
```

5. **List schemas**
```
list_schemas_tool(datasource_id="analytics_warehouse")
```

6. **List tables in a schema**
```
list_tables_tool(datasource_id="analytics_warehouse", schema_name="public")
```

7. **Discover schema metadata**
```
discover_schema_metadata_tool(
    datasource_id="analytics_warehouse",
    schema_name="public"
)
```

8. **Get table profile**
```
get_table_profile_tool(
    datasource_id="analytics_warehouse",
    schema_name="public",
    table_name="customers"
)
```

### Query and Analysis Tools

9. **Execute SQL query (requires approval)**
```
execute_sql_tool(
    datasource_id="analytics_warehouse",
    sql_query="SELECT * FROM public.customers LIMIT 10"
)
```

10. **Check data quality**
```
check_data_quality_tool(
    datasource_id="analytics_warehouse",
    schema_name="public",
    table_name="customers"
)
```

### Redshift-Specific Tools

11. **Analyze query performance**
```
analyze_query_performance_tool(
    datasource_id="analytics_warehouse",
    limit=10
)
```

12. **Get business metrics**
```
get_business_metrics_tool(
    datasource_id="analytics_warehouse",
    schema_name="public",
    days=30
)
```

## Environment Variables

- `DATASOURCES_CONFIG`: Path to datasources configuration file (default: `datasources.json`)
- `TRANSPORT`: Set to `http` for HTTP mode - can be used to host the server (default: `stdio`)

## Running in HTTP Mode

For hosting or testing:

```bash
# Using uv
uv run contextDB --http

# Or set environment variable
TRANSPORT=http uv run contextDB
```

The server will start on `http://127.0.0.1:8000`

## Supported Database Types

- **Redshift**: Full feature support including performance analysis and business metrics
- **PostgreSQL**: Core features (schemas, tables, queries, data quality)
- **Aurora PostgreSQL**: Core features
- **MySQL**: Core features

## Security Notes

- All SQL queries are READ-ONLY (SELECT statements only)
- User approval required for query execution
- Credentials stored in local configuration file


## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT

## Requirements

- Python >=3.10
- Database drivers (installed automatically):
  - `asyncpg` for PostgreSQL/Redshift/Aurora
  - `redshift-connector` for Redshift-specific features
- MCP SDK

## Troubleshooting

### Connection Issues

1. Verify your datasources.json credentials
2. Check network connectivity to database
3. Ensure database allows connections from your IP
4. Verify database user has necessary permissions
