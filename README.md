# Redshift MCP Server

A Model Context Protocol (MCP) server for Amazon Redshift that enables AI assistants to interact with Redshift databases.

## Introduction

Redshift MCP Server is a Python-based implementation of the [Model Context Protocol](https://github.com/modelcontextprotocol/mcp) that provides tools and resources for interacting with Amazon Redshift databases. It allows AI assistants to:

- List schemas and tables in a Redshift database
- Retrieve table DDL (Data Definition Language) scripts
- Get table statistics
- Execute SQL queries
- Analyze tables to collect statistics information
- Get execution plans for SQL queries
- Discover schema metadata
- Analyze query performance
- Find table dependencies
- Validate report queries

## Installation

### Prerequisites

- Python 3.13 or higher
- Amazon Redshift cluster
- Redshift credentials (host, port, username, password, database)

### Install from source

```bash
# Clone the repository
git clone https://github.com/Moonlight-CL/redshift-mcp-server.git
cd redshift-mcp-server

# Install dependencies
uv sync
```

## Configuration

The server requires the following environment variables to connect to your Redshift cluster:

```
RS_HOST=your-redshift-cluster.region.redshift.amazonaws.com
RS_PORT=5439
RS_USER=your_username
RS_PASSWORD=your_password
RS_DATABASE=your_database
RS_SCHEMA=your_schema  # Optional, defaults to "public"
```

You can set these environment variables directly or use a `.env` file.

## Usage

### Starting the server

```bash

mcp run src/redshift_mcp_server/server.py
```

### Integrating with AI assistants

To use this server with an AI assistant that supports MCP, you can configure your MCP settings to point to this server.

## Features

### Resources

The server provides the following resources:

- `rs:///schemas` - Lists all schemas in the database
- `rs:///{schema}/tables` - Lists all tables in a specific schema
- `rs:///{schema}/{table}/ddl` - Gets the DDL script for a specific table
- `rs:///{schema}/{table}/statistic` - Gets statistics for a specific table

### Tools

The server provides the following tools:

- `execute_sql` - Executes a SQL query on the Redshift cluster
- `analyze_table` - Analyzes a table to collect statistics information
- `get_execution_plan` - Gets the execution plan with runtime statistics for a SQL query
- `discover_schema_metadata` - Extracts comprehensive schema metadata
- `analyze_query_performance` - Analyzes query execution plan and performance
- `find_table_dependencies` - Maps table relationships and dependencies
- `validate_report_queries` - Batch validates queries from reports

## Development

### Project structure

```
redshift-mcp-server/
├── src/
│   └── redshift_mcp_server/
│       ├── __init__.py
│       ├── server.py
│       ├── database.py
│       ├── settings.py
│       ├── tools/
│       │   ├── __init__.py
│       │   └── ...
│       └── resources/
│           ├── __init__.py
│           └── ...
├── pyproject.toml
└── README.md
```

### Dependencies

- `mcp[cli]>=1.5.0` - Model Context Protocol SDK
- `python-dotenv>=1.1.0` - For loading environment variables from .env files
- `redshift-connector>=2.1.5` - Python connector for Amazon Redshift
