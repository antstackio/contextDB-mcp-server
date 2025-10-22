"""Entry point for running the MCP server as a module."""
import sys
import os
from contextDB.server import mcp, init_registry, logger

def main():
    """Main entry point for the MCP server."""
    # Initialize DatasourceRegistry
    registry = init_registry()

    # Determine transport mode from command-line args or environment variable
    use_http = '--http' in sys.argv or os.getenv('TRANSPORT', '').lower() == 'http'

    if use_http:
        # HTTP transport mode
        print("=" * 70)
        print(f"🚀 Multi-Datasource MCP Server (HTTP Mode)")
        print("=" * 70)
        print(f"📊 Datasources Loaded: {len(registry.list_all())}")
        for ds in registry.list_all():
            print(f"   • {ds.id} ({ds.type}) - {ds.name}")
        print("")
        print(f"Transport: HTTP")
        print(f"Address: http://127.0.0.1:8000")
        print("=" * 70)
        print(f"🔧 Available Tools: 16")
        print(f"   • 3 Discovery Tools (list_datasources, overview, search)")
        print(f"   • 13 Database Tools (schema, tables, queries, analysis)")
        print("=" * 70)
        mcp.run(transport="http", host="127.0.0.1", port=8000)
    else:
        # STDIO transport mode (default)
        logger.info(f"✅ Multi-Datasource MCP Server starting (stdio mode)")
        logger.info(f"📊 {len(registry.list_all())} datasources loaded")
        for ds in registry.list_all():
            logger.info(f"   • {ds.id} ({ds.type})")
        mcp.run()

if __name__ == "__main__":
    main()
