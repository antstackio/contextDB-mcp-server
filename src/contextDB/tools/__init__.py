"""
Tools for database operations.

Structure:
- universal/: Tools that work with ALL datasources (postgres, redshift, mysql, aurora)
  - discovery/: Datasource exploration, overview, search
  - analysis/: Performance monitoring and business metrics
- redshift/: Redshift-specific tools (STL/SVL system tables)

Note: Most tool logic is now in the server (@mcp.tool() handlers) which use adapters directly.
      Only reusable tool functions are kept in separate modules.
"""

# Universal discovery tools
from .universal.discovery import (
    list_all_datasources,
    get_datasource_overview,
    search_across_datasources,
    get_database_overview,
)

# Universal analysis tools
from .universal.analysis import (
    analyze_query_performance,
    get_business_metrics,
)

__all__ = [
    # Universal Discovery
    "list_all_datasources",
    "get_datasource_overview",
    "search_across_datasources",
    "get_database_overview",
    # Universal Analysis
    "analyze_query_performance",
    "get_business_metrics",
]

