python -m src.redshift_mcp_server.redshift_mcp_server                         


"  - python -m src.redshift_mcp_server.redshift_mcp_server - Python treats src as a proper package and resolves imports relative to the current working
  directory

  The -m flag makes Python add the current directory to sys.path and properly handle the package imports like from src.redshift_mcp_server.db_utils import 
  "