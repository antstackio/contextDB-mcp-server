import logging
from mcp.server.fastmcp.server import FastMCP

# init logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('redshift_mcp_log.out')
    ]
)
logger = logging.getLogger('redshift-mcp-server')

server = FastMCP(
    name="redshift-mcp-server",
    instructions="A Model Context Protocol (MCP) server for Amazon Redshift that enables AI assistants to interact with Redshift databases.",
)

if __name__ == "__main__":
    server.run(transport="sse")
