import logging

from dotenv import load_dotenv
from mcp.server import FastMCP

load_dotenv()

from .resources import docs
from . import datacontract

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("datacontract-mcp")

app = FastMCP("datacontract-mcp")

# MCP server resources
@app.resource("datacontract-ref://schema")
async def datacontract_schema() -> str:
    """The official Data Contract JSON schema"""
    logger.debug("Fetching schema")
    return docs.get_datacontract_schema()

@app.resource("datacontract-ref://example")
async def datacontract_example() -> str:
    """A concrete example of a Data Contract from the domain 'retail'"""
    logger.debug("Fetching example")
    return docs.get_datacontract_example()

# MCP server tools
@app.tool("datacontracts_get_schema")
async def datacontracts_get_schema() -> str:
    """Get the Data Contract schema."""
    return docs.get_datacontract_schema()

@app.tool("datacontracts_list_datacontracts")
async def datacontracts_list_datacontracts() -> list[str]:
    """Lists all available Data Contracts (by filename)."""
    return datacontract.list_contract_files()

@app.tool("datacontracts_get_datacontract")
async def datacontracts_get_datacontract(filename: str) -> str:
    """Return the content of a single Data Contract."""
    return datacontract.load_contract_file(filename)

@app.tool("datacontracts_query_datacontract")
async def datacontracts_query_datacontract(filename: str, query: str, server: str = None, model: str = None) -> list[dict]:
    """Execute a ready-only query against source defined in a Data Contract YAML"""
    return datacontract.execute_query(
        filename=filename,
        query=query,
        server_key=server,
        model_key=model
    )


def main():
    """Entry point for CLI execution"""
    app.run(transport="stdio")

if __name__ == "__main__":
    main()
