import logging

from dotenv import load_dotenv
from mcp.server import FastMCP
from typing import Union, List, Dict, Any

load_dotenv()

from .asset_manager import DataAssetManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("datacontract-mcp")

app = FastMCP("datacontract-mcp")

# Resources
@app.resource("dataproduct-ref://schema")
async def dataproduct_schema() -> str:
    """The official Data Product JSON schema"""
    logger.debug("Fetching schema")
    return DataAssetManager.get_product_schema()

@app.resource("dataproduct-ref://example")
async def dataproduct_example() -> str:
    """A concrete example of a Data Product"""
    logger.debug("Fetching example")
    return DataAssetManager.get_product_example()

# Data Product tools
@app.tool("dataproducts_list")
async def dataproducts_list() -> List[str]:
    """Lists all available Data Products (by filename)."""
    return DataAssetManager.list_products()

@app.tool("dataproducts_get")
async def dataproducts_get(filename: str) -> str:
    """Return the content of a single Data Product."""
    return DataAssetManager.get_product_content(filename)

@app.tool("dataproducts_validate")
async def dataproducts_validate(filename: str) -> Dict[str, Any]:
    """Validate a data product and return its structured representation."""
    return DataAssetManager.get_product(filename)

@app.tool("dataproducts_get_outputs")
async def dataproducts_get_outputs(filename: str) -> Dict[str, Any]:
    """
    Get all output ports from a data product with linked contracts.
    
    Args:
        filename: Name of the data product file
        
    Returns:
        Dictionary with output port information and any linked contracts
    """
    return DataAssetManager.get_product_outputs(filename)

@app.tool("dataproducts_get_output_schema")
async def dataproducts_get_output_schema(filename: str, port_id: str) -> Dict[str, Any]:
    """
    Get the schema for a specific output port (using its linked data contract).
    
    Args:
        filename: Name of the data product file
        port_id: ID of the output port
        
    Returns:
        Schema information for the output port data
    """
    return DataAssetManager.get_output_schema(filename, port_id)

@app.tool("dataproducts_query")
async def dataproducts_query(
    filename: str,
    query: str,
    port_id: str = None,
    server: str = None,
    model: str = None,
    include_metadata: bool = False
) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Query data from a data product's output port.
    
    Args:
        filename: Name of the data product file
        query: SQL query to execute
        port_id: Optional ID of the output port (uses first port if not specified)
        server: Optional server key to use
        model: Optional model key to use
        include_metadata: Include metadata in the response
        
    Returns:
        Query results
    """
    return DataAssetManager.query_product(
        product_filename=filename,
        query=query,
        port_id=port_id,
        server_key=server,
        model_key=model,
        include_metadata=include_metadata
    )

def main():
    """Entry point for CLI execution"""
    app.run(transport="stdio")

if __name__ == "__main__":
    main()