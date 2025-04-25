import logging

from dotenv import load_dotenv
from mcp.server import FastMCP
from typing import Union, List, Dict, Any

load_dotenv()

from .resources import docs
from . import datacontract
from . import dataproduct

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

@app.resource("dataproduct-ref://schema")
async def dataproduct_schema() -> str:
    """The official Data Product JSON schema"""
    logger.debug("Fetching schema")
    return docs.get_dataproduct_schema()

@app.resource("dataproduct-ref://example")
async def dataproduct_example() -> str:
    """A concrete example of a Data Product"""
    logger.debug("Fetching example")
    return docs.get_dataproduct_example()

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

@app.tool("datacontracts_validate")
async def datacontracts_validate(filename: str) -> dict:
    """Validate a data contract and return its structured representation"""
    # Parse and validate the contract using Pydantic
    contract = datacontract.get_contract(filename)
    # Return the model as a dictionary
    return contract.model_dump()

@app.tool("datacontracts_query_datacontract")
async def datacontracts_query_datacontract(filename: str, query: str, server: str = None, model: str = None, include_metadata: bool = False) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    """Execute a read-only query against source defined in a Data Contract YAML

    Args:
        filename: Name of the data contract file
        query: SQL query to execute
        server: Optional server key to use
        model: Optional model key to use
        include_metadata: Set to True to include metadata in response

    Returns:
        Just the query records by default, or full result with metadata if include_metadata=True
    """
    # Execute query with structured result
    result = datacontract.query_contract(
        filename=filename,
        query=query,
        server_key=server,
        model_key=model
    )

    # Return full model or just records based on parameter
    return result.model_dump() if include_metadata else result.records

@app.tool("dataproducts_get_schema")
async def dataproducts_get_schema() -> str:
    """Get the Data Product schema."""
    return docs.get_dataproduct_schema()

@app.tool("dataproducts_get_example")
async def dataproducts_get_example() -> str:
    """Get an example Data Product."""
    return docs.get_dataproduct_example()

@app.tool("dataproducts_list_dataproducts")
async def dataproducts_list_dataproducts() -> list[str]:
    """Lists all available Data Products (by filename)."""
    return dataproduct.list_product_files()

@app.tool("dataproducts_get_dataproduct")
async def dataproducts_get_dataproduct(filename: str) -> str:
    """Return the content of a single Data Product."""
    return dataproduct.load_product_file(filename)

@app.tool("dataproducts_validate")
async def dataproducts_validate(filename: str) -> dict:
    """Validate a data product and return its structured representation"""
    # Parse and validate the product using Pydantic
    product = dataproduct.get_product(filename)
    # Return the model as a dictionary
    return product.model_dump()


def main():
    """Entry point for CLI execution"""
    app.run(transport="stdio")

if __name__ == "__main__":
    main()
