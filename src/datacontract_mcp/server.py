import logging
from typing import Union, List, Dict, Any

from dotenv import load_dotenv
from mcp.server import FastMCP

load_dotenv()

from .asset_manager import DataAssetManager
from .types import DataAssetType
from .asset_identifier import AssetIdentifier

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("datacontract-mcp")

app = FastMCP("datacontract-mcp")

# Prompts
@app.prompt(name="Initial Prompt")
def initial_prompt() -> str:
    return """
You are now connected to the Data Contract and Data Product server through the Model Context Protocol (MCP).

IMPORTANT - ALWAYS follow these guidelines when working with data assets:

1. ASSET IDENTIFIERS
 All assets are identified using this format: [source]:[type]/[id]

 Examples:
 - local:product/orders.dataproduct.yaml
 - local:contract/orders.datacontract.yaml
 - datameshmanager:product/orders
 - datameshmanager:contract/orders_pii_v2

2. MAINTAINING IDENTIFIER CONSISTENCY
 - When you receive a response from any dataproduct tool, note the source (e.g., "datameshmanager")
 - When making subsequent requests about the same asset or related assets, use the SAME SOURCE
 - ALWAYS use the full identifier format with source prefix when referencing assets

3. WORKING WITH OUTPUT PORTS AND DATA CONTRACTS
 - Data products contain output ports which may be linked to data contracts
 - When working with an output port from a data product (e.g., datameshmanager:product/orders)
 - Use the SAME SOURCE when requesting its data contract (e.g., datameshmanager:contract/orders_pii_v2)
 - NEVER request a contract with just the ID portion (e.g., "orders_pii_v2")

4. COMMON MISTAKES TO AVOID
 - DO NOT omit the source prefix when referencing assets
 - DO NOT switch sources when working with related assets
 - DO NOT assume a data contract ID is just the name without the source prefix

5. EXAMPLES OF CORRECT USAGE:
 - When you discover datameshmanager:product/orders with an output port linked to contract "orders_pii_v2"
 - CORRECT: Use datameshmanager:contract/orders_pii_v2 for subsequent requests
 - INCORRECT: Just using "orders_pii_v2" without the source prefix

6. FEDERATED QUERY CAPABILITIES
 - Query data across multiple data products using the dataproducts_query tool with multiple sources
 - Join data from different sources with SQL
 - Use aliases to reference tables in queries

7. QUERY TABLE NAMES
 - When writing SQL queries, use the actual table names from the data products
 - For single source queries, the table name typically matches the product name (e.g., "orders" for orders.dataproduct.yaml)
 - For federated queries, use the alias provided in the sources configuration
 - DO NOT use generic names like "source_data" in your queries

8. PLANNING FEDERATED QUERIES
 To effectively plan queries across multiple data products:

 a) SCHEMA DISCOVERY
   - Get data product definitions with dataproducts_get
   - Extract output port information and their data contracts
   - Analyze field names, types, and descriptions to understand each data source
 
 b) IDENTIFYING JOIN RELATIONSHIPS
   - Look for common field names that suggest relationships (e.g., customer_id)
   - Check field descriptions for references to other data products
   - Examine example data to understand value patterns and compatibility
 
 c) QUERY CONSTRUCTION
   - Use meaningful table aliases in your queries
   - Prefer selective column projection over SELECT *
   - Use appropriate join types (INNER, LEFT, RIGHT) based on data relationships
   - Consider type compatibility for join columns

Available tools:
- dataproducts_list - Lists all available Data Products
- dataproducts_get - Returns a single Data Product by identifier
- dataproducts_get_output_schema - Gets the data contract schema for a specific output port
- dataproducts_query - Query data from one or more data products
    """

# Resources
@app.resource("dataproduct-ref://schema", name="Data Product Schema")
async def dataproduct_schema() -> str:
    """The official Data Product JSON schema"""
    logger.debug("Fetching schema")
    return DataAssetManager.get_schema(asset_type=DataAssetType.DATA_PRODUCT)

@app.resource("dataproduct-ref://example", name="Data Product Example")
async def dataproduct_example() -> str:
    """A concrete example of a Data Product"""
    logger.debug("Fetching example")
    return DataAssetManager.get_example(asset_type=DataAssetType.DATA_PRODUCT)

@app.resource("datacontract-ref://schema", name="Data Contract Schema")
async def datacontract_schema() -> str:
    """The official Data Contract JSON schema"""
    logger.debug("Fetching schema")
    return DataAssetManager.get_schema(asset_type=DataAssetType.DATA_CONTRACT)

# Data Product tools
@app.tool("dataproducts_list")
async def dataproducts_list() -> List[Dict[str, str]]:
    """Lists all available Data Products."""
    identifiers = DataAssetManager.list_assets(DataAssetType.DATA_PRODUCT)
    return [{"id": str(identifier), "source": identifier.source} for identifier in identifiers]

@app.tool("dataproducts_get")
async def dataproducts_get(identifier: str) -> str:
    """
    Return the content of a single Data Product.

    Args:
        identifier: Asset identifier (e.g. 'local:product/orders.dataproduct.yaml' or 'datameshmanager:product/123')
    """
    # Parse the identifier string
    asset_identifier = AssetIdentifier.from_string(identifier)
    if not asset_identifier.is_product():
        raise ValueError(f"Identifier does not refer to a product: {identifier}")

    return DataAssetManager.get_asset_content(asset_identifier)

@app.tool("dataproducts_get_output_schema")
async def dataproducts_get_output_port(identifier: str) -> str:
    """
    Get the full data contract specified by the identifier.

    Args:
        identifier: The contract identifier, which can be in different formats:
                   - Asset identifier format: 'local:contract/orders.datacontract.yaml' or 'datameshmanager:contract/123'
                   - URN format: 'urn:datacontract:checkout:orders-latest'
                   - Plain ID: 'orders-latest'
                   This should be the dataContractId value from a data product's output port.

    Returns:
        The complete data contract content
    """
    return DataAssetManager.get_contract_by_id(identifier)

@app.tool("dataproducts_query")
async def dataproducts_query(
    sources: List[Dict[str, Any]],
    query: str,
    include_metadata: bool = False
) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Query data from one or more data products.

    Args:
        sources: List of source configurations. Each source should contain:
                - product_id: Full product identifier (required)
                - port_id: Optional output port ID
                - server: Optional server key
                - model: Optional model key
                - alias: Optional alias to use in the query (recommended for multi-source queries)
        query: SQL query to execute
        include_metadata: Whether to include metadata in the response

    Returns:
        Query results from either a single source or multiple joined sources

    Notes:
        - Table names in the SQL query should match the data structure in each source
        - For single-source queries, use the name of the table in the data product (often the same as the product name)
        - For federated queries, use the source's alias or refer to each table by its actual name

    Examples:
        Single product query:
        ```
        {
            "sources": [
                {
                    "product_id": "local:product/orders.dataproduct.yaml"
                }
            ],
            "query": "SELECT * FROM orders LIMIT 10"
        }
        ```

        Federated query across multiple products:
        ```
        {
            "sources": [
                {
                    "product_id": "local:product/orders.dataproduct.yaml",
                    "alias": "orders"
                },
                {
                    "product_id": "local:product/customers.dataproduct.yaml",
                    "alias": "customers"
                }
            ],
            "query": "SELECT o.order_id, o.customer_id, c.name, o.total FROM orders o JOIN customers c ON o.customer_id = c.id"
        }
        ```
    """
    try:
        # Validate input
        if not sources:
            raise ValueError("At least one source must be provided")

        if not query:
            raise ValueError("Query cannot be empty")

        # Create an asset manager and execute the query
        asset_manager = DataAssetManager()
        return asset_manager.execute_query(
            sources=sources,
            query=query,
            include_metadata=include_metadata
        )
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        raise

def main():
    """Entry point for CLI execution"""
    app.run(transport="stdio")


if __name__ == "__main__":
    main()
