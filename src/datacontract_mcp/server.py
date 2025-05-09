import logging
from typing import Union, List, Dict, Any

from dotenv import load_dotenv
from mcp.server import FastMCP

load_dotenv()

from .asset_manager import DataAssetManager
from .types import DataAssetType
from .asset_identifier import AssetIdentifier
from .query import QuerySource, FederatedQueryEngine

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
 - Query data across multiple data products using the dataproducts_query_federated tool
 - Join data from different sources with SQL
 - Use aliases to reference tables in queries

7. PLANNING FEDERATED QUERIES
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
- dataproducts_query - Query data from a data product's output port
- dataproducts_query_federated - Query data across multiple data products
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
    identifier: str,
    query: str,
    port_id: str = None,
    server: str = None,
    model: str = None,
    include_metadata: bool = False
) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Query data from a data product's output port.

    Args:
        identifier: The product identifier, which can be in different formats:
                  - Asset identifier format: 'local:product/orders.dataproduct.yaml' or 'datameshmanager:product/123'
                  - URN format: 'urn:dataproduct:checkout:orders'
                  - Plain ID: 'orders'
        query: SQL query to execute
        port_id: Optional ID of the output port (uses first port if not specified)
        server: Optional server key to use
        model: Optional model key to use
        include_metadata: Include metadata in the response

    Returns:
        Query results
    """
    return DataAssetManager.query_by_identifier_string(
        identifier=identifier,
        query=query,
        port_id=port_id,
        server=server,
        model=model,
        include_metadata=include_metadata
    )

@app.tool("dataproducts_query_federated")
async def dataproducts_query_federated(
    sources: List[Dict[str, str]],
    query: str,
    include_metadata: bool = False
) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Execute a federated query across multiple data products.
    
    This tool enables you to join and query data from multiple data products in a single operation.
    To plan effective federated queries:
    
    1. SCHEMA DISCOVERY
       - First use dataproducts_get to examine each product's structure
       - Check output ports and their associated data contracts
       - Understand field names, types, and semantics for proper joins
    
    2. RELATIONSHIP ANALYSIS
       - Look for common field names across products (e.g., customer_id, product_id)
       - Check data contract field descriptions for relationship hints
       - Consider field semantics and naming patterns
    
    3. QUERY OPTIMIZATION
       - Use meaningful table aliases for readability
       - Prefer specific column selection over SELECT *
       - Join smaller datasets to larger ones when possible
       - Consider type compatibility for join fields
    
    Args:
        sources: List of source configurations, each containing:
                - product_id: Full product identifier (required)
                - port_id: Optional output port ID
                - server: Optional server key
                - model: Optional model key
                - alias: Optional alias to use in the query
        query: SQL query to execute across the sources
        include_metadata: Whether to include metadata in the response
    
    Returns:
        Query results combined from all sources
    
    Example:
        To join orders and customers data:
        
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
        
        # Convert source dictionaries to QuerySource objects
        query_sources = []
        for source_dict in sources:
            if "product_id" not in source_dict:
                raise ValueError(f"Missing required 'product_id' field in source: {source_dict}")
            
            # Create a QuerySource object from the dictionary
            query_source = QuerySource(
                product_id=source_dict["product_id"],
                port_id=source_dict.get("port_id"),
                server=source_dict.get("server"),
                model=source_dict.get("model"),
                alias=source_dict.get("alias")
            )
            query_sources.append(query_source)
        
        # Create a federated query engine
        asset_manager = DataAssetManager()
        engine = FederatedQueryEngine(asset_manager)
        
        # Execute the query
        results = engine.execute_query(query, sources=query_sources)
        
        # Format the result based on include_metadata flag
        if include_metadata:
            # Create metadata for federated query
            metadata = {
                "query": query,
                "sources": [{"product_id": s.product_id, "port_id": s.port_id} for s in query_sources],
                "federated": True,
                "record_count": len(results)
            }
            
            return {
                "metadata": metadata,
                "records": results
            }
        else:
            return results
            
    except Exception as e:
        logger.error(f"Error executing federated query: {str(e)}")
        raise

def main():
    """Entry point for CLI execution"""
    app.run(transport="stdio")


if __name__ == "__main__":
    main()