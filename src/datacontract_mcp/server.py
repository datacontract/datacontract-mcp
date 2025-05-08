import logging

from dotenv import load_dotenv
from mcp.server import FastMCP
from typing import Union, List, Dict, Any

load_dotenv()

from .asset_manager import DataAssetManager
from .asset_identifier import AssetIdentifier, DataAssetType

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

Available tools:
- dataproducts_list - Lists all available Data Products
- dataproducts_get - Returns a single Data Product by identifier
- dataproducts_get_output_schema - Gets the data contract schema for a specific output port
- dataproducts_query - Query data from a data product's output port
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
    # Check if this is an asset identifier format (contains : and / in expected format)
    if ":" in identifier and "/" in identifier and identifier.split(":", 1)[1].split("/", 1)[0] == "contract":
        try:
            # Parse as standard asset identifier
            asset_identifier = AssetIdentifier.from_string(identifier)

            # Verify this is a contract identifier
            if not asset_identifier.is_contract():
                raise ValueError(f"Identifier does not refer to a data contract: {identifier}")

            # Return the complete contract content
            return DataAssetManager.get_asset_content(asset_identifier)
        except Exception as e:
            logger.error(f"Error processing contract identifier '{identifier}': {str(e)}")
            raise
    else:
        # Handle URN format or plain ID by finding the contract by ID
        try:
            # Use helper method to find contract by ID
            contract_identifier, contract_dict = DataAssetManager._find_contract_by_id(identifier)

            if not contract_identifier:
                raise ValueError(f"Could not find data contract with ID: {identifier}")

            # Return the complete contract content
            return DataAssetManager.get_asset_content(contract_identifier)
        except Exception as e:
            logger.error(f"Error finding contract with ID '{identifier}': {str(e)}")
            raise

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
    # Check if this is an asset identifier format (contains : and / in expected format)
    if ":" in identifier and "/" in identifier and identifier.split(":", 1)[1].split("/", 1)[0] == "product":
        try:
            # Parse as standard asset identifier
            asset_identifier = AssetIdentifier.from_string(identifier)

            # Verify this is a product identifier
            if not asset_identifier.is_product():
                raise ValueError(f"Identifier does not refer to a product: {identifier}")

            # Query using the asset identifier
            return DataAssetManager.query_product(
                product_identifier=asset_identifier,
                query=query,
                port_id=port_id,
                server_key=server,
                model_key=model,
                include_metadata=include_metadata
            )
        except Exception as e:
            logger.error(f"Error processing product identifier '{identifier}': {str(e)}")
            raise
    else:
        # For URN format or plain ID, we need to find the product first
        try:
            # Find product by ID (using _find_asset_by_type_and_id under the hood)
            product_identifier, product_dict = DataAssetManager._find_asset_by_type_and_id(
                DataAssetType.DATA_PRODUCT, identifier
            )

            if not product_identifier:
                raise ValueError(f"Could not find data product with ID: {identifier}")

            # Query using the found product identifier
            return DataAssetManager.query_product(
                product_identifier=product_identifier,
                query=query,
                port_id=port_id,
                server_key=server,
                model_key=model,
                include_metadata=include_metadata
            )
        except Exception as e:
            logger.error(f"Error finding product with ID '{identifier}': {str(e)}")
            raise

def main():
    """Entry point for CLI execution"""
    app.run(transport="stdio")


if __name__ == "__main__":
    main()
