"""Unified manager for data contracts and data products."""

import logging
import contextlib
from typing import Dict, List, Optional, Any, Union, Tuple, Generator

from .utils.yaml_utils import parse_yaml, AssetParseError


class AssetLoadError(Exception):
    """Error raised when loading an asset file fails."""
    pass


class AssetQueryError(Exception):
    """Error raised when querying an asset fails."""
    pass

from .types import DataAssetType
from .asset_identifier import AssetIdentifier

from .sources.data_source import ServerType
from .sources.data_source import DataSourceRegistry
from .sources.asset_source import AssetSourceRegistry
from .resources import docs

# Mapping of output port types to server types
PORT_TYPE_TO_SERVER_TYPE = {
    "s3": ServerType.S3,
    "local": ServerType.LOCAL,
    "file": ServerType.LOCAL,
    "bigquery": ServerType.BIGQUERY,
    "snowflake": ServerType.SNOWFLAKE,
    "redshift": ServerType.REDSHIFT,
    "postgres": ServerType.POSTGRES,
}

logger = logging.getLogger("datacontract-mcp.asset_manager")

@contextlib.contextmanager
def handle_asset_errors(
    operation_description: str,
    context_identifier: Optional[Any] = None,
    reraise_types: Tuple[type, ...] = (AssetLoadError, AssetParseError, AssetQueryError)
) -> Generator[None, None, None]:
    """
    Context manager for consistent error handling across asset operations.

    Args:
        operation_description: Description of the operation being performed
        context_identifier: Optional context identifier (like product ID or file path)
        reraise_types: Tuple of exception types to re-raise with original type

    Yields:
        None

    Raises:
        Original exception if it's in reraise_types
        AssetQueryError for all other exceptions
    """
    try:
        yield
    except reraise_types as e:
        # Re-raise these exceptions directly
        context_str = f" on {context_identifier}" if context_identifier else ""
        logger.error(f"Error {operation_description}{context_str}: {str(e)}")
        raise
    except Exception as e:
        # Wrap other exceptions as AssetQueryError
        context_str = f" on {context_identifier}" if context_identifier else ""
        error_msg = f"Unexpected error {operation_description}{context_str}: {str(e)}"
        logger.error(error_msg)
        raise AssetQueryError(error_msg) from e

class DataAssetManager:
    """Manager for unified access to data contracts and data products."""

    def __init__(self):
        """Initialize the DataAssetManager."""

    # Generic asset methods
    @staticmethod
    def get_schema(asset_type: DataAssetType) -> str:
        """
        Get the JSON schema for a specific asset type.

        Args:
            asset_type: Type of asset (contract or product)

        Returns:
            JSON schema as string
        """
        if asset_type == DataAssetType.DATA_CONTRACT:
            return docs.get_datacontract_schema()
        elif asset_type == DataAssetType.DATA_PRODUCT:
            return docs.get_dataproduct_schema()
        else:
            raise ValueError(f"Unsupported asset type: {asset_type}")

    @staticmethod
    def get_example(asset_type: DataAssetType) -> str:
        """
        Get an example of a specific asset type.

        Args:
            asset_type: Type of asset (contract or product)

        Returns:
            Example as string
        """
        if asset_type == DataAssetType.DATA_CONTRACT:
            return docs.get_datacontract_example()
        elif asset_type == DataAssetType.DATA_PRODUCT:
            return docs.get_dataproduct_example()
        else:
            raise ValueError(f"Unsupported asset type: {asset_type}")

    @staticmethod
    def list_assets(asset_type: DataAssetType) -> List[AssetIdentifier]:
        """
        List all available assets of a specific type across all sources.

        Args:
            asset_type: Type of asset (contract or product)

        Returns:
            List of AssetIdentifier objects
        """
        return AssetSourceRegistry.list_assets(asset_type)

    @staticmethod
    def get_asset_content(asset_identifier: AssetIdentifier) -> str:
        """
        Get the raw content of an asset.

        Args:
            asset_identifier: Asset identifier

        Returns:
            Asset contents as string

        Raises:
            AssetLoadError: If loading fails
        """
        try:
            return AssetSourceRegistry.load_content(asset_identifier)
        except Exception as e:
            # Convert source-specific error to the general asset error
            raise AssetLoadError(str(e))

    @staticmethod
    def get_contract_by_id(identifier: str) -> str:
        """
        Get data contract content by ID, handling various identifier formats.

        Args:
            identifier: The contract identifier, which can be in different formats:
                       - Asset identifier format: 'local:contract/orders.datacontract.yaml'
                       - URN format: 'urn:datacontract:checkout:orders-latest'
                       - Plain ID: 'orders-latest'

        Returns:
            Data contract content as string

        Raises:
            ValueError: If no contract with the given ID is found
            AssetLoadError: If loading fails
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

    def execute_query(
        self,
        sources: List[Dict[str, Any]],
        query: str,
        include_metadata: bool = False
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Unified query method that handles both single-source and federated queries.

        Args:
            sources: List of source configurations. Each source should have:
                    - product_id: Full product identifier (required)
                    - port_id: Optional output port ID
                    - server: Optional server key
                    - model: Optional model key
                    - alias: Optional alias to use in the query
            query: SQL query to execute
            include_metadata: Whether to include metadata in the response

        Returns:
            Query results

        Raises:
            ValueError: If invalid parameters are provided
            AssetQueryError: If query execution fails
        """
        # Import here to avoid circular dependency
        from .query import QuerySource, FederatedQueryEngine

        if not sources:
            raise ValueError("At least one source must be provided")

        # Convert source dictionaries to QuerySource objects
        query_sources = []
        for source_dict in sources:
            if "product_id" not in source_dict:
                raise ValueError(f"Missing required 'product_id' field in source: {source_dict}")

            query_source = QuerySource(
                product_id=source_dict["product_id"],
                port_id=source_dict.get("port_id"),
                server=source_dict.get("server"),
                model=source_dict.get("model"),
                alias=source_dict.get("alias")
            )
            query_sources.append(query_source)

        # Create a federated query engine and execute
        engine = FederatedQueryEngine(self)
        results = engine.execute_query(query, sources=query_sources)

        # Format the result based on include_metadata flag
        if include_metadata:
            metadata = {
                "query": query,
                "sources": [{
                    "product_id": s.product_id,
                    "port_id": s.port_id
                } for s in query_sources],
                "federated": True,
                "record_count": len(results)
            }

            return {
                "metadata": metadata,
                "records": results
            }
        else:
            return results

    @staticmethod
    def query_product(
            identifier: AssetIdentifier,
            query: str,
            port_id: Optional[str] = None,
            server_key: Optional[str] = None,
            model_key: Optional[str] = None,
            include_metadata: bool = False
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Query data from a data product output port.

        This method can work in two ways:
        1. If the output port has a reference to a data contract, it will use that contract
        2. If no data contract is available, it will query the output port directly using its server information

        Args:
            identifier: Identifier for the data product
            query: SQL query to execute
            port_id: Optional ID of the output port (uses first port if not specified)
            server_key: Optional key of the server to use in the contract (if using data contract)
            model_key: Optional key of the model to use (if using data contract) or table name (if direct querying)
            include_metadata: Whether to include metadata in the response

        Returns:
            If include_metadata is False: List of record dictionaries
            If include_metadata is True: Dictionary with query metadata and results

        Raises:
            AssetLoadError: If files cannot be loaded
            AssetParseError: If the assets are invalid
            AssetQueryError: If the query execution fails or if required resources aren't found
        """
        matching_contract_identifier = None  # Initialize to avoid fragile checking

        with handle_asset_errors("querying product", identifier):
            # Load and parse the data product using our helper
            product = DataAssetManager._load_and_parse_asset(identifier)

            # Find the specified output port or use the first one
            port = DataAssetManager._get_output_port(product, port_id)

            # Check if the port has a data contract reference
            port_id = port.get("id", "unknown")
            contract_id = port.get("dataContractId")

            if contract_id:
                # Find the contract by ID using our helper method
                matching_contract_identifier, contract = DataAssetManager._find_contract_by_id(contract_id)

                if not matching_contract_identifier or not contract:
                    raise AssetQueryError(f"Couldn't find data contract with ID '{contract_id}'")

                # Query the contract using our internal method
                result = DataAssetManager._query_from_data_contract(
                    contract=contract,
                    query=query,
                    server_key=server_key,
                    model_key=model_key
                )
            elif port.get("server"):
                # If no contract but server info is available, query directly from the output port
                result = DataAssetManager._query_from_data_product(
                    port=port,
                    query=query,
                    model_key=model_key or port_id
                )
            else:
                raise AssetQueryError(f"Output port '{port_id}' has neither a data contract reference nor server information")

            # Format the response
            return DataAssetManager._format_query_response(
                result=result,
                product=product,
                product_identifier=identifier,
                port=port,
                query=query,
                model_key=model_key,
                matching_contract_identifier=matching_contract_identifier,
                include_metadata=include_metadata
            )

    @staticmethod
    def _load_and_parse_asset(asset_identifier: AssetIdentifier) -> Dict[str, Any]:
        """
        Load and parse an asset as a dictionary.

        Args:
            asset_identifier: Identifier for the asset

        Returns:
            Parsed asset dictionary

        Raises:
            AssetLoadError: If loading fails
            AssetParseError: If parsing fails
        """
        with handle_asset_errors("loading and parsing asset", asset_identifier):
            content = DataAssetManager.get_asset_content(asset_identifier)
            return parse_yaml(content)

    @staticmethod
    def _find_asset_by_type_and_id(
            asset_type: DataAssetType,
            asset_id: str
    ) -> Tuple[Optional[AssetIdentifier], Optional[Dict[str, Any]]]:
        """
        Find an asset by its type and ID.

        Args:
            asset_type: Type of asset to find
            asset_id: ID of the asset to find

        Returns:
            Tuple of (asset_identifier, asset_dict) if found, or (None, None) if not found
        """
        identifiers = DataAssetManager.list_assets(asset_type)

        for identifier in identifiers:
            try:
                # Load and parse the asset
                asset_dict = DataAssetManager._load_and_parse_asset(identifier)

                if asset_dict.get("id") == asset_id:
                    return identifier, asset_dict
            except (AssetLoadError, AssetParseError):
                continue

        return None, None

    @staticmethod
    def _find_contract_by_id(contract_id: str) -> Tuple[Optional[AssetIdentifier], Optional[Dict[str, Any]]]:
        """
        Find a data contract by its ID.

        Args:
            contract_id: ID of the contract to find, which can be in various formats:
                        - Simple ID (e.g., 'snowflake_customers_latest_npii_v1')
                        - Source-prefixed ID (e.g., 'datameshmanager:contract/snowflake_customers_latest_npii_v1')
                        - URN format (e.g., 'urn:datacontract:sales:customers')

        Returns:
            Tuple of (contract_identifier, contract_dict) if found, or (None, None) if not found
        """
        # First, extract the simple ID if in prefixed format
        simple_id = contract_id

        # Handle source-prefixed format
        if ":" in contract_id and "/" in contract_id:
            parts = contract_id.split("/", 1)
            if len(parts) > 1:
                # Extract the actual ID part after the source:type/ prefix
                simple_id = parts[1]

        # Also handle URN format if needed (urn:datacontract:domain:name)
        elif contract_id.startswith("urn:datacontract:") and contract_id.count(":") >= 3:
            # Get the last part of the URN which is typically the ID
            simple_id = contract_id.split(":")[-1]

        logger.info(f"Looking for contract with ID '{contract_id}', simplified to '{simple_id}'")

        # Try to find by simple ID first
        result = DataAssetManager._find_asset_by_type_and_id(
            DataAssetType.DATA_CONTRACT, simple_id
        )

        # If found, return the result
        if result[0] is not None:
            return result

        # If not found and the original ID was different, try with the original
        if simple_id != contract_id:
            logger.info(f"Contract not found with simplified ID, trying original ID: '{contract_id}'")
            return DataAssetManager._find_asset_by_type_and_id(
                DataAssetType.DATA_CONTRACT, contract_id
            )

        # Not found with either ID
        return None, None


    # Product output port methods (public)

    @staticmethod
    def _get_output_port(product: Dict[str, Any], port_id: Optional[str]) -> Dict[str, Any]:
        """
        Get an output port from a data product dictionary.

        Args:
            product: Data product dictionary
            port_id: Optional ID of the output port (uses first port if not specified)

        Returns:
            The output port as a dictionary

        Raises:
            AssetQueryError: If the port is not found or if the product has no ports
        """
        # Get the product ID for error messages
        product_id = product.get("id", "unknown")

        # Get output ports
        output_ports = product.get("outputPorts", [])
        if not isinstance(output_ports, list):
            raise AssetQueryError(f"Invalid outputPorts in product {product_id}")

        if port_id:
            # Find port by ID
            port = next(
                (p for p in output_ports if p.get("id") == port_id),
                None
            )
            if not port:
                raise AssetQueryError(f"Output port '{port_id}' not found in product {product_id}")
        else:
            # Check if outputPorts exists and has elements
            if not output_ports:
                raise AssetQueryError(f"Data product {product_id} has no output ports")
            port = output_ports[0]

        return port

    @staticmethod
    def _format_query_response(
            result: Union[Dict[str, Any], List[Dict[str, Any]]],
            product: Dict[str, Any],
            product_identifier: AssetIdentifier,
            port: Dict[str, Any],
            query: str,
            model_key: Optional[str],
            matching_contract_identifier: Optional[AssetIdentifier] = None,
            include_metadata: bool = False
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Format query response with optional metadata.

        Args:
            result: Query result (either result dictionary or raw records)
            product: Data product dictionary
            product_identifier: Data product identifier
            port: Output port dictionary
            query: SQL query executed
            model_key: Model key used
            matching_contract_identifier: Optional contract identifier
            include_metadata: Whether to include metadata

        Returns:
            Formatted query response (raw records or metadata dictionary)
        """
        if not include_metadata:
            return result.get("records", result) if isinstance(result, dict) and "records" in result else result

        # Get product ID and port ID
        product_id = product.get("id", "unknown")
        port_id = port.get("id", "unknown")

        # Format with metadata
        metadata = {
            "product": {
                "id": product_id,
                "identifier": str(product_identifier),
                "output_port": port_id
            }
        }

        # Add contract info if available
        port_contract_id = port.get("dataContractId")
        if port_contract_id and matching_contract_identifier:
            metadata["contract"] = {
                "id": port_contract_id,
                "identifier": str(matching_contract_identifier)
            }

        # Add query result data
        if isinstance(result, dict) and "records" in result:
            metadata["query_result"] = result
        else:
            # Get server info
            server_type = "unknown"
            if "server" in port and isinstance(port["server"], dict):
                server_type = port["server"].get("type", "unknown")

            metadata["query_result"] = {
                "records": result,
                "query": query,
                "model_key": model_key or port_id,
                "server_type": server_type
            }

        return metadata

    @staticmethod
    def _create_server_config_from_port(port: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a server configuration from an output port dictionary.

        Args:
            port: Output port dictionary

        Returns:
            Server configuration dictionary

        Raises:
            AssetQueryError: If server information is missing
        """
        # Get port ID for error messages
        port_id = port.get("id", "unknown")

        # Get server information from port
        server = port.get("server")
        port_type = port.get("type")
        port_location = port.get("location")

        # Validate server information
        if not server:
            # Try using type and location if server is not specified
            if port_type and port_location:
                logger.warning(f"Output port '{port_id}' doesn't have server object, using type and location")
                # Create simple server configuration from port type and location
                return {"location": port_location}
            else:
                raise AssetQueryError("Output port doesn't have server information")
        else:
            # Return server as is
            return server

    @staticmethod
    def _resolve_server_type(port: Dict[str, Any], server_config: Dict[str, Any]) -> str:
        """
        Resolve the server type from port information and server configuration.

        Args:
            port: Output port dictionary
            server_config: Server configuration dictionary

        Returns:
            Resolved server type as a string

        Raises:
            AssetQueryError: If server type cannot be resolved
        """
        # Get port type from dictionary
        port_type_orig = port.get("type")

        # Get server config type
        server_type_str = server_config.get("type", "")

        # Normalize port type
        port_type = port_type_orig.lower() if port_type_orig else server_type_str.lower()

        # Look up server type in the mapping
        server_type = PORT_TYPE_TO_SERVER_TYPE.get(port_type)

        # Default to local if type is unknown but location exists
        if server_type is None:
            if "location" in server_config:
                server_type = ServerType.LOCAL
                logger.warning(f"Unknown server type '{port_type}', defaulting to LOCAL")
            else:
                raise AssetQueryError(f"Unsupported server type '{port_type}' for direct querying")

        # Ensure we always return a string value
        if isinstance(server_type, str):
            return server_type
        else:
            # Convert any ServerType enum object to its string value
            for attr_name in dir(ServerType):
                if attr_name.isupper() and getattr(ServerType, attr_name) == server_type:
                    return getattr(ServerType, attr_name)

            # Fallback: convert to string if all else fails
            return str(server_type)

    @staticmethod
    def _query_from_data_contract(
            contract: Dict[str, Any],
            query: str,
            server_key: Optional[str] = None,
            model_key: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Execute a query against a data contract dictionary and return structured results.

        Args:
            contract: Contract dictionary
            query: SQL query to execute
            server_key: Optional key of the server to use
            model_key: Optional key of the model to use

        Returns:
            Dictionary with query results and metadata

        Raises:
            AssetQueryError: If query execution fails
        """
        with handle_asset_errors("executing contract query", contract.get("id", "unknown")):
            # Get servers and models
            servers = contract.get("servers", {})
            models = contract.get("models", {})

            # Use defaults if not specified
            server_key = server_key or next(iter(servers), None)
            model_key = model_key or next(iter(models), None)

            # Determine server to use
            if not server_key:
                raise AssetQueryError("No servers defined in contract")

            if server_key not in servers:
                raise AssetQueryError(f"Server '{server_key}' not found in contract")

            server = servers[server_key]

            # Determine model to use
            if not model_key:
                raise AssetQueryError("No models defined in contract")

            if model_key not in models:
                raise AssetQueryError(f"Model '{model_key}' not found in contract")

            # Get server type
            server_type = server.get("type")
            if not server_type:
                raise AssetQueryError("Server missing 'type' field")

            # Ensure server_type is a valid string value
            if isinstance(server_type, str):
                server_type = server_type.lower()
                # Check if server_type corresponds to a known server type
                if not hasattr(ServerType, server_type.upper()):
                    logger.warning(f"Unknown server type '{server_type}', using as is")
            else:
                # Convert non-string type to string
                server_type = str(server_type)

            # Execute the query using the DataSourceRegistry
            records = DataSourceRegistry.execute_query(server_type, model_key, query, server)

            # Return structured result as dictionary
            return {
                "records": records,
                "query": query,
                "model_key": model_key,
                "server_key": server_key
            }

    @staticmethod
    def _query_from_data_product(port: Dict[str, Any], query: str, model_key: str) -> Dict[str, Any]:
        """
        Query a data product output port directly without requiring a data contract.

        Args:
            port: The output port dictionary from the data product
            query: SQL query to execute
            model_key: The name to use for the queried model

        Returns:
            Dictionary with query results

        Raises:
            AssetQueryError: If the query execution fails
        """
        # Get port ID for error messages
        port_id = port.get("id", "unknown") if isinstance(port, dict) else getattr(port, "id", "unknown")

        with handle_asset_errors("executing direct port query", port_id):
            # Create server configuration
            server_config = DataAssetManager._create_server_config_from_port(port)

            # Resolve server type
            server_type = DataAssetManager._resolve_server_type(port, server_config)

            # Add type to config - ensure it's a string
            server_config["type"] = server_type

            # For LOCAL type, ensure path is set (use location if path not available)
            if server_type == ServerType.LOCAL and "path" not in server_config and "location" in server_config:
                server_config["path"] = server_config["location"]

            # Default model key to port id if not specified
            effective_model_key = model_key or port_id

            # Execute the query using the DataSourceRegistry
            records = DataSourceRegistry.execute_query(server_type, effective_model_key, query, server_config)

            # Return result as dictionary
            return {
                "records": records,
                "query": query,
                "model_key": effective_model_key,
                "server_key": "default"
            }
