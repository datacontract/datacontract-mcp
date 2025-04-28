"""Unified manager for data contracts and data products."""

import logging
from enum import Enum
from typing import Dict, List, Optional, Any, Union

from .asset_utils import (
    AssetLoadError,
    AssetParseError,
    AssetQueryError
)

from . import datacontract
from . import dataproduct
from .resources import docs

logger = logging.getLogger("datacontract-mcp.asset_manager")

class DataAssetType(str, Enum):
    """Types of data assets supported."""
    DATA_CONTRACT = "contract"
    DATA_PRODUCT = "product"

class DataAssetManager:
    """Manager for unified access to data contracts and data products."""

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
    def list_assets(asset_type: DataAssetType) -> List[str]:
        """
        List all available assets of a specific type.

        Args:
            asset_type: Type of asset (contract or product)

        Returns:
            List of filenames
        """
        if asset_type == DataAssetType.DATA_CONTRACT:
            return datacontract.list_contract_files()
        elif asset_type == DataAssetType.DATA_PRODUCT:
            return dataproduct.list_product_files()
        else:
            raise ValueError(f"Unsupported asset type: {asset_type}")

    @staticmethod
    def get_asset_content(asset_type: DataAssetType, filename: str) -> str:
        """
        Get the raw content of an asset file.

        Args:
            asset_type: Type of asset (contract or product)
            filename: Name of the asset file

        Returns:
            File contents as string
        """
        if asset_type == DataAssetType.DATA_CONTRACT:
            return datacontract.load_contract_file(filename)
        elif asset_type == DataAssetType.DATA_PRODUCT:
            return dataproduct.load_product_file(filename)
        else:
            raise ValueError(f"Unsupported asset type: {asset_type}")

    @staticmethod
    def validate_asset(asset_type: DataAssetType, filename: str) -> Dict[str, Any]:
        """
        Validate an asset and return its structured representation.

        Args:
            asset_type: Type of asset (contract or product)
            filename: Name of the asset file

        Returns:
            Validated asset as a dictionary
        """
        if asset_type == DataAssetType.DATA_CONTRACT:
            contract = datacontract.get_contract(filename)
            return contract.model_dump()
        elif asset_type == DataAssetType.DATA_PRODUCT:
            product = dataproduct.get_product(filename)
            return product.model_dump()
        else:
            raise ValueError(f"Unsupported asset type: {asset_type}")

    # Product-specific methods
    @staticmethod
    def get_product_schema() -> str:
        """Get the Data Product JSON schema."""
        return DataAssetManager.get_schema(DataAssetType.DATA_PRODUCT)

    @staticmethod
    def get_product_example() -> str:
        """Get an example Data Product."""
        return DataAssetManager.get_example(DataAssetType.DATA_PRODUCT)

    @staticmethod
    def list_products() -> List[str]:
        """List all available data products."""
        return DataAssetManager.list_assets(DataAssetType.DATA_PRODUCT)

    @staticmethod
    def get_product(filename: str) -> Dict[str, Any]:
        """Get a validated data product."""
        return DataAssetManager.validate_asset(DataAssetType.DATA_PRODUCT, filename)

    @staticmethod
    def get_product_content(filename: str) -> str:
        """Get raw content of a data product file."""
        return DataAssetManager.get_asset_content(DataAssetType.DATA_PRODUCT, filename)

    # Contract-specific methods (public)
    @staticmethod
    def get_contract(filename: str) -> Dict[str, Any]:
        """Get a validated data contract."""
        return DataAssetManager.validate_asset(DataAssetType.DATA_CONTRACT, filename)

    # Contract query methods (public)
    @staticmethod
    def query_contract(
        filename: str,
        query: str,
        server_key: Optional[str] = None,
        model_key: Optional[str] = None,
        include_metadata: bool = False
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Execute a query against a data contract.

        Args:
            filename: Name of the data contract file
            query: SQL query to execute
            server_key: Optional key of the server to use
            model_key: Optional key of the model to use
            include_metadata: Whether to include metadata in the response

        Returns:
            Query results (with or without metadata)

        Raises:
            AssetLoadError: If the file is not found
            AssetParseError: If contract is invalid
            AssetQueryError: If query execution fails
        """
        return DataAssetManager._query_contract(
            filename=filename,
            query=query,
            server_key=server_key,
            model_key=model_key,
            include_metadata=include_metadata
        )

    # Contract-related methods (internal)
    @staticmethod
    def _query_contract(
        filename: str,
        query: str,
        server_key: Optional[str] = None,
        model_key: Optional[str] = None,
        include_metadata: bool = False
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Execute a query against a data contract.

        Args:
            filename: Name of the data contract file
            query: SQL query to execute
            server_key: Optional key of the server to use
            model_key: Optional key of the model to use
            include_metadata: Whether to include metadata in the response

        Returns:
            Query results (with or without metadata)

        Raises:
            AssetLoadError: If the file is not found
            AssetParseError: If contract is invalid
            AssetQueryError: If query execution fails
        """
        try:
            result = datacontract.query_contract(
                filename=filename,
                query=query,
                server_key=server_key,
                model_key=model_key
            )

            return result.model_dump() if include_metadata else result.records
        except (AssetLoadError, AssetParseError, AssetQueryError) as e:
            # Re-raise with more context about the specific operation
            logger.error(f"Error querying contract {filename}: {str(e)}")
            raise

    @staticmethod
    def _find_related_assets() -> List[Dict[str, Any]]:
        """
        Find relationships between data products and data contracts.

        Returns:
            List of dictionaries with relationship information
        """
        product_files = DataAssetManager.list_assets(DataAssetType.DATA_PRODUCT)
        contract_files = DataAssetManager.list_assets(DataAssetType.DATA_CONTRACT)

        # Map of contract IDs to contract details
        contract_map = {}
        for cf in contract_files:
            try:
                contract = datacontract.get_contract(cf)
                contract_map[contract.id] = {
                    "id": contract.id,
                    "title": contract.info.title,
                    "filename": cf
                }
            except Exception as e:
                logger.warning(f"Error loading data contract {cf}: {str(e)}")

        # Find relationships
        relationships = []
        for pf in product_files:
            try:
                product = dataproduct.get_product(pf)

                # Skip products without output ports
                if not product.outputPorts:
                    continue

                # Check each output port for a data contract reference
                for port in product.outputPorts:
                    if port.dataContractId and port.dataContractId in contract_map:
                        contract_info = contract_map[port.dataContractId]

                        relationships.append({
                            "product": {
                                "id": product.id,
                                "title": product.info.title,
                                "filename": pf
                            },
                            "output_port": {
                                "id": port.id,
                                "name": port.name
                            },
                            "contract": contract_info
                        })
            except Exception as e:
                logger.warning(f"Error loading data product {pf}: {str(e)}")

        return relationships

    # Product output port methods (public)
    @staticmethod
    def get_product_outputs(filename: str) -> Dict[str, Any]:
        """
        Get all output ports from a data product with linked contracts.

        Args:
            filename: Name of the data product file

        Returns:
            Dictionary with output port information and any linked contracts
        """
        # Get the product
        product_dict = DataAssetManager.validate_asset(DataAssetType.DATA_PRODUCT, filename)

        # Get relationship information
        relationships = DataAssetManager._find_related_assets()
        product_relationships = {
            rel["output_port"]["id"]: rel["contract"]
            for rel in relationships
            if rel["product"]["filename"] == filename
        }

        # Build enhanced output ports
        output_ports = []
        for port in product_dict.get("outputPorts", []):
            port_info = {
                "id": port["id"],
                "name": port.get("name", ""),
                "description": port.get("description", ""),
                "type": port.get("type", ""),
                "location": port.get("location", "")
            }

            # Add contract info if available
            if port["id"] in product_relationships:
                port_info["contract"] = product_relationships[port["id"]]

            output_ports.append(port_info)

        return {
            "product_id": product_dict["id"],
            "product_name": product_dict["info"]["title"],
            "output_ports": output_ports
        }

    @staticmethod
    def get_output_schema(filename: str, port_id: str) -> Dict[str, Any]:
        """
        Get the schema for a specific output port (using its linked data contract).

        Args:
            filename: Name of the data product file
            port_id: ID of the output port

        Returns:
            Schema information for the output port data
        """
        # Find the related contract for this output port
        relationships = DataAssetManager._find_related_assets()
        matching_rel = None

        for rel in relationships:
            if (rel["product"]["filename"] == filename and
                rel["output_port"]["id"] == port_id):
                matching_rel = rel
                break

        if not matching_rel:
            # Check if the port exists but has no contract
            product_dict = DataAssetManager.validate_asset(DataAssetType.DATA_PRODUCT, filename)
            for port in product_dict.get("outputPorts", []):
                if port["id"] == port_id:
                    return {
                        "output_port": port_id,
                        "error": "No data contract linked to this output port"
                    }

            return {
                "error": f"Output port '{port_id}' not found in data product '{filename}'"
            }

        # Get the contract details
        contract_filename = matching_rel["contract"]["filename"]
        contract_dict = DataAssetManager.validate_asset(DataAssetType.DATA_CONTRACT, contract_filename)

        # Extract schema information from contract
        schema = {
            "output_port": port_id,
            "contract_id": contract_dict["id"],
            "models": {}
        }

        # Include model information from the contract
        for model_name, model_data in contract_dict.get("models", {}).items():
            schema["models"][model_name] = {
                "type": model_data.get("type", ""),
                "description": model_data.get("description", ""),
                "fields": model_data.get("fields", {})
            }

        return schema

    @staticmethod
    def query_product(
        product_filename: str,
        query: str,
        port_id: Optional[str] = None,
        server_key: Optional[str] = None,
        model_key: Optional[str] = None,
        include_metadata: bool = False
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Query a data contract associated with a data product output port.

        Args:
            product_filename: Name of the data product file
            query: SQL query to execute
            port_id: Optional ID of the output port (uses first port if not specified)
            server_key: Optional key of the server to use in the contract
            model_key: Optional key of the model to use in the contract
            include_metadata: Whether to include metadata in the response

        Returns:
            Query results based on the associated data contract

        Raises:
            AssetLoadError: If files cannot be loaded
            AssetParseError: If the assets are invalid
            AssetQueryError: If the query execution fails or if required resources aren't found
        """
        try:
            # Get the data product
            product = dataproduct.get_product(product_filename)

            # Find the specified output port or use the first one
            port = None
            if port_id:
                for p in product.outputPorts:
                    if p.id == port_id:
                        port = p
                        break
                if not port:
                    raise AssetQueryError(f"Output port '{port_id}' not found in product {product_filename}")
            else:
                if not product.outputPorts:
                    raise AssetQueryError(f"Data product {product_filename} has no output ports")
                port = product.outputPorts[0]

            # Check if the port has a data contract reference
            if not port.dataContractId:
                raise AssetQueryError(f"Output port '{port.id}' doesn't reference a data contract")

            # Find the contract file by ID
            contract_filename = None
            for cf in datacontract.list_contract_files():
                try:
                    contract = datacontract.get_contract(cf)
                    if contract.id == port.dataContractId:
                        contract_filename = cf
                        break
                except (AssetLoadError, AssetParseError):
                    continue

            if not contract_filename:
                raise AssetQueryError(f"Couldn't find data contract with ID '{port.dataContractId}'")

            # Query the contract
            result = datacontract.query_contract(
                filename=contract_filename,
                query=query,
                server_key=server_key,
                model_key=model_key
            )

            # Format the response
            if include_metadata:
                return {
                    "product": {
                        "id": product.id,
                        "filename": product_filename,
                        "output_port": port.id
                    },
                    "contract": {
                        "id": port.dataContractId,
                        "filename": contract_filename
                    },
                    "query_result": result.model_dump()
                }
            else:
                return result.records

        except (AssetLoadError, AssetParseError) as e:
            # Re-raise preserving the original error
            logger.error(f"Error accessing assets for query: {str(e)}")
            raise
        except AssetQueryError as e:
            # Re-raise preserving the original error
            logger.error(f"Error executing query on product {product_filename}: {str(e)}")
            raise
        except Exception as e:
            # Convert unexpected errors to AssetQueryError
            error_msg = f"Unexpected error querying product {product_filename}: {str(e)}"
            logger.error(error_msg)
            raise AssetQueryError(error_msg) from e
