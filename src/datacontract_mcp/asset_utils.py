"""Common utilities for data contracts and data products."""

import logging
import os
import yaml
from typing import List, Type, TypeVar, Dict, Any, Optional

from .asset_identifier import (
    AssetIdentifier,
    LocalAssetIdentifier,
    DataMeshManagerAssetIdentifier
)

# Type variable for Pydantic models
T = TypeVar('T')

logger = logging.getLogger("datacontract-mcp.asset_utils")

# Common asset source directory (optional)
ASSETS_SOURCE = os.getenv("DATAASSET_SOURCE", "")
USE_LOCAL_ASSETS = ASSETS_SOURCE != ""

# DataMeshManager API integration (optional)
DATAMESH_MANAGER_API_KEY = os.getenv("DATAMESH_MANAGER_API_KEY")
DATAMESH_MANAGER_HOST = os.getenv("DATAMESH_MANAGER_HOST", "https://api.datamesh-manager.com")
USE_DATAMESHMANAGER = DATAMESH_MANAGER_API_KEY is not None and DATAMESH_MANAGER_HOST is not None

# Cache to store DataMeshManager metadata (to reduce API calls)
DATAMESHMANAGER_CACHE: Dict[str, Any] = {
    "data_products": {},
    "data_contracts": {}
}


class AssetLoadError(Exception):
    """Error raised when loading an asset file fails."""
    pass


class AssetParseError(Exception):
    """Error raised when parsing an asset file fails."""
    pass


class AssetQueryError(Exception):
    """Error raised when querying an asset fails."""
    pass


def parse_yaml(content: str) -> Dict[str, Any]:
    """
    Parse a YAML string into a dictionary.

    Args:
        content: YAML content

    Returns:
        Dictionary representation of the YAML content

    Raises:
        AssetParseError: If YAML is invalid
    """
    try:
        # Parse with PyYAML to get the raw dictionary
        asset_dict = yaml.safe_load(content)
        
        if not isinstance(asset_dict, dict):
            raise AssetParseError("YAML content does not represent a dictionary")
            
        # Make sure we have at least basic required fields
        if "id" not in asset_dict:
            logger.warning("Missing 'id' field in asset")
            # Use a default ID based on content hash if missing
            import hashlib
            asset_dict["id"] = f"default_{hashlib.md5(content.encode()).hexdigest()[:8]}"
            
        # Ensure we have an info section with at least a title
        if "info" not in asset_dict or not isinstance(asset_dict["info"], dict):
            logger.warning("Missing or invalid 'info' section in asset, creating default")
            asset_dict["info"] = asset_dict.get("info", {})
            if not isinstance(asset_dict["info"], dict):
                asset_dict["info"] = {}
                
        if "title" not in asset_dict["info"]:
            asset_dict["info"]["title"] = asset_dict.get("id", "Untitled")
        
        return asset_dict

    except yaml.YAMLError as e:
        raise AssetParseError(f"Error parsing YAML: {str(e)}")
    except AssetParseError:
        raise
    except Exception as e:
        logger.warning(f"Error in parse_yaml: {str(e)}")
        # Try to return a basic dict that won't break functionality
        return {"id": "default", "info": {"title": "Default"}}


def get_datameshmanager():
    """
    Get a configured DataMeshManager client instance.

    Returns:
        DataMeshManager instance

    Raises:
        ImportError: If datameshmanager.py is not available
        ValueError: If API token is not available
    """
    if not USE_DATAMESHMANAGER:
        raise ValueError("DataMeshManager integration not enabled. Set DATAMESH_MANAGER_HOST and DATAMESH_MANAGER_API_KEY.")

    try:
        from .datameshmanager import DataMeshManager
        return DataMeshManager(
            base_url=DATAMESH_MANAGER_HOST,
            api_key=DATAMESH_MANAGER_API_KEY
        )
    except ImportError as e:
        logger.error(f"Failed to import DataMeshManager: {str(e)}")
        raise


def add_to_datamesh_cache(identifier: AssetIdentifier, data: Dict[str, Any]) -> None:
    """
    Add data to the DataMeshManager cache.

    Args:
        identifier: Asset identifier
        data: Data to cache
    """
    if not isinstance(identifier, DataMeshManagerAssetIdentifier):
        return

    cache_key = str(identifier)

    if identifier.is_product():
        DATAMESHMANAGER_CACHE["data_products"][cache_key] = data
    elif identifier.is_contract():
        DATAMESHMANAGER_CACHE["data_contracts"][cache_key] = data


def get_from_datamesh_cache(identifier: AssetIdentifier) -> Optional[Dict[str, Any]]:
    """
    Get data from the DataMeshManager cache.

    Args:
        identifier: Asset identifier

    Returns:
        Cached data or None if not found
    """
    if not isinstance(identifier, DataMeshManagerAssetIdentifier):
        return None

    cache_key = str(identifier)

    if identifier.is_product():
        return DATAMESHMANAGER_CACHE["data_products"].get(cache_key)
    elif identifier.is_contract():
        return DATAMESHMANAGER_CACHE["data_contracts"].get(cache_key)

    return None


def sanitize_id_for_filename(identifier: str) -> str:
    """
    Sanitize an identifier for use in a filename.

    Args:
        identifier: Identifier to sanitize

    Returns:
        Sanitized identifier
    """
    return identifier.replace("/", "_").replace(":", "_")


def list_assets(asset_type: str) -> List[AssetIdentifier]:
    """
    List all available assets of a specific type.
    If DataMeshManager integration is enabled, this will include assets from both local files and the API.

    Args:
        asset_type: Type of asset ("product" or "contract")

    Returns:
        List of AssetIdentifier objects
    """
    if asset_type not in ["product", "contract"]:
        raise ValueError(f"Invalid asset type: {asset_type}")

    extension = '.dataproduct.yaml' if asset_type == "product" else '.datacontract.yaml'
    identifiers = []

    # Check local files if enabled
    if USE_LOCAL_ASSETS:
        if os.path.exists(ASSETS_SOURCE):
            for fname in os.listdir(ASSETS_SOURCE):
                file_path = os.path.join(ASSETS_SOURCE, fname)
                if os.path.isfile(file_path) and fname.lower().endswith(extension):
                    try:
                        # Create a local asset identifier
                        identifier = LocalAssetIdentifier.from_filename(fname)
                        identifiers.append(identifier)
                    except ValueError:
                        # Skip files with invalid naming
                        logger.warning(f"Skipping file with invalid name format: {fname}")
        else:
            logger.warning(f"Assets directory {ASSETS_SOURCE} does not exist")
    else:
        logger.info("Local assets disabled (DATAASSET_SOURCE not set)")

    # If DataMeshManager integration is enabled, add assets from the API
    if USE_DATAMESHMANAGER:
        try:
            dmm = get_datameshmanager()

            if asset_type == "product":
                # Get data products from the API
                products = dmm.list_data_products()
                for product in products.get('items', []):
                    product_id = product.get('id')
                    if product_id:
                        identifier = DataMeshManagerAssetIdentifier.for_product(product_id)
                        identifiers.append(identifier)
                        # Update cache
                        add_to_datamesh_cache(identifier, product)

            elif asset_type == "contract":
                # Get data contracts from the API
                contracts = dmm.list_data_contracts()
                for contract in contracts.get('items', []):
                    contract_id = contract.get('id')
                    if contract_id:
                        identifier = DataMeshManagerAssetIdentifier.for_contract(contract_id)
                        identifiers.append(identifier)
                        # Update cache
                        add_to_datamesh_cache(identifier, contract)

        except Exception as e:
            logger.error(f"Error fetching assets from DataMeshManager: {str(e)}")
            # Continue with local files only

    return identifiers
