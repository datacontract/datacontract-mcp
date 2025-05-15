"""DataMeshManager asset source plugin for data products and contracts."""

import logging
import os
import yaml
import time
from typing import Dict, List, Any, Optional, ClassVar

from ...types import DataAssetType
from ...asset_identifier import AssetIdentifier
from ...asset_manager import AssetLoadError
from ..asset_source import AssetSourcePlugin
from .datameshmanager_client import DataMeshManager

logger = logging.getLogger("dataproduct-mcp.sources.asset_plugins.datameshmanager")

class DataMeshManagerAssetIdentifier(AssetIdentifier):
    """Asset identifier for Data Mesh Manager API sources."""

    def __init__(self, asset_id: str, asset_type: str):
        """
        Initialize a Data Mesh Manager asset identifier.

        Args:
            asset_id: ID of the asset in the Data Mesh Manager
            asset_type: Type of asset ("product" or "contract")
        """
        super().__init__(asset_id=asset_id, asset_type=asset_type, source_name="datameshmanager")


@AssetSourcePlugin.register
class DataMeshManagerSource(AssetSourcePlugin):
    """Plugin for accessing data assets from the Data Mesh Manager API."""

    # Class-level cache for DataMeshManager assets
    _cache: ClassVar[Dict[str, Dict[str, Any]]] = {
        "product": {},
        "contract": {}
    }

    # Cache expiration (in seconds)
    _cache_expiry: ClassVar[Dict[str, float]] = {}

    # Default cache TTL (5 minutes)
    _default_cache_ttl = 300

    def __init__(self):
        """Initialize the DataMeshManager source plugin."""
        self._api_token = os.getenv("DATAMESH_MANAGER_API_KEY")
        self._api_url = os.getenv("DATAMESH_MANAGER_HOST", "https://api.datamesh-manager.com")
        self._cache_ttl = int(os.getenv("DATAMESH_MANAGER_CACHE_TTL", str(self._default_cache_ttl)))

    @property
    def source_name(self) -> str:
        """The unique name of this source."""
        return "datameshmanager"

    def get_identifier(self, asset_type: DataAssetType, asset_id: str) -> AssetIdentifier:
        """Create an identifier for this source.

        Args:
            asset_type: Type of asset (product or contract)
            asset_id: DataMeshManager asset ID

        Returns:
            DataMeshManagerAssetIdentifier instance
        """
        return DataMeshManagerAssetIdentifier(asset_id=asset_id, asset_type=asset_type.value)

    def list_assets(self, asset_type: DataAssetType) -> List[AssetIdentifier]:
        """List all available DataMeshManager assets of a specific type.

        Args:
            asset_type: Type of asset (product or contract)

        Returns:
            List of DataMeshManagerAssetIdentifier objects
        """
        if not self.is_available():
            logger.info("DataMeshManager API key not set, skipping DataMeshManager resources")
            return []

        identifiers = []

        try:
            dmm = DataMeshManager(base_url=self._api_url, api_key=self._api_token)

            asset_type_str = asset_type.value

            if asset_type == DataAssetType.DATA_PRODUCT:
                # Get data products from the API
                products = dmm.list_data_products()
                # Handle different response formats
                items = products.get('items', []) if isinstance(products, dict) else products
                for product in items:
                    if not isinstance(product, dict):
                        continue
                    product_id = product.get('id')
                    if product_id:
                        identifier = self.get_identifier(asset_type, product_id)
                        identifiers.append(identifier)
                        # Update cache
                        self._update_cache(asset_type_str, str(identifier), product)

            elif asset_type == DataAssetType.DATA_CONTRACT:
                # Get data contracts from the API
                contracts = dmm.list_data_contracts()
                # Handle different response formats
                items = contracts.get('items', []) if isinstance(contracts, dict) else contracts
                for contract in items:
                    if not isinstance(contract, dict):
                        continue
                    contract_id = contract.get('id')
                    if contract_id:
                        identifier = self.get_identifier(asset_type, contract_id)
                        identifiers.append(identifier)
                        # Update cache
                        self._update_cache(asset_type_str, str(identifier), contract)

        except ImportError:
            logger.warning("DataMeshManager module not available")
        except Exception as e:
            logger.warning(f"Error listing assets from DataMeshManager: {str(e)}")

        return identifiers

    def load_asset_content(self, identifier: AssetIdentifier) -> str:
        """Load the content of a DataMeshManager asset.

        For data products, this method adds source prefixes to dataContractId fields
        to ensure consistent identifier resolution.

        Args:
            identifier: AssetIdentifier for the asset to load

        Returns:
            Asset content as a string, with dataContractId fields properly prefixed

        Raises:
            AssetLoadError: If the asset cannot be loaded
        """
        if not isinstance(identifier, DataMeshManagerAssetIdentifier):
            raise AssetLoadError(f"Invalid identifier type for DataMeshManager source: {type(identifier)}")

        if not self.is_available():
            raise AssetLoadError("DataMeshManager API key not set")

        # Check cache first
        asset_type = identifier.asset_type
        cache_key = str(identifier)

        cached_data = self._get_from_cache(asset_type, cache_key)
        if cached_data:
            # Even for cached content, ensure dataContractId fields have source prefix
            if identifier.is_product() and "outputPorts" in cached_data:
                modified = False
                for port in cached_data.get("outputPorts", []):
                    if "dataContractId" in port and port["dataContractId"]:
                        contract_id = port["dataContractId"]
                        # Only add prefix if it doesn't already have one
                        if ":" not in contract_id:
                            logger.info(f"Adding source prefix to cached dataContractId: {contract_id} -> {self.source_name}:contract/{contract_id}")
                            port["dataContractId"] = f"{self.source_name}:contract/{contract_id}"
                            modified = True

                # Update cache if modified
                if modified:
                    self._update_cache(asset_type, cache_key, cached_data)

            # Return cached content as YAML
            return yaml.dump(cached_data)

        # Not in cache, fetch from API
        try:
            from .datameshmanager_client import DataMeshManager
            dmm = DataMeshManager(base_url=self._api_url, api_key=self._api_token)

            if identifier.is_product():
                data = dmm.get_data_product(identifier.asset_id)
            elif identifier.is_contract():
                data = dmm.get_data_contract(identifier.asset_id)
            else:
                raise AssetLoadError(f"Unsupported asset type: {identifier.asset_type}")

            # If this is a product, process dataContractId fields to add source prefix
            if identifier.is_product():
                # Handle different structures - ensure outputPorts exists
                if "outputPorts" not in data and isinstance(data, dict):
                    # Try to detect if this is a data product without the expected structure
                    if "id" in data and "info" in data:
                        # Initialize empty outputPorts if it doesn't exist
                        data["outputPorts"] = data.get("outputPorts", [])

                # Now process dataContractId fields
                for port in data.get("outputPorts", []):
                    if "dataContractId" in port and port["dataContractId"]:
                        contract_id = port["dataContractId"]
                        # Only add prefix if it doesn't already have one
                        if ":" not in contract_id:
                            logger.info(f"Adding source prefix to dataContractId: {contract_id} -> {self.source_name}:contract/{contract_id}")
                            port["dataContractId"] = f"{self.source_name}:contract/{contract_id}"
                        else:
                            logger.info(f"dataContractId already has prefix: {contract_id}")

            # Cache the result
            self._update_cache(asset_type, cache_key, data)

            # Return as YAML
            return yaml.dump(data)
        except ImportError as e:
            raise AssetLoadError(f"Failed to import DataMeshManager: {str(e)}")
        except Exception as e:
            raise AssetLoadError(f"Error loading asset from DataMeshManager: {str(e)}")

    def is_available(self) -> bool:
        """Check if DataMeshManager API is available.

        Returns:
            True if API key is set, False otherwise
        """
        return bool(self._api_token)

    def get_configuration(self) -> Dict[str, Any]:
        """Get the current configuration for DataMeshManager.

        Returns:
            Dictionary with configuration values
        """
        return {
            "api_url": self._api_url,
            "api_token_set": bool(self._api_token),
            "cache_ttl": self._cache_ttl,
            "available": self.is_available()
        }

    def configure(self, config: Dict[str, Any]) -> None:
        """Configure the DataMeshManager source.

        Args:
            config: Dictionary with configuration values

        Supported configuration:
        - api_url: URL of the DataMeshManager API
        - api_token: API token for authentication
        - cache_ttl: Cache time-to-live in seconds
        """
        if "api_url" in config:
            self._api_url = config["api_url"]
            logger.info(f"Updated DataMeshManager API URL: {self._api_url}")

        if "api_token" in config:
            self._api_token = config["api_token"]
            logger.info("Updated DataMeshManager API token")

        if "cache_ttl" in config:
            self._cache_ttl = int(config["cache_ttl"])
            logger.info(f"Updated DataMeshManager cache TTL: {self._cache_ttl} seconds")

    def _update_cache(self, asset_type: str, key: str, data: Dict[str, Any]) -> None:
        """Add or update data in the cache.

        Args:
            asset_type: Type of asset ("product" or "contract")
            key: Cache key
            data: Data to cache
        """
        self._cache.setdefault(asset_type, {})
        self._cache[asset_type][key] = data
        self._cache_expiry[key] = time.time() + self._cache_ttl
        logger.debug(f"Cached {asset_type} data for {key}")

    def _get_from_cache(self, asset_type: str, key: str) -> Optional[Dict[str, Any]]:
        """Get data from the cache if not expired.

        Args:
            asset_type: Type of asset ("product" or "contract")
            key: Cache key

        Returns:
            Cached data if valid, None otherwise
        """
        if asset_type not in self._cache or key not in self._cache[asset_type]:
            return None

        if key not in self._cache_expiry or self._cache_expiry[key] < time.time():
            # Expired or no expiry set
            logger.debug(f"Cache expired for {key}")
            return None

        logger.debug(f"Using cached data for {key}")
        return self._cache[asset_type][key]
