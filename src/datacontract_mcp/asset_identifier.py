"""Object-oriented asset identification system for data contracts and data products."""

import logging
import os
import yaml
from abc import ABC, abstractmethod
from typing import Dict, List, Type, ClassVar, Union
from pathlib import Path
from enum import Enum

logger = logging.getLogger("datacontract-mcp.asset_identifier")


class DataAssetType(str, Enum):
    """Types of data assets supported."""
    DATA_CONTRACT = "contract"
    DATA_PRODUCT = "product"


class AssetLoadError(Exception):
    """Error raised when loading an asset fails."""
    pass


class AssetIdentifier(ABC):
    """Base class for all asset identifiers."""

    # Registry of source-specific identifier subclasses
    registry: ClassVar[Dict[str, Type['AssetIdentifier']]] = {}

    def __init__(self, asset_id: str, asset_type: str):
        """
        Initialize an asset identifier.

        Args:
            asset_id: Unique identifier for the asset
            asset_type: Type of asset ("product" or "contract")
        """
        if asset_type not in ["product", "contract"]:
            raise ValueError(f"Invalid asset type: {asset_type}")

        self.asset_id = asset_id
        self.asset_type = asset_type

    @property
    @abstractmethod
    def source(self) -> str:
        """The source of the asset (e.g., 'local', 'datameshmanager')."""
        pass

    def is_product(self) -> bool:
        """Check if this identifier refers to a data product."""
        return self.asset_type == "product"

    def is_contract(self) -> bool:
        """Check if this identifier refers to a data contract."""
        return self.asset_type == "contract"

    @abstractmethod
    def load_content(self) -> str:
        """
        Load the content of this asset.

        Returns:
            Content as a string

        Raises:
            AssetLoadError: If loading fails
        """
        pass

    @classmethod
    def register(cls, source_name: str):
        """
        Decorator to register a source-specific identifier class.

        Args:
            source_name: Name of the source (e.g., 'local', 'datameshmanager')
        """
        def wrapper(subclass):
            cls.registry[source_name] = subclass
            return subclass
        return wrapper

    @classmethod
    def from_string(cls, identifier_str: str) -> 'AssetIdentifier':
        """
        Create an asset identifier from a string representation.

        Format: [source]:[type]/[id]

        Examples:
        - local:product/orders.dataproduct.yaml
        - local:contract/orders.datacontract.yaml
        - datameshmanager:product/123
        - datameshmanager:contract/abc-456

        Args:
            identifier_str: String representation of the identifier

        Returns:
            Appropriate AssetIdentifier subclass instance

        Raises:
            ValueError: If the format is invalid or source is unknown
        """
        try:
            # Split the string into source, type, and id parts
            if ":" not in identifier_str:
                raise ValueError(f"Invalid identifier format: {identifier_str}")

            source, rest = identifier_str.split(":", 1)

            if "/" not in rest:
                raise ValueError(f"Invalid identifier format: {identifier_str}")

            asset_type, asset_id = rest.split("/", 1)

            # Validate components
            if not source:
                raise ValueError(f"Missing source in identifier: {identifier_str}")
            if not asset_type:
                raise ValueError(f"Missing asset type in identifier: {identifier_str}")
            if not asset_id:
                raise ValueError(f"Missing asset ID in identifier: {identifier_str}")

            # Create appropriate identifier instance based on source
            if source not in cls.registry:
                raise ValueError(f"Unknown asset source: {source}")

            identifier_class = cls.registry[source]
            return identifier_class.from_parts(asset_type, asset_id)

        except Exception as e:
            if not isinstance(e, ValueError):
                raise ValueError(f"Error parsing identifier '{identifier_str}': {str(e)}")
            raise

    @classmethod
    @abstractmethod
    def from_parts(cls, asset_type: str, asset_id: str) -> 'AssetIdentifier':
        """
        Create an asset identifier from its component parts.

        Args:
            asset_type: Type of asset ("product" or "contract")
            asset_id: Unique identifier for the asset

        Returns:
            AssetIdentifier instance
        """
        pass

    @classmethod
    def create_identifier(cls, source: str, asset_type: str, asset_id: str) -> 'AssetIdentifier':
        """
        Factory method to create an appropriate asset identifier.

        Args:
            source: Source of the asset (e.g., 'local', 'datameshmanager')
            asset_type: Type of asset ("product" or "contract")
            asset_id: Unique identifier for the asset

        Returns:
            AssetIdentifier instance

        Raises:
            ValueError: If source is unknown
        """
        if source not in cls.registry:
            raise ValueError(f"Unknown asset source: {source}")

        identifier_class = cls.registry[source]
        return identifier_class.from_parts(asset_type, asset_id)

    @classmethod
    def list_assets(cls, asset_type: Union[str, DataAssetType]) -> List['AssetIdentifier']:
        """
        List all available assets of a specific type across all sources.

        Args:
            asset_type: Type of asset (DataAssetType or str: "product" or "contract")

        Returns:
            List of AssetIdentifier objects
        """
        # Convert DataAssetType to string if needed
        if isinstance(asset_type, DataAssetType):
            asset_type = asset_type.value

        identifiers = []

        # Ask each registered source to list its assets
        for source_name, identifier_class in cls.registry.items():
            try:
                # Check if the class has a list_assets method
                if hasattr(identifier_class, 'list_assets') and callable(getattr(identifier_class, 'list_assets')):
                    source_identifiers = identifier_class.list_assets(asset_type)
                    identifiers.extend(source_identifiers)
            except Exception as e:
                logger.warning(f"Error listing assets from source {source_name}: {str(e)}")

        return identifiers

    def __str__(self) -> str:
        """
        Convert to string representation.

        Returns:
            String in the format [source]:[type]/[id]
        """
        return f"{self.source}:{self.asset_type}/{self.asset_id}"

    def __eq__(self, other):
        """Check equality with another identifier."""
        if not isinstance(other, AssetIdentifier):
            return False
        return (
            self.source == other.source and
            self.asset_type == other.asset_type and
            self.asset_id == other.asset_id
        )

    def __hash__(self):
        """Hash based on source, type, and id."""
        return hash((self.source, self.asset_type, self.asset_id))


@AssetIdentifier.register("local")
class LocalAssetIdentifier(AssetIdentifier):
    """Identifier for local file assets."""

    # Class-level variable for the assets directory
    assets_dir = os.getenv("DATAASSET_SOURCE", "")

    @property
    def source(self) -> str:
        return "local"

    @property
    def filename(self) -> str:
        """Get the local filename for this asset."""
        return self.asset_id

    def load_content(self) -> str:
        """
        Load the content of this local asset.
        
        For data products, this method adds source prefixes to dataContractId fields
        to ensure consistent identifier resolution.

        Returns:
            Content as a string, with dataContractId fields properly prefixed

        Raises:
            AssetLoadError: If loading fails
        """
        if not LocalAssetIdentifier.assets_dir:
            logger.info("DATAASSET_SOURCE environment variable not set, local resources unavailable")
            raise AssetLoadError("Local resources unavailable - DATAASSET_SOURCE not set")

        resource_path = Path(f"{LocalAssetIdentifier.assets_dir}/{self.filename}")

        if not resource_path.exists():
            raise AssetLoadError(f"Asset file not found at {resource_path}")

        try:
            with open(resource_path, "r", encoding="utf-8") as f:
                content = f.read()
                
            # If this is a product, process dataContractId fields to add source prefix
            if self.is_product():
                try:
                    data = yaml.safe_load(content)
                    if data and "outputPorts" in data:
                        modified = False
                        for port in data.get("outputPorts", []):
                            if "dataContractId" in port and port["dataContractId"]:
                                contract_id = port["dataContractId"]
                                # Only add prefix if it doesn't already have one
                                if ":" not in contract_id:
                                    logger.info(f"Adding source prefix to local dataContractId: {contract_id} -> {self.source}:contract/{contract_id}")
                                    port["dataContractId"] = f"{self.source}:contract/{contract_id}"
                                    modified = True
                                else:
                                    logger.info(f"Local dataContractId already has prefix: {contract_id}")
                        
                        # If modifications were made, convert back to YAML
                        if modified:
                            content = yaml.dump(data)
                except Exception as e:
                    # If YAML processing fails, just return the original content
                    logger.warning(f"Error processing dataContractId in {self.filename}: {str(e)}")
                    
            return content
        except Exception as e:
            raise AssetLoadError(f"Error reading local asset file {self.filename}: {str(e)}")

    @classmethod
    def from_filename(cls, filename: str) -> 'LocalAssetIdentifier':
        """
        Create a local asset identifier from a filename.

        Args:
            filename: Local filename (e.g., 'orders.dataproduct.yaml')

        Returns:
            LocalAssetIdentifier instance

        Raises:
            ValueError: If filename format is invalid
        """
        if filename.endswith('.dataproduct.yaml'):
            return cls(asset_id=filename, asset_type="product")
        elif filename.endswith('.datacontract.yaml'):
            return cls(asset_id=filename, asset_type="contract")
        else:
            raise ValueError(f"Invalid filename format: {filename}")

    @classmethod
    def from_parts(cls, asset_type: str, asset_id: str) -> 'LocalAssetIdentifier':
        """
        Create a local asset identifier from its component parts.

        Args:
            asset_type: Type of asset ("product" or "contract")
            asset_id: Local filename

        Returns:
            LocalAssetIdentifier instance
        """
        return cls(asset_id=asset_id, asset_type=asset_type)

    @classmethod
    def list_assets(cls, asset_type: str) -> List['LocalAssetIdentifier']:
        """
        List all available local assets of a specific type.

        Args:
            asset_type: Type of asset ("product" or "contract")

        Returns:
            List of LocalAssetIdentifier objects
        """
        if not cls.assets_dir:
            logger.info("DATAASSET_SOURCE environment variable not set, skipping local resources")
            return []

        extension = '.dataproduct.yaml' if asset_type == "product" else '.datacontract.yaml'
        identifiers = []

        if os.path.exists(cls.assets_dir):
            for fname in os.listdir(cls.assets_dir):
                file_path = os.path.join(cls.assets_dir, fname)
                if os.path.isfile(file_path) and fname.lower().endswith(extension):
                    try:
                        identifiers.append(cls(asset_id=fname, asset_type=asset_type))
                    except ValueError:
                        logger.warning(f"Skipping file with invalid name format: {fname}")
        else:
            logger.warning(f"Assets directory {cls.assets_dir} does not exist")

        return identifiers


@AssetIdentifier.register("datameshmanager")
class DataMeshManagerAssetIdentifier(AssetIdentifier):
    """Identifier for DataMeshManager assets."""

    # Class-level variable for API token and URL
    api_token = os.getenv("DATAMESH_MANAGER_API_KEY")
    api_url = os.getenv("DATAMESH_MANAGER_HOST", "https://api.datamesh-manager.com")

    # Class-level cache for DataMeshManager assets
    cache = {
        "product": {},
        "contract": {}
    }
    
    # Flag to track if the cache needs to be refreshed
    _cache_initialized = False

    @property
    def source(self) -> str:
        return "datameshmanager"

    def load_content(self) -> str:
        """
        Load the content of this DataMeshManager asset.
        
        For data products, this method adds source prefixes to dataContractId fields
        to ensure consistent identifier resolution.

        Returns:
            Content as a string, with dataContractId fields properly prefixed

        Raises:
            AssetLoadError: If loading fails
        """
        if not self.api_token:
            raise AssetLoadError("DATAMESH_MANAGER_API_KEY environment variable not set")
            
        # Clear the cache if this is the first access in a session
        if not DataMeshManagerAssetIdentifier._cache_initialized:
            logger.info("Initializing DataMeshManager cache")
            DataMeshManagerAssetIdentifier.cache = {
                "product": {},
                "contract": {}
            }
            DataMeshManagerAssetIdentifier._cache_initialized = True

        # Check cache first
        cache_key = str(self)
        if self.asset_type in self.cache and cache_key in self.cache[self.asset_type]:
            logger.info(f"Using cached content for {cache_key}")
            cached_data = self.cache[self.asset_type][cache_key]
            
            # Even for cached content, ensure dataContractId fields have source prefix
            if self.is_product() and "outputPorts" in cached_data:
                modified = False
                for port in cached_data.get("outputPorts", []):
                    if "dataContractId" in port and port["dataContractId"]:
                        contract_id = port["dataContractId"]
                        # Only add prefix if it doesn't already have one
                        if ":" not in contract_id:
                            logger.info(f"Adding source prefix to cached dataContractId: {contract_id} -> {self.source}:contract/{contract_id}")
                            port["dataContractId"] = f"{self.source}:contract/{contract_id}"
                            modified = True
                
                # Update cache if modified
                if modified:
                    self.cache[self.asset_type][cache_key] = cached_data
            
            # Extra debug info for data products
            if self.is_product() and "outputPorts" in cached_data:
                for port in cached_data.get("outputPorts", []):
                    if "dataContractId" in port and port["dataContractId"]:
                        logger.info(f"Final cached dataContractId value: {port['dataContractId']}")
            
            # Return cached content as YAML
            return yaml.dump(self.cache[self.asset_type][cache_key])

        # Not in cache, fetch from API
        try:
            from .datameshmanager import DataMeshManager
            dmm = DataMeshManager(base_url=self.api_url, api_key=self.api_token)

            if self.is_product():
                data = dmm.get_data_product(self.asset_id)
            elif self.is_contract():
                data = dmm.get_data_contract(self.asset_id)
            else:
                raise AssetLoadError(f"Unsupported asset type: {self.asset_type}")

            # If this is a product, process dataContractId fields to add source prefix
            if self.is_product() and "outputPorts" in data:
                for port in data.get("outputPorts", []):
                    if "dataContractId" in port and port["dataContractId"]:
                        contract_id = port["dataContractId"]
                        # Only add prefix if it doesn't already have one
                        if ":" not in contract_id:
                            # Add logging to trace execution
                            logger.info(f"Adding source prefix to dataContractId: {contract_id} -> {self.source}:contract/{contract_id}")
                            port["dataContractId"] = f"{self.source}:contract/{contract_id}"
                        else:
                            logger.info(f"dataContractId already has prefix: {contract_id}")
            
            # Cache the result
            self.cache[self.asset_type][cache_key] = data

            # Extra debug info for data products
            if self.is_product() and "outputPorts" in data:
                for port in data.get("outputPorts", []):
                    if "dataContractId" in port and port["dataContractId"]:
                        logger.info(f"Final dataContractId value: {port['dataContractId']}")
            
            # Return as YAML
            return yaml.dump(data)
        except ImportError as e:
            raise AssetLoadError(f"Failed to import DataMeshManager: {str(e)}")
        except Exception as e:
            raise AssetLoadError(f"Error loading asset from DataMeshManager: {str(e)}")

    @classmethod
    def for_product(cls, product_id: str) -> 'DataMeshManagerAssetIdentifier':
        """
        Create an identifier for a DataMeshManager product.

        Args:
            product_id: DataMeshManager product ID

        Returns:
            DataMeshManagerAssetIdentifier instance
        """
        return cls(asset_id=product_id, asset_type="product")

    @classmethod
    def for_contract(cls, contract_id: str) -> 'DataMeshManagerAssetIdentifier':
        """
        Create an identifier for a DataMeshManager contract.

        Args:
            contract_id: DataMeshManager contract ID

        Returns:
            DataMeshManagerAssetIdentifier instance
        """
        return cls(asset_id=contract_id, asset_type="contract")

    @classmethod
    def from_parts(cls, asset_type: str, asset_id: str) -> 'DataMeshManagerAssetIdentifier':
        """
        Create a DataMeshManager asset identifier from its component parts.

        Args:
            asset_type: Type of asset ("product" or "contract")
            asset_id: DataMeshManager ID

        Returns:
            DataMeshManagerAssetIdentifier instance
        """
        return cls(asset_id=asset_id, asset_type=asset_type)

    @classmethod
    def list_assets(cls, asset_type: str) -> List['DataMeshManagerAssetIdentifier']:
        """
        List all available DataMeshManager assets of a specific type.

        Args:
            asset_type: Type of asset ("product" or "contract")

        Returns:
            List of DataMeshManagerAssetIdentifier objects
        """
        if not cls.api_token:
            logger.info("DATAMESH_MANAGER_API_KEY environment variable not set")
            return []

        identifiers = []

        try:
            from .datameshmanager import DataMeshManager
            dmm = DataMeshManager(base_url=cls.api_url, api_key=cls.api_token)

            if asset_type == "product":
                products = dmm.list_data_products()
                # Handle different response formats
                items = products.get('items', []) if isinstance(products, dict) else products
                for product in items:
                    if not isinstance(product, dict):
                        continue
                    product_id = product.get('id')
                    if product_id:
                        identifier = cls(asset_id=product_id, asset_type=asset_type)
                        identifiers.append(identifier)
                        # Update cache
                        cls.cache["product"][str(identifier)] = product

            elif asset_type == "contract":
                contracts = dmm.list_data_contracts()
                # Handle different response formats
                items = contracts.get('items', []) if isinstance(contracts, dict) else contracts
                for contract in items:
                    if not isinstance(contract, dict):
                        continue
                    contract_id = contract.get('id')
                    if contract_id:
                        identifier = cls(asset_id=contract_id, asset_type=asset_type)
                        identifiers.append(identifier)
                        # Update cache
                        cls.cache["contract"][str(identifier)] = contract

        except ImportError:
            logger.warning("DataMeshManager module not available")
        except Exception as e:
            logger.warning(f"Error listing assets from DataMeshManager: {str(e)}")

        return identifiers
