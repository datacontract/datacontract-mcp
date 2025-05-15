"""Local file asset source plugin for data products and contracts."""

import logging
import os
from pathlib import Path
from typing import Any, Dict, List

import yaml

from ...asset_identifier import AssetIdentifier
from ...asset_manager import AssetLoadError
from ...types import DataAssetType
from ..asset_source import AssetSourcePlugin

logger = logging.getLogger("dataproduct-mcp.sources.asset_plugins.local")


class LocalAssetIdentifier(AssetIdentifier):
    """Asset identifier for local file sources."""
    
    def __init__(self, asset_id: str, asset_type: str):
        """
        Initialize a local asset identifier.
        
        Args:
            asset_id: Filename of the asset
            asset_type: Type of asset ("product" or "contract")
        """
        super().__init__(asset_id=asset_id, asset_type=asset_type, source_name="local")


@AssetSourcePlugin.register
class LocalAssetSource(AssetSourcePlugin):
    """Plugin for accessing data assets from local files."""

    def __init__(self):
        """Initialize the local asset source."""
        self._assets_dir = os.getenv("DATAASSET_SOURCE", "")

    @property
    def source_name(self) -> str:
        """The unique name of this source."""
        return "local"

    def get_identifier(self, asset_type: DataAssetType, asset_id: str) -> AssetIdentifier:
        """Create an identifier for this source.

        Args:
            asset_type: Type of asset (product or contract)
            asset_id: Local filename

        Returns:
            LocalAssetIdentifier instance
        """
        return LocalAssetIdentifier(asset_id=asset_id, asset_type=asset_type.value)

    def list_assets(self, asset_type: DataAssetType) -> List[AssetIdentifier]:
        """List all available local assets of a specific type.

        Args:
            asset_type: Type of asset (product or contract)

        Returns:
            List of LocalAssetIdentifier objects
        """
        if not self.is_available():
            logger.info("DATAASSET_SOURCE environment variable not set, skipping local resources")
            return []

        extension = '.dataproduct.yaml' if asset_type == DataAssetType.DATA_PRODUCT else '.datacontract.yaml'
        identifiers = []

        if os.path.exists(self._assets_dir):
            for fname in os.listdir(self._assets_dir):
                file_path = os.path.join(self._assets_dir, fname)
                if os.path.isfile(file_path) and fname.lower().endswith(extension):
                    try:
                        identifiers.append(self.get_identifier(asset_type, fname))
                    except ValueError:
                        logger.warning(f"Skipping file with invalid name format: {fname}")
        else:
            logger.warning(f"Assets directory {self._assets_dir} does not exist")

        return identifiers

    def load_asset_content(self, identifier: AssetIdentifier) -> str:
        """Load the content of a local asset.

        For data products, this method adds source prefixes to dataContractId fields
        to ensure consistent identifier resolution.

        Args:
            identifier: AssetIdentifier for the asset to load

        Returns:
            Asset content as a string, with dataContractId fields properly prefixed

        Raises:
            AssetLoadError: If the asset cannot be loaded
        """
        if not isinstance(identifier, LocalAssetIdentifier):
            raise AssetLoadError(f"Invalid identifier type for local source: {type(identifier)}")

        if not self.is_available():
            logger.info("DATAASSET_SOURCE environment variable not set, local resources unavailable")
            raise AssetLoadError("Local resources unavailable - DATAASSET_SOURCE not set")

        filename = identifier.asset_id
        resource_path = Path(f"{self._assets_dir}/{filename}")

        if not resource_path.exists():
            raise AssetLoadError(f"Asset file not found at {resource_path}")

        try:
            with open(resource_path, "r", encoding="utf-8") as f:
                content = f.read()

            # If this is a product, process dataContractId fields to add source prefix
            if identifier.is_product():
                try:
                    data = yaml.safe_load(content)
                    if data:
                        # Handle different structures - ensure outputPorts exists
                        if "outputPorts" not in data and isinstance(data, dict):
                            # Try to detect if this is a data product without the expected structure
                            if "id" in data and "info" in data:
                                # Initialize empty outputPorts if it doesn't exist
                                data["outputPorts"] = data.get("outputPorts", [])

                        modified = False
                        for port in data.get("outputPorts", []):
                            if "dataContractId" in port and port["dataContractId"]:
                                contract_id = port["dataContractId"]
                                # Only add prefix if it doesn't already have one
                                if ":" not in contract_id:
                                    logger.info(f"Adding source prefix to local dataContractId: {contract_id} -> {self.source_name}:contract/{contract_id}")
                                    port["dataContractId"] = f"{self.source_name}:contract/{contract_id}"
                                    modified = True
                                else:
                                    logger.info(f"Local dataContractId already has prefix: {contract_id}")

                        # If modifications were made, convert back to YAML
                        if modified:
                            content = yaml.dump(data)
                except Exception as e:
                    # If YAML processing fails, just return the original content
                    logger.warning(f"Error processing dataContractId in {filename}: {str(e)}")

            return content
        except Exception as e:
            raise AssetLoadError(f"Error reading local asset file {filename}: {str(e)}")

    def is_available(self) -> bool:
        """Check if local assets are available.

        Returns:
            True if DATAASSET_SOURCE is set and the directory exists, False otherwise
        """
        if not self._assets_dir:
            return False

        return os.path.exists(self._assets_dir)

    def get_configuration(self) -> Dict[str, Any]:
        """Get the current configuration for local assets.

        Returns:
            Dictionary with configuration values
        """
        return {
            "assets_dir": self._assets_dir,
            "available": self.is_available()
        }

    def configure(self, config: Dict[str, Any]) -> None:
        """Configure the local source.

        Args:
            config: Dictionary with configuration values

        Supported configuration:
        - assets_dir: Directory containing data asset files
        """
        if "assets_dir" in config:
            self._assets_dir = config["assets_dir"]
            logger.info(f"Updated local assets directory: {self._assets_dir}")
