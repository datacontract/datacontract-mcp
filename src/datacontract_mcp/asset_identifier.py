"""Object-oriented asset identification system for data contracts and data products."""

import logging
from typing import TYPE_CHECKING

logger = logging.getLogger("datacontract-mcp.asset_identifier")

# Avoid circular import
if TYPE_CHECKING:
    from .sources.asset_source import AssetSourceRegistry


class AssetIdentifier:
    """Base class for all asset identifiers.

    An AssetIdentifier uniquely identifies a data asset (contract or product)
    and provides methods to load its content. Each identifier is associated with
    a specific source (e.g., local files, DataMeshManager API).

    The string representation format is [source]:[type]/[id], for example:
    - local:product/orders.dataproduct.yaml
    - datameshmanager:contract/123
    """

    def __init__(self, asset_id: str, asset_type: str, source_name: str):
        """
        Initialize an asset identifier.

        Args:
            asset_id: Unique identifier for the asset
            asset_type: Type of asset ("product" or "contract")
            source_name: Name of the source (e.g., 'local', 'datameshmanager')
        """
        if asset_type not in ["product", "contract"]:
            raise ValueError(f"Invalid asset type: {asset_type}")

        self.asset_id = asset_id
        self.asset_type = asset_type
        self._source_name = source_name

    @classmethod
    def from_string(cls, identifier_str: str) -> 'AssetIdentifier':
        """
        Create an asset identifier from a string representation.

        Args:
            identifier_str: String in the format [source]:[type]/[id]

        Returns:
            AssetIdentifier instance

        Raises:
            ValueError: If the string format is invalid
        """
        # Import here to avoid circular imports
        from .sources.asset_source import AssetSourceRegistry
        
        # Use the asset source registry to parse and create the identifier
        identifier = AssetSourceRegistry.get_identifier_from_string(identifier_str)

        if not identifier:
            raise ValueError(f"Invalid asset identifier format: {identifier_str}")

        return identifier

    @property
    def source(self) -> str:
        """The source of the asset (e.g., 'local', 'datameshmanager')."""
        return self._source_name

    def is_product(self) -> bool:
        """Check if this identifier refers to a data product."""
        return self.asset_type == "product"

    def is_contract(self) -> bool:
        """Check if this identifier refers to a data contract."""
        return self.asset_type == "contract"

    def load_content(self) -> str:
        """
        Load the content of this asset.

        Returns:
            Content as a string

        Raises:
            AssetLoadError: If loading fails
        """
        # Import here to avoid circular imports
        from .sources.asset_source import AssetSourceRegistry
        return AssetSourceRegistry.load_content(self)

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


