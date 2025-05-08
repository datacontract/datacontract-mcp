"""Source plugins for data asset metadata (data products and contracts)."""

import logging
import importlib
import pkgutil

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Type, ClassVar, TYPE_CHECKING, ForwardRef

from ..types import DataAssetType
from ..config import get_source_config, get_enabled_sources

# Use a forward reference for AssetIdentifier to avoid circular import
if TYPE_CHECKING:
    from ..asset_identifier import AssetIdentifier
else:
    AssetIdentifier = ForwardRef('AssetIdentifier')

logger = logging.getLogger("datacontract-mcp.sources.asset_source")


class AssetSourcePlugin(ABC):
    """Base interface for data asset source plugins.

    An asset source plugin provides access to data asset metadata (data products
    and contracts) from a specific source system. Examples include local files,
    Data Mesh Manager API, or other external catalog systems.

    Each plugin is responsible for:
    1. Creating appropriate identifiers for its source
    2. Listing available assets from the source
    3. Loading asset content from the source
    """

    # Class-level registry of available plugins by source name
    _registry: ClassVar[Dict[str, Type['AssetSourcePlugin']]] = {}

    @property
    @abstractmethod
    def source_name(self) -> str:
        """The unique name of this source (e.g., 'local', 'datameshmanager')."""
        pass

    @abstractmethod
    def get_identifier(self, asset_type: DataAssetType, asset_id: str) -> AssetIdentifier:
        """Create an identifier for this source.

        Args:
            asset_type: Type of asset (product or contract)
            asset_id: Unique identifier for the asset within this source

        Returns:
            An appropriate AssetIdentifier for the specified asset
        """
        pass

    @abstractmethod
    def list_assets(self, asset_type: DataAssetType) -> List[AssetIdentifier]:
        """List all assets of a specific type from this source.

        Args:
            asset_type: Type of asset (product or contract)

        Returns:
            List of AssetIdentifier objects for all available assets
        """
        pass

    @abstractmethod
    def load_asset_content(self, identifier: AssetIdentifier) -> str:
        """Load the content of an asset as a string.

        Args:
            identifier: AssetIdentifier for the asset to load

        Returns:
            Asset content as a string

        Raises:
            AssetLoadError: If the asset cannot be loaded
        """
        pass

    @abstractmethod
    def is_available(self) -> bool:
        """Check if this source is properly configured and available.

        Returns:
            True if the source is available, False otherwise
        """
        pass

    @abstractmethod
    def get_configuration(self) -> Dict[str, Any]:
        """Get the current configuration for this source.

        Returns:
            Dictionary of configuration values
        """
        pass

    @abstractmethod
    def configure(self, config: Dict[str, Any]) -> None:
        """Configure this source with specific values.

        Args:
            config: Dictionary of configuration values
        """
        pass

    @classmethod
    def register(cls, plugin_class: Type['AssetSourcePlugin']) -> Type['AssetSourcePlugin']:
        """Register a plugin class.

        Args:
            plugin_class: The plugin class to register

        Returns:
            The registered plugin class (for decorator usage)
        """
        # Create an instance to get the source name
        plugin_instance = plugin_class()
        source_name = plugin_instance.source_name
        
        # Check if already registered to avoid duplicate messages
        if source_name not in cls._registry:
            cls._registry[source_name] = plugin_class
            logger.debug(f"Registered asset source plugin: {source_name}")
        
        return plugin_class

    @classmethod
    def get_plugin_class(cls, source_name: str) -> Optional[Type['AssetSourcePlugin']]:
        """Get a plugin class by source name.

        Args:
            source_name: Name of the source

        Returns:
            Plugin class if registered, None otherwise
        """
        return cls._registry.get(source_name)

    @classmethod
    def get_registered_sources(cls) -> List[str]:
        """Get a list of all registered source names.

        Returns:
            List of source names
        """
        return list(cls._registry.keys())


class AssetSourceRegistry:
    """Central registry for asset source plugins.

    This class manages the registration, discovery, and access to asset source plugins.
    It provides a unified interface for working with different data asset sources.
    """

    # Dictionary of plugin instances by source name
    _instances: Dict[str, AssetSourcePlugin] = {}

    # Flag to track if plugins have been discovered
    _plugins_discovered = False

    @classmethod
    def discover_plugins(cls) -> None:
        """Discover and load all available asset source plugins."""
        if cls._plugins_discovered:
            return

        # We don't need to clear instances here - if they're already
        # properly created from previous imports, we should keep them
        
        try:
            # Import the plugins package to trigger registration
            try:
                # First, try to import the whole plugins package
                # This will trigger imports in __init__.py which registers plugins
                from ..sources import asset_plugins
                
                # If we have plugins registered after importing the package, we're done
                if AssetSourcePlugin.get_registered_sources():
                    cls._plugins_discovered = True
                    logger.debug(f"Asset plugins already registered: {', '.join(AssetSourcePlugin.get_registered_sources())}")
                    return
                    
            except ImportError:
                logger.warning("Asset source plugins package not found, skipping auto-discovery")
                return

            # Only scan if we need to discover more plugins
            for _, name, is_pkg in pkgutil.iter_modules(asset_plugins.__path__, asset_plugins.__name__ + '.'):
                if not is_pkg:
                    try:
                        importlib.import_module(name)
                        logger.debug(f"Loaded asset source plugin module: {name}")
                    except Exception as e:
                        logger.warning(f"Error loading asset source plugin module {name}: {str(e)}")

            cls._plugins_discovered = True
        except Exception as e:
            logger.warning(f"Error during asset source plugin discovery: {str(e)}")

    @classmethod
    def register_source(cls, source_name: str, plugin_class: Type[AssetSourcePlugin]) -> None:
        """Register a source plugin class."""
        # Check if already registered with the same class to avoid duplicate messages
        if source_name in AssetSourcePlugin._registry:
            if AssetSourcePlugin._registry[source_name] == plugin_class:
                # Already registered with the same class, nothing to do
                return
            else:
                # Log a warning if trying to register a different class for the same source
                logger.warning(f"Replacing existing asset source plugin for {source_name}")
        
        # Register the plugin
        AssetSourcePlugin._registry[source_name] = plugin_class
        logger.debug(f"Registered asset source plugin: {source_name}")

        # Clear instance if it exists to ensure fresh instantiation with the new class
        if source_name in cls._instances:
            del cls._instances[source_name]

    @classmethod
    def get_source(cls, source_name: str) -> Optional[AssetSourcePlugin]:
        """Get a source plugin instance by name."""
        # Return cached instance if available
        if source_name in cls._instances:
            return cls._instances[source_name]

        # Discover plugins if not already done
        cls.discover_plugins()

        # Get the plugin class
        plugin_class = AssetSourcePlugin.get_plugin_class(source_name)
        if not plugin_class:
            return None

        # Create and cache the instance
        try:
            instance = plugin_class()

            # Apply configuration from environment variables
            source_config = get_source_config(source_name)
            if source_config:
                instance.configure(source_config)
                logger.debug(f"Applied configuration to asset source: {source_name}")

            cls._instances[source_name] = instance
            return instance
        except Exception as e:
            logger.error(f"Error creating asset source plugin instance for source {source_name}: {str(e)}")
            return None

    @classmethod
    def get_available_sources(cls) -> List[str]:
        """Get a list of all available source names."""
        # Get enabled sources from global config function
        enabled_sources = get_enabled_sources()

        available_sources = []
        for source_name in enabled_sources:
            # Get the source plugin
            source = cls.get_source(source_name)
            if source:
                available_sources.append(source_name)

        if available_sources:
            return available_sources

        # Fallback to plugin availability checks if no sources were found

        # Discover plugins if not already done
        cls.discover_plugins()

        # Check which sources are available based on their own checks
        available_sources = []
        for source_name in AssetSourcePlugin.get_registered_sources():
            source = cls.get_source(source_name)
            if source and source.is_available():
                available_sources.append(source_name)

        return available_sources

    @classmethod
    def get_identifier_from_string(cls, identifier_str: str) -> Optional[AssetIdentifier]:
        """Create an asset identifier from a string representation."""
        try:
            # Split the string into source, type, and id parts
            if ":" not in identifier_str:
                raise ValueError(f"Invalid identifier format (missing source): {identifier_str}")

            source_name, rest = identifier_str.split(":", 1)

            if "/" not in rest:
                raise ValueError(f"Invalid identifier format (missing type): {identifier_str}")

            asset_type_str, asset_id = rest.split("/", 1)

            # Validate components
            if not source_name:
                raise ValueError(f"Missing source in identifier: {identifier_str}")
            if not asset_type_str:
                raise ValueError(f"Missing asset type in identifier: {identifier_str}")
            if not asset_id:
                raise ValueError(f"Missing asset ID in identifier: {identifier_str}")

            # Convert asset type string to enum
            try:
                asset_type = DataAssetType(asset_type_str)
            except ValueError:
                raise ValueError(f"Invalid asset type: {asset_type_str}")

            # Get the source plugin
            source = cls.get_source(source_name)
            if not source:
                raise ValueError(f"Unknown asset source: {source_name}")

            # Create the identifier
            return source.get_identifier(asset_type, asset_id)

        except Exception as e:
            logger.error(f"Error parsing identifier '{identifier_str}': {str(e)}")
            return None

    @classmethod
    def create_identifier(cls, source_name: str, asset_type: DataAssetType, asset_id: str) -> Optional[AssetIdentifier]:
        """Create an asset identifier for a specific source."""
        source = cls.get_source(source_name)
        if not source:
            return None

        return source.get_identifier(asset_type, asset_id)

    @classmethod
    def list_assets(cls, asset_type: DataAssetType) -> List[AssetIdentifier]:
        """List all available assets of a specific type across all sources."""
        all_assets = []

        for source_name in cls.get_available_sources():
            source = cls.get_source(source_name)
            if source:
                try:
                    source_assets = source.list_assets(asset_type)
                    all_assets.extend(source_assets)
                except Exception as e:
                    logger.warning(f"Error listing assets from source {source_name}: {str(e)}")

        return all_assets

    @classmethod
    def load_content(cls, identifier: AssetIdentifier) -> str:
        """Load the content of an asset."""
        source_name = identifier.source
        source = cls.get_source(source_name)

        if not source:
            from ..asset_manager import AssetLoadError
            raise AssetLoadError(f"Unknown source: {source_name}")

        return source.load_asset_content(identifier)

    @classmethod
    def configure_source(cls, source_name: str, config: Dict[str, Any]) -> bool:
        """Configure a specific source."""
        source = cls.get_source(source_name)
        if not source:
            return False

        try:
            source.configure(config)
            return True
        except Exception as e:
            logger.error(f"Error configuring source {source_name}: {str(e)}")
            return False
