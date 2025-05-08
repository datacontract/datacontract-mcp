"""Data query source plugins for accessing and querying actual data."""

import logging
import importlib
import pkgutil

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Type, ClassVar

from ..config import get_source_config

logger = logging.getLogger("datacontract-mcp.sources.data_source")


class ServerType:
    """Enumeration of supported server types for data sources."""
    LOCAL = "local"
    S3 = "s3"
    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"
    REDSHIFT = "redshift"
    POSTGRES = "postgres"
    FILE = "file"  # Alias for LOCAL for backward compatibility


class DataFormat:
    """Enumeration of supported data formats."""
    CSV = "csv"
    PARQUET = "parquet"
    JSON = "json"
    AVRO = "avro"
    ORC = "orc"
    DELTA = "delta"


class DataSourcePlugin(ABC):
    """Base interface for data query source plugins.

    A data source plugin provides access to the actual data for querying.
    Examples include local files via DuckDB, S3 data, or database connections.

    Each plugin is responsible for:
    1. Executing queries against a specific data source type
    2. Managing connections to the data source
    3. Handling data format conversions
    """

    # Class-level registry of available plugins by server type
    _registry: ClassVar[Dict[str, Type['DataSourcePlugin']]] = {}

    @property
    @abstractmethod
    def server_type(self) -> str:
        """The server type this plugin supports (e.g., 'local', 's3')."""
        pass

    @abstractmethod
    def execute(self, model_key: str, query: str, server_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute a query against the data source.

        Args:
            model_key: The key of the model/table to query
            query: SQL query to execute
            server_config: Server configuration

        Returns:
            List of records as dictionaries

        Raises:
            DataQueryError: If the query fails
        """
        pass

    @abstractmethod
    def is_available(self) -> bool:
        """Check if this data source is properly configured and available.

        Returns:
            True if the source is available, False otherwise
        """
        pass

    @abstractmethod
    def get_configuration(self) -> Dict[str, Any]:
        """Get the current configuration for this data source.

        Returns:
            Dictionary of configuration values
        """
        pass

    @abstractmethod
    def configure(self, config: Dict[str, Any]) -> None:
        """Configure this data source with specific values.

        Args:
            config: Dictionary of configuration values
        """
        pass

    @classmethod
    def register(cls, server_type: str) -> callable:
        """Decorator to register a plugin class for a specific server type.

        Args:
            server_type: The server type this plugin handles

        Returns:
            Decorator function
        """
        def decorator(plugin_class: Type['DataSourcePlugin']) -> Type['DataSourcePlugin']:
            # Check if already registered to avoid duplicate messages
            if server_type not in cls._registry:
                cls._registry[server_type] = plugin_class
                logger.debug(f"Registered data source plugin for server type: {server_type}")
            return plugin_class
        return decorator

    @classmethod
    def get_plugin_class(cls, server_type: str) -> Optional[Type['DataSourcePlugin']]:
        """Get a plugin class by server type.

        Args:
            server_type: Type of server

        Returns:
            Plugin class if registered, None otherwise
        """
        # Handle special case for FILE (alias for LOCAL)
        if server_type == ServerType.FILE and server_type not in cls._registry and ServerType.LOCAL in cls._registry:
            server_type = ServerType.LOCAL

        return cls._registry.get(server_type)

    @classmethod
    def get_registered_types(cls) -> List[str]:
        """Get a list of all registered server types.

        Returns:
            List of server types
        """
        return list(cls._registry.keys())


class DataSourceRegistry:
    """Central registry for data source plugins.

    This class manages the registration, discovery, and access to data source plugins.
    It provides a unified interface for querying data across different sources.
    """

    # Dictionary of plugin instances by server type
    _instances: Dict[str, DataSourcePlugin] = {}

    # Flag to track if plugins have been discovered
    _plugins_discovered = False

    @classmethod
    def discover_plugins(cls) -> None:
        """Discover and load all available data source plugins."""
        if cls._plugins_discovered:
            return

        # We don't need to clear instances here - if they're already
        # properly created from previous imports, we should keep them
            
        try:
            # Import the plugins package to trigger registration
            try:
                # First, try to import the whole plugins package
                # This will trigger imports in __init__.py which registers plugins
                from ..sources import data_plugins
                
                # If we have plugins registered after importing the package, we're done
                if DataSourcePlugin.get_registered_types():
                    cls._plugins_discovered = True
                    logger.debug(f"Data plugins already registered: {', '.join(DataSourcePlugin.get_registered_types())}")
                    return
                    
            except ImportError:
                logger.warning("Data source plugins package not found, skipping auto-discovery")
                return

            # Only scan if we need to discover more plugins
            for _, name, is_pkg in pkgutil.iter_modules(data_plugins.__path__, data_plugins.__name__ + '.'):
                if not is_pkg:
                    try:
                        importlib.import_module(name)
                        logger.debug(f"Loaded data source plugin module: {name}")
                    except Exception as e:
                        logger.warning(f"Error loading data source plugin module {name}: {str(e)}")

            cls._plugins_discovered = True
        except Exception as e:
            logger.warning(f"Error during data source plugin discovery: {str(e)}")

    @classmethod
    def register_source(cls, server_type: str, plugin_class: Type[DataSourcePlugin]) -> None:
        """Register a data source plugin class."""
        # Check if already registered with the same class to avoid duplicate messages
        if server_type in DataSourcePlugin._registry:
            if DataSourcePlugin._registry[server_type] == plugin_class:
                # Already registered with the same class, nothing to do
                return
            else:
                # Log a warning if trying to register a different class for the same source
                logger.warning(f"Replacing existing data source plugin for {server_type}")
        
        # Register the plugin
        DataSourcePlugin._registry[server_type] = plugin_class
        logger.debug(f"Registered data source plugin for server type: {server_type}")

        # Clear instance if it exists to ensure fresh instantiation with the new class
        if server_type in cls._instances:
            del cls._instances[server_type]

    @classmethod
    def get_source(cls, server_type: str) -> Optional[DataSourcePlugin]:
        """Get a data source plugin instance by server type."""
        # Handle special case for FILE (alias for LOCAL)
        if server_type == ServerType.FILE and server_type not in DataSourcePlugin._registry:
            if ServerType.LOCAL in DataSourcePlugin._registry:
                server_type = ServerType.LOCAL

        # Return cached instance if available
        if server_type in cls._instances:
            return cls._instances[server_type]

        # Discover plugins if not already done
        cls.discover_plugins()

        # Get the plugin class
        plugin_class = DataSourcePlugin.get_plugin_class(server_type)
        if not plugin_class:
            return None

        # Create and cache the instance
        try:
            instance = plugin_class()

            # Apply configuration from environment variables
            try:
                source_config = get_source_config(server_type)
                if source_config:
                    instance.configure(source_config)
                    logger.debug(f"Applied configuration to data source: {server_type}")
            except ImportError:
                logger.debug("Config module not available")

            cls._instances[server_type] = instance
            return instance
        except Exception as e:
            logger.error(f"Error creating data source plugin instance for server type {server_type}: {str(e)}")
            return None

    @classmethod
    def get_available_sources(cls) -> List[str]:
        """Get a list of all available server types."""
        # Discover plugins if not already done
        cls.discover_plugins()

        # Check which sources are available
        available_types = []
        for server_type in DataSourcePlugin.get_registered_types():
            source = cls.get_source(server_type)
            if source and source.is_available():
                available_types.append(server_type)

        return available_types

    @classmethod
    def execute_query(cls,
                     server_type: str,
                     model_key: str,
                     query: str,
                     server_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute a query against a specific data source.

        Args:
            server_type: Type of server (e.g., 'local', 's3')
            model_key: Model/table key
            query: SQL query to execute
            server_config: Server configuration

        Returns:
            List of records as dictionaries

        Raises:
            DataQueryError: If the query fails or the server type is not supported
        """
        source = cls.get_source(server_type)

        if not source:
            from ..asset_manager import AssetQueryError
            supported_types = ", ".join(cls.get_available_sources())
            raise AssetQueryError(
                f"Unsupported server type: {server_type}. Available types: {supported_types}"
            )

        try:
            return source.execute(model_key, query, server_config)
        except Exception as e:
            from ..asset_manager import AssetQueryError
            raise AssetQueryError(f"Error executing query on {server_type}: {str(e)}")

    @classmethod
    def configure_source(cls, server_type: str, config: Dict[str, Any]) -> bool:
        """Configure a specific data source.

        Args:
            server_type: Type of server
            config: Configuration values

        Returns:
            True if successful, False otherwise
        """
        source = cls.get_source(server_type)
        if not source:
            return False

        try:
            source.configure(config)
            return True
        except Exception as e:
            logger.error(f"Error configuring data source {server_type}: {str(e)}")
            return False
