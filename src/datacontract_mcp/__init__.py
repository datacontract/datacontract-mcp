"""Data Contract MCP - A Model Context Protocol implementation for data contracts and products."""

# Import core modules for easier access
from . import server
from .sources.asset_plugins.datameshmanager_client import DataMeshManager
from .asset_identifier import AssetIdentifier, DataAssetType
from .sources.asset_plugins.local import LocalAssetIdentifier
from .sources.asset_plugins.datameshmanager import DataMeshManagerAssetIdentifier
from .asset_manager import DataAssetManager
from .asset_manager import AssetLoadError, AssetQueryError
from .utils.yaml_utils import AssetParseError

# Import source plugins to ensure they're registered
from .sources import asset_source, data_source
import importlib

# Attempt to load plugins
try:
    from .sources import asset_plugins
    importlib.import_module(".asset_plugins.local", package="datacontract_mcp.sources")
    importlib.import_module(".asset_plugins.datameshmanager", package="datacontract_mcp.sources")
except ImportError:
    pass

try:
    from .sources import data_plugins
    importlib.import_module(".data_plugins.local", package="datacontract_mcp.sources")
    importlib.import_module(".data_plugins.s3", package="datacontract_mcp.sources")
except ImportError:
    pass

def main():
    """Main entry point for the package."""
    server.main()

# Expose core modules at package level
__all__ = [
    'main', 
    'server', 
    'DataMeshManager',
    'AssetIdentifier',
    'LocalAssetIdentifier',
    'DataMeshManagerAssetIdentifier',
    'DataAssetType',
    'DataAssetManager',
    'AssetLoadError', 
    'AssetParseError', 
    'AssetQueryError'
]
