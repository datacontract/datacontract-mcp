"""Data Contract MCP - A Model Context Protocol implementation for data contracts and products."""

# Import core modules for easier access
from . import server
from .sources.asset_plugins.datameshmanager_client import DataMeshManager
from .types import DataAssetType
from .asset_identifier import AssetIdentifier
from .sources.asset_plugins.local import LocalAssetIdentifier
from .sources.asset_plugins.datameshmanager import DataMeshManagerAssetIdentifier
from .asset_manager import DataAssetManager
from .asset_manager import AssetLoadError, AssetQueryError
from .utils.yaml_utils import AssetParseError

# Import source registries (but not plugins - we'll let discovery handle that)
from .sources import asset_source, data_source

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
    'AssetQueryError',
    'asset_source',
    'data_source'
]
