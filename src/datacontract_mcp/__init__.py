from . import server
from .datameshmanager import DataMeshManager

def main():
    """Main entry point for the package."""
    server.main()

# Expose core modules at package level
__all__ = ['main', 'server', 'DataMeshManager']
