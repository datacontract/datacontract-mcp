from . import server
from . import datacontract

def main():
    """Main entry point for the package."""
    server.main()

# Expose core modules at package level
__all__ = ['main', 'server', 'datacontract']
