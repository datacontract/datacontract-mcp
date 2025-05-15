"""Test that server can start up properly with Databricks implementation."""

import unittest
import sys
import os

class TestServerStartup(unittest.TestCase):
    """Test server startup."""

    def test_import_server(self):
        """Test that server module can be imported without errors."""
        # This will fail if there are any import errors
        from src.dataproduct_mcp import server
        
        # Verify FastMCP app exists
        self.assertTrue(hasattr(server, 'app'))
        
    def test_data_sources_available(self):
        """Test that data sources are available and properly registered."""
        # Import data source components
        from src.dataproduct_mcp.sources.data_source import DataSourceRegistry, ServerType, DataSourcePlugin
        
        # Force discovery of plugins
        DataSourceRegistry.discover_plugins()
        
        # Get available types
        available_types = DataSourceRegistry.get_available_sources()
        
        # Local should always be available as it has no external dependencies
        self.assertIn(ServerType.LOCAL, available_types)
        
        # Check if Databricks is recognized as a type (even if not necessarily available)
        registered_types = DataSourcePlugin.get_registered_types()
        self.assertIn(ServerType.DATABRICKS, registered_types)
        
        # Show available types for debugging
        print(f"Available source types: {available_types}")
        print(f"Registered source types: {registered_types}")
        
        # Check if Databricks SDK is importable
        try:
            import databricks.sdk
            print("Databricks SDK is installed and importable")
            
            # Get the Databricks source instance
            databricks_source = DataSourceRegistry.get_source(ServerType.DATABRICKS)
            if databricks_source:
                print(f"Databricks source instance: {databricks_source}")
                print(f"Databricks availability check result: {databricks_source.is_available()}")
                print(f"Databricks configuration: {databricks_source.get_configuration()}")
                
                # Test with mocked environment variables
                import os
                from unittest.mock import patch
                with patch.dict(os.environ, {
                    "DATABRICKS_WORKSPACE_URL": "https://test.databricks.com",
                    "DATABRICKS_TOKEN": "test-token"
                }):
                    # Create a new instance with the mocked environment
                    from src.dataproduct_mcp.sources.data_plugins.databricks import DatabricksDataSource
                    test_source = DatabricksDataSource()
                    print(f"Test source with mock env vars - availability: {test_source.is_available()}")
                    print(f"Test source configuration: {test_source.get_configuration()}")
            else:
                print("Databricks source instance is None")
        except ImportError as e:
            print(f"Databricks SDK import error: {e}")
        
if __name__ == "__main__":
    unittest.main()
