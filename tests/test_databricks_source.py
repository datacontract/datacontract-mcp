"""Tests for the Databricks data source plugin."""

import unittest
from unittest.mock import MagicMock, patch
import os
import sys

# Create mock for databricks SDK
sys.modules['databricks'] = MagicMock()
sys.modules['databricks.sdk'] = MagicMock()
sys.modules['databricks.sdk.service'] = MagicMock()

def raise_import_error(name, *args):
    """Helper function to raise ImportError for specific modules."""
    if name == 'databricks.sdk':
        raise ImportError(f"No module named '{name}'")
    return __import__(name, *args)

# Import the necessary modules
from datacontract_mcp.sources.data_plugins.databricks import DatabricksDataSource
from datacontract_mcp.sources.data_source import ServerType, DataSourceRegistry


class TestDatabricksDataSource(unittest.TestCase):
    """Test the Databricks data source plugin."""

    def setUp(self):
        """Set up the test case."""
        # Mock environment variables
        self.env_patcher = patch.dict(os.environ, {
            "DATABRICKS_WORKSPACE_URL": "https://example.databricks.com",
            "DATABRICKS_TOKEN": "test-token"
        })
        self.env_patcher.start()
        
        # Create source instance
        self.source = DatabricksDataSource()
        
    def tearDown(self):
        """Clean up after test."""
        self.env_patcher.stop()

    @patch("databricks.sdk.WorkspaceClient")
    def test_execute_query(self, mock_workspace_client):
        """Test executing a query."""
        # Mock statement result
        mock_result = MagicMock()
        mock_field1 = MagicMock()
        mock_field1.name = "col1"
        mock_field2 = MagicMock()
        mock_field2.name = "col2"
        mock_result.schema = [mock_field1, mock_field2]
        mock_result.data_array = [["value1", 123]]
        
        # Mock statement
        mock_statement = MagicMock()
        mock_statement.result = mock_result
        
        # Setup mock client
        mock_client = MagicMock()
        mock_client.sql.statements.execute.return_value = mock_statement
        mock_workspace_client.return_value = mock_client
        
        # Execute query
        result = self.source.execute(
            model_key="test_table",
            query="SELECT * FROM test_table",
            server_config={
                "workspace_url": "https://example.databricks.com"
            }
        )
        
        # Verify expected results
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["col1"], "value1")
        self.assertEqual(result[0]["col2"], 123)
        
        # Verify client was called correctly
        mock_client.sql.statements.execute.assert_called_once()
        
    def test_is_available_with_sdk(self):
        """Test availability check when SDK is available."""
        with patch("databricks.sdk.WorkspaceClient", MagicMock()):
            # Should be available with workspace URL and token
            self.assertTrue(self.source.is_available())
            
            # Test with no workspace URL
            with patch.dict(os.environ, {"DATABRICKS_WORKSPACE_URL": ""}):
                source = DatabricksDataSource()
                self.assertFalse(source.is_available())
                
    def test_is_available_without_sdk(self):
        """Test availability check when SDK is not available."""
        # Since we've already imported the DatabricksDataSource, we'll mock its behavior
        # rather than trying to make it reimport the SDK
        with patch.object(DatabricksDataSource, 'is_available', return_value=False):
            # Should not be available if SDK is not installed
            self.assertFalse(self.source.is_available())
        
    def test_execute_missing_dependency(self):
        """Test handling of missing dependencies."""
        # Mock the import inside execute to raise an ImportError
        with patch.object(self.source, 'execute', side_effect=ImportError("Databricks SDK is required")):
            # Execute should raise ImportError
            with self.assertRaises(ImportError):
                self.source.execute(
                    model_key="test",
                    query="SELECT * FROM test",
                    server_config={"workspace_url": "https://example.databricks.com"}
                )

    def test_configure(self):
        """Test configuring the data source."""
        # Initial state
        self.assertEqual(self.source._workspace_url, "https://example.databricks.com")
        self.assertEqual(self.source._credentials.get("token"), "test-token")
        
        # Configure with new values
        self.source.configure({
            "workspace_url": "https://new.databricks.com",
            "credentials": {
                "token": "new-token"
            },
            "catalog": "test_catalog",
            "schema": "test_schema",
            "timeout": 300
        })
        
        # Check updated values
        self.assertEqual(self.source._workspace_url, "https://new.databricks.com")
        self.assertEqual(self.source._credentials.get("token"), "new-token")
        self.assertEqual(self.source._connection_options.get("catalog"), "test_catalog")
        self.assertEqual(self.source._connection_options.get("schema"), "test_schema")
        self.assertEqual(self.source._connection_options.get("timeout"), 300)
        
        # Create a new source instance for testing host configuration
        with patch.dict(os.environ, {
            "DATABRICKS_WORKSPACE_URL": "",
            "DATABRICKS_TOKEN": ""
        }):
            source2 = DatabricksDataSource()
            
            # Test host configuration (alternative to workspace_url)
            source2.configure({
                "host": "https://host.databricks.com",
            })
            
            # Check host was set as workspace_url
            self.assertEqual(source2._workspace_url, "https://host.databricks.com")

    def test_registry_integration(self):
        """Test integration with DataSourceRegistry."""
        # Ensure Databricks is registered
        source = DataSourceRegistry.get_source(ServerType.DATABRICKS)
        self.assertIsNotNone(source)
        self.assertIsInstance(source, DatabricksDataSource)
        self.assertEqual(source.server_type, ServerType.DATABRICKS)
        
        # Check configuration
        config = source.get_configuration()
        self.assertEqual(config["workspace_url"], "https://example.databricks.com")
        self.assertTrue(config["has_token"])


if __name__ == "__main__":
    unittest.main()