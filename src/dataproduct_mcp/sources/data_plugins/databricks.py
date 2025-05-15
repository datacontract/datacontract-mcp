"""Databricks data source plugin for querying data from Databricks."""

import logging
import os
from typing import Any, Dict, List

from ..data_source import DataSourcePlugin, ServerType

logger = logging.getLogger("dataproduct-mcp.sources.data_plugins.databricks")

def _get_env_workspace_url() -> str:
    """Get Databricks workspace URL from environment variables."""
    return os.getenv("DATABRICKS_WORKSPACE_URL", "")

def _get_env_credentials() -> Dict[str, Any]:
    """Get Databricks credentials from environment variables."""
    return {
        "token": os.getenv("DATABRICKS_TOKEN"),
        "client_id": os.getenv("DATABRICKS_CLIENT_ID"),
        "client_secret": os.getenv("DATABRICKS_CLIENT_SECRET"),
    }

@DataSourcePlugin.register(ServerType.DATABRICKS)
class DatabricksDataSource(DataSourcePlugin):
    """Plugin for querying data from Databricks."""

    def __init__(self):
        """Initialize the Databricks data source plugin."""
        self._workspace_url = _get_env_workspace_url()
        self._credentials = _get_env_credentials()
        self._connection_options = {
            "timeout": int(os.getenv("DATABRICKS_TIMEOUT", "120")),
            "catalog": os.getenv("DATABRICKS_CATALOG", ""),
            "schema": os.getenv("DATABRICKS_SCHEMA", ""),
        }

    @property
    def server_type(self) -> str:
        """The server type this plugin supports."""
        return ServerType.DATABRICKS

    def execute(self, model_key: str, query: str, server_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute a query against Databricks.

        Args:
            model_key: The name of the model or table to query
            query: SQL query to execute
            server_config: Server configuration with additional options

        Returns:
            List of records as dictionaries

        Raises:
            ImportError: If required dependencies are not installed
            Exception: For any other errors during query execution
        """
        try:
            # Import Databricks SDK
            from databricks.sdk import WorkspaceClient
            
            # Get configuration from server_config, supporting both 'workspace_url' and 'host'
            workspace_url = server_config.get("workspace_url") or server_config.get("host", self._workspace_url)
            token = server_config.get("token", self._credentials.get("token"))
            catalog = server_config.get("catalog", self._connection_options.get("catalog"))
            schema = server_config.get("schema", self._connection_options.get("schema")) 
            
            if not workspace_url:
                raise ValueError("No workspace URL or host provided in server configuration")
            
            # Ensure workspace_url is a complete URL
            if not workspace_url.startswith(("http://", "https://")):
                workspace_url = f"https://{workspace_url}"
                
            # Configure authentication
            auth = {}
            if token:
                auth["token"] = token
            elif self._credentials.get("client_id") and self._credentials.get("client_secret"):
                auth["client_id"] = self._credentials.get("client_id")
                auth["client_secret"] = self._credentials.get("client_secret")
            
            # Create workspace client
            client = WorkspaceClient(host=workspace_url, **auth)
            
            # Prepare SQL statement
            # If model_key is provided and query doesn't explicitly mention it,
            # modify the query to use the model_key as the table name
            if model_key and model_key not in query:
                # Construct fully qualified name if catalog/schema provided
                table_name = model_key
                if catalog and schema:
                    table_name = f"{catalog}.{schema}.{model_key}"
                elif schema:
                    table_name = f"{schema}.{model_key}"
                
                # Simple approach: replace "source_data" with the actual table name
                if "source_data" in query:
                    query = query.replace("source_data", table_name)
                # If query is just "SELECT *", expand it to include table name
                elif query.strip().lower() == "select *":
                    query = f"SELECT * FROM {table_name}"
            
            # Execute the query using Databricks SQL
            statement = client.sql.statements.execute(
                statement=query,
                wait_timeout=self._connection_options.get("timeout"),
                catalog=catalog,
                schema=schema
            )
            
            # Convert result to list of dictionaries
            records = []
            if statement.result and hasattr(statement.result, "data_array"):
                # Get column names from the result
                columns = []
                if statement.result.schema:
                    columns = [field.name for field in statement.result.schema]
                
                # Convert rows to dictionaries
                for row in statement.result.data_array:
                    record = {}
                    for i, column in enumerate(columns):
                        record[column] = row[i]
                    records.append(record)
                    
            return records
            
        except ImportError as e:
            logger.error(f"Error importing Databricks SDK: {e}")
            raise ImportError("Databricks SDK is required for Databricks data querying. "
                            "Install with: pip install databricks-sdk")
        except Exception as e:
            logger.error(f"Error executing Databricks query: {e}")
            raise

    def is_available(self) -> bool:
        """Check if this data source is properly configured and available."""
        try:
            # Try importing databricks.sdk directly
            try:
                import databricks.sdk  # noqa: F401
                is_importable = True
            except ImportError:
                is_importable = False
                
            if not is_importable:
                return False
                
            # Check if we have minimum required configuration
            if not self._workspace_url:
                return False
                
            # Check if we have any authentication method
            has_token = bool(self._credentials.get("token"))
            has_client_creds = (
                bool(self._credentials.get("client_id")) and 
                bool(self._credentials.get("client_secret"))
            )
            
            # Consider available if we have workspace URL and at least one auth method
            # or if default auth would work
            return bool(self._workspace_url) and (has_token or has_client_creds or True)
        except ImportError:
            return False

    def get_configuration(self) -> Dict[str, Any]:
        """Get the current configuration for this data source."""
        return {
            "workspace_url": self._workspace_url,
            "has_token": bool(self._credentials.get("token")),
            "has_client_credentials": (
                bool(self._credentials.get("client_id")) and 
                bool(self._credentials.get("client_secret"))
            ),
            "catalog": self._connection_options.get("catalog"),
            "schema": self._connection_options.get("schema"),
            "timeout": self._connection_options.get("timeout"),
        }

    def configure(self, config: Dict[str, Any]) -> None:
        """Configure this data source with specific values."""
        if "workspace_url" in config:
            self._workspace_url = config["workspace_url"]
            
        if "host" in config:
            self._workspace_url = config["host"]
            
        if "credentials" in config:
            self._credentials.update(config["credentials"])
            
        if "catalog" in config:
            self._connection_options["catalog"] = config["catalog"]
            
        if "schema" in config:
            self._connection_options["schema"] = config["schema"]
            
        if "timeout" in config:
            self._connection_options["timeout"] = int(config["timeout"])
