"""Simple environment variable-based configuration."""

import os
import logging
from typing import Dict, Any, List, Literal

logger = logging.getLogger("dataproduct-mcp.config")

# Source types
SourceType = Literal["asset", "data"]

# Map of environment variables required for each asset source
ASSET_SOURCE_ENV_VARS = {
    "local": "DATAASSET_SOURCE",  # Directory for local assets
    "datameshmanager": "DATAMESH_MANAGER_API_KEY"  # API key for Data Mesh Manager
}

# Map of environment variables required for each data source
DATA_SOURCE_ENV_VARS = {
    "local": None,  # Local data source doesn't require a specific env var
    "s3": None,     # S3 can use IAM roles, so no required env var
    "databricks": "DATABRICKS_WORKSPACE_URL",  # Databricks workspace URL is required
}

# Additional environment variables
ADDITIONAL_ENV_VARS = {
    # Asset sources
    "datameshmanager": {
        "DATAMESH_MANAGER_HOST": "https://api.datamesh-manager.com"  # Default API URL
    },

    # Data sources
    "s3": {
        "AWS_REGION": "us-east-1",  # Default region
        "S3_ALLOWED_BUCKETS": "",  # Comma-separated list of allowed buckets
        "S3_MAX_BUCKETS": "10"  # Maximum number of buckets
    },
    
    # Databricks configuration
    "databricks": {
        "DATABRICKS_TIMEOUT": "120",  # Default timeout in seconds
        "DATABRICKS_CATALOG": "",     # Default catalog (optional)
        "DATABRICKS_SCHEMA": "",      # Default schema (optional)
    }
}


def get_config() -> Dict[str, Any]:
    """
    Get configuration based on environment variables.

    Returns:
        Dictionary with configuration
    """
    config = {}

    # Asset source configurations
    config["asset_sources"] = {}

    # Local file source
    local_dir = os.getenv(ASSET_SOURCE_ENV_VARS["local"])
    config["asset_sources"]["local"] = {
        "enabled": bool(local_dir),
        "assets_dir": local_dir or ""
    }

    # Data Mesh Manager source
    dmm_api_key = os.getenv(ASSET_SOURCE_ENV_VARS["datameshmanager"])
    config["asset_sources"]["datameshmanager"] = {
        "enabled": bool(dmm_api_key),
        "api_key": dmm_api_key,
        "api_url": os.getenv("DATAMESH_MANAGER_HOST", ADDITIONAL_ENV_VARS["datameshmanager"]["DATAMESH_MANAGER_HOST"])
    }

    # Data source configurations
    config["data_sources"] = {}

    # Local data source
    config["data_sources"]["local"] = {
        "enabled": True,  # Always enabled if DuckDB is installed
        "connection_pooling": os.getenv("DATACONTRACT_LOCAL_CONNECTION_POOLING", "1") != "0",
        "max_connections": int(os.getenv("DATACONTRACT_LOCAL_MAX_CONNECTIONS", "5")),
        "idle_timeout": int(os.getenv("DATACONTRACT_LOCAL_IDLE_TIMEOUT", "300"))
    }

    # S3 data source
    config["data_sources"]["s3"] = {
        "enabled": True,  # Will be checked based on DuckDB availability and AWS creds
        "region": os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1")),
        "allowed_buckets": [b.strip() for b in os.getenv("S3_ALLOWED_BUCKETS", "").split(",") if b.strip()],
        "max_buckets": int(os.getenv("S3_MAX_BUCKETS", "10")),
        "endpoint_url": os.getenv("AWS_ENDPOINT_URL"),
        "credentials": {
            "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "aws_session_token": os.getenv("AWS_SESSION_TOKEN")
        }
    }

    # Databricks data source
    workspace_url = os.getenv("DATABRICKS_WORKSPACE_URL")
    config["data_sources"]["databricks"] = {
        "enabled": bool(workspace_url),
        "workspace_url": workspace_url,
        "catalog": os.getenv("DATABRICKS_CATALOG", ""),
        "schema": os.getenv("DATABRICKS_SCHEMA", ""),
        "timeout": int(os.getenv("DATABRICKS_TIMEOUT", "120")),
        "credentials": {
            "token": os.getenv("DATABRICKS_TOKEN"),
            "client_id": os.getenv("DATABRICKS_CLIENT_ID"),
            "client_secret": os.getenv("DATABRICKS_CLIENT_SECRET"),
        }
    }

    return config


def get_source_config(source_name: str, source_type: SourceType = "asset") -> Dict[str, Any]:
    """
    Get configuration for a specific source.

    Args:
        source_name: Name of the source
        source_type: Type of source ("asset" or "data")

    Returns:
        Dictionary of configuration for the source
    """
    config = get_config()
    source_category = f"{source_type}_sources"
    return config.get(source_category, {}).get(source_name, {})


def is_source_enabled(source_name: str, source_type: SourceType = "asset") -> bool:
    """
    Check if a source is enabled based on environment variables.

    Args:
        source_name: Name of the source
        source_type: Type of source ("asset" or "data")

    Returns:
        True if the source is enabled, False otherwise
    """
    # Select appropriate environment variable mapping based on source type
    env_vars = ASSET_SOURCE_ENV_VARS if source_type == "asset" else DATA_SOURCE_ENV_VARS

    if source_name not in env_vars:
        return False

    # Special case for data sources that don't require specific env vars
    if source_type == "data" and env_vars[source_name] is None:
        return True

    env_var = env_vars[source_name]
    return bool(os.getenv(env_var))


def get_enabled_sources(source_type: SourceType = "asset") -> List[str]:
    """
    Get a list of all enabled sources of a specific type.

    Args:
        source_type: Type of source ("asset" or "data")

    Returns:
        List of enabled source names
    """
    # Select appropriate environment variable mapping based on source type
    env_vars = ASSET_SOURCE_ENV_VARS if source_type == "asset" else DATA_SOURCE_ENV_VARS

    return [
        source_name
        for source_name in env_vars
        if is_source_enabled(source_name, source_type)
    ]
