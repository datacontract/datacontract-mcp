"""YAML utilities for dataproduct-mcp."""

import logging
import yaml
import hashlib
from typing import Dict, Any

logger = logging.getLogger("dataproduct-mcp.utils.yaml_utils")


class AssetParseError(Exception):
    """Error raised when parsing an asset file fails."""
    pass


def parse_yaml(content: str | bytes) -> Dict[str, Any]:
    """
    Parse a YAML string or bytes into a dictionary.

    Args:
        content: YAML content (string or bytes)

    Returns:
        Dictionary representation of the YAML content

    Raises:
        AssetParseError: If YAML is invalid
    """
    try:
        # Parse with PyYAML to get the raw dictionary
        asset_dict = yaml.safe_load(content)

        if not isinstance(asset_dict, dict):
            raise AssetParseError("YAML content does not represent a dictionary")

        # Make sure we have at least basic required fields
        if "id" not in asset_dict:
            logger.warning("Missing 'id' field in asset")
            # Use a default ID based on content hash if missing
            # Convert content to string first, then encode to bytes
            content_str = content.decode('utf-8') if isinstance(content, bytes) else content
            asset_dict["id"] = f"default_{hashlib.md5(str(content_str).encode('utf-8')).hexdigest()[:8]}"

        # Ensure we have an info section with at least a title
        if "info" not in asset_dict or not isinstance(asset_dict["info"], dict):
            logger.warning("Missing or invalid 'info' section in asset, creating default")
            asset_dict["info"] = asset_dict.get("info", {})
            if not isinstance(asset_dict["info"], dict):
                asset_dict["info"] = {}

        if "title" not in asset_dict["info"]:
            asset_dict["info"]["title"] = asset_dict.get("id", "Untitled")

        return asset_dict

    except yaml.YAMLError as e:
        raise AssetParseError(f"Error parsing YAML: {str(e)}")
    except AssetParseError:
        raise
    except Exception as e:
        logger.warning(f"Error in parse_yaml: {str(e)}")
        # Try to return a basic dict that won't break functionality
        return {"id": "default", "info": {"title": "Default"}}
