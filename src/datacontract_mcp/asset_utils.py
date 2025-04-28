"""Common utilities for data contracts and data products."""

import logging
import os
import yaml
from pathlib import Path
from typing import List, Type, TypeVar

# Type variable for Pydantic models
T = TypeVar('T')

logger = logging.getLogger("datacontract-mcp.asset_utils")

# Common asset source directory
ASSETS_SOURCE = os.getenv("DATAASSET_SOURCE", "")
if ASSETS_SOURCE == "":
    raise ValueError(f"DATAASSET_SOURCE environment variable required. Working directory: {os.getcwd()}")


class AssetLoadError(Exception):
    """Error raised when loading an asset file fails."""
    pass


class AssetParseError(Exception):
    """Error raised when parsing an asset file fails."""
    pass


class AssetQueryError(Exception):
    """Error raised when querying an asset fails."""
    pass


def load_asset_file(filename: str) -> str:
    """
    Load a data asset file from the configured source directory.

    Args:
        filename: Name of the asset file

    Returns:
        File contents as string

    Raises:
        AssetLoadError: If the file is not found or cannot be read
    """
    resource_path = Path(f"{ASSETS_SOURCE}/{filename}")

    if not resource_path.exists():
        raise AssetLoadError(f"Asset file {filename} not found at {resource_path}")

    try:
        with open(resource_path, "r", encoding="utf-8") as f:
            content = f.read()
        return content
    except Exception as e:
        raise AssetLoadError(f"Error reading asset file {filename}: {str(e)}")


def parse_yaml_with_model(content: str, model_class: Type[T]) -> T:
    """
    Parse a YAML string into a validated Pydantic model object.

    Args:
        content: YAML content
        model_class: Pydantic model class to validate against

    Returns:
        Validated Pydantic model object

    Raises:
        AssetParseError: If YAML is invalid or model validation fails
    """
    try:
        # First parse with PyYAML to get the raw dictionary
        asset_dict = yaml.safe_load(content)

        # Then validate with Pydantic
        return model_class.model_validate(asset_dict)

    except yaml.YAMLError as e:
        raise AssetParseError(f"Error parsing YAML: {str(e)}")
    except Exception as e:
        raise AssetParseError(f"Error validating data asset: {str(e)}")


def list_files_with_extension(extension: str) -> List[str]:
    """
    List all available files with the given extension in the assets directory.

    Args:
        extension: File extension to filter by (e.g., '.datacontract.yaml')

    Returns:
        List of filenames
    """
    files = []
    if not os.path.exists(ASSETS_SOURCE):
        logger.warning(f"Assets directory {ASSETS_SOURCE} does not exist")
        return files

    for fname in os.listdir(ASSETS_SOURCE):
        file_path = os.path.join(ASSETS_SOURCE, fname)
        if os.path.isfile(file_path) and fname.lower().endswith(extension):
            files.append(fname)

    return files
