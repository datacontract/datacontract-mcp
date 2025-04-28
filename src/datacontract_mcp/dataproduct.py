"""Data Product core functionality using Pydantic models."""

import logging
from typing import List

from .models_dataproduct import DataProduct

from .asset_utils import (
    load_asset_file,
    parse_yaml_with_model,
    list_files_with_extension
)

logger = logging.getLogger("datacontract-mcp.dataproduct")


def load_product_file(filename: str) -> str:
    """
    Load a data product file from the configured source directory.

    Args:
        filename: Name of the data product file

    Returns:
        File contents as string

    Raises:
        AssetLoadError: If the file is not found
    """
    return load_asset_file(filename)


def parse_product(content: str) -> DataProduct:
    """
    Parse a data product string into a validated DataProduct object.

    Args:
        content: Data product YAML content

    Returns:
        Validated DataProduct object

    Raises:
        AssetParseError: If product is invalid or parsing fails
    """
    return parse_yaml_with_model(content, DataProduct)


def get_product(filename: str) -> DataProduct:
    """
    Load and parse a data product file.

    Args:
        filename: Name of the data product file

    Returns:
        Validated DataProduct object

    Raises:
        AssetLoadError: If the file is not found
        AssetParseError: If product is invalid or parsing fails
    """
    content = load_product_file(filename)
    return parse_product(content)


def list_product_files() -> List[str]:
    """
    List all available data product files.

    Returns:
        List of filenames
    """
    return list_files_with_extension('.dataproduct.yaml')
