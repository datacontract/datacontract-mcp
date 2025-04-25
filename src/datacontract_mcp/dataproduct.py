"""Data Product core functionality using Pydantic models."""

import logging
import os
import yaml

from pathlib import Path
from typing import List

from .models_dataproduct import DataProduct

logger = logging.getLogger("datacontract-mcp.dataproduct")

dataproducts_source = os.getenv("DATAPRODUCTS_SOURCE", "")
if dataproducts_source == "":
    raise ValueError(f"DATAPRODUCTS_SOURCE environment variable required. Working directory: {os.getcwd()}")


def load_product_file(filename: str) -> str:
    """
    Load a data product file from the configured source directory.

    Args:
        filename: Name of the data product file

    Returns:
        File contents as string

    Raises:
        FileNotFoundError: If the file is not found
    """
    resource_path = Path(f"{dataproducts_source}/{filename}")

    if not resource_path.exists():
        raise FileNotFoundError(f"Data product file {filename} not found at {resource_path}")

    with open(resource_path, "r", encoding="utf-8") as f:
        content = f.read()

    return content


def parse_product(content: str) -> DataProduct:
    """
    Parse a data product string into a validated DataProduct object.

    Args:
        content: Data product YAML content

    Returns:
        Validated DataProduct object

    Raises:
        ValueError: If product is invalid or parsing fails
    """
    try:
        # First parse with PyYAML to get the raw dictionary
        product_dict = yaml.safe_load(content)

        # Then validate with Pydantic
        return DataProduct.model_validate(product_dict)

    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML: {str(e)}")
    except Exception as e:
        raise ValueError(f"Error validating data product: {str(e)}")


def get_product(filename: str) -> DataProduct:
    """
    Load and parse a data product file.

    Args:
        filename: Name of the data product file

    Returns:
        Validated DataProduct object

    Raises:
        FileNotFoundError: If the file is not found
        ValueError: If product is invalid or parsing fails
    """
    content = load_product_file(filename)
    return parse_product(content)


def list_product_files() -> List[str]:
    """
    List all available data product files.

    Returns:
        List of filenames
    """
    files = []
    if not os.path.exists(dataproducts_source):
        logger.warning(f"Data products directory {dataproducts_source} does not exist")
        return files

    for fname in os.listdir(dataproducts_source):
        file_path = os.path.join(dataproducts_source, fname)
        if os.path.isfile(file_path) and fname.lower().endswith('.yaml'):
            files.append(fname)

    return files
