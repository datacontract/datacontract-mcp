
"""
Documentation loaders and helpers for Data Contract.
"""

import logging

from pathlib import Path
from typing import Dict

logger = logging.getLogger("datacontract-mcp-server.resources.docs")

# Cache for loaded documentation
_docs_cache: Dict[str, str] = {}


def get_datacontract_schema() -> str:
    """
    Get Data Contract schema.

    Returns:
        Documentation string
    """
    return _load_doc_resource("datacontract.schema.json")


def get_datacontract_example() -> str:
    """
    Get a concrete example of a Data Contract from the domain retail.

    Returns:
        Documentation string
    """
    return _load_doc_resource("example.datacontract.yaml")


def _load_doc_resource(filename: str) -> str:
    """
    Load a documentation resource file.

    Args:
        filename: Resource filename

    Returns:
        File contents as string
    """
    # Check cache first
    if filename in _docs_cache:
        return _docs_cache[filename]

    try:
        resource_extension = Path(filename).suffix.lstrip('.')

        # Try to load from package resources
        resource_path = Path(__file__).parent / resource_extension / filename

        if resource_path.exists():
            with open(resource_path, "r", encoding="utf-8") as f:
                content = f.read()
        else:
            raise FileNotFoundError(f"Documentation resource {filename} not found")

        # Cache the content
        _docs_cache[filename] = content
        return content

    except Exception as e:
        logger.error(f"Error loading documentation resource {filename}: {str(e)}")
        # Return fallback if loading fails
        return ""
