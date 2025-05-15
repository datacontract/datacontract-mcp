"""Common type definitions for dataproduct-mcp."""

from enum import Enum


class DataAssetType(str, Enum):
    """Types of data assets supported."""
    DATA_CONTRACT = "contract"
    DATA_PRODUCT = "product"
