"""Query module for executing queries against data assets (contracts and products)."""

from .base import DataQueryStrategy, create_duckdb_connection
from .factory import get_query_strategy
from .local import LocalFileQueryStrategy
from .s3 import S3QueryStrategy

__all__ = [
    'DataQueryStrategy',
    'create_duckdb_connection',
    'get_query_strategy',
    'LocalFileQueryStrategy',
    'S3QueryStrategy',
]