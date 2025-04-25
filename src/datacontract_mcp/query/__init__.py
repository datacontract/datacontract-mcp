"""Query module for executing queries against data contracts."""

from .base import QueryStrategy, create_duckdb_connection
from .factory import get_query_strategy
from .local import LocalFileStrategy
from .s3 import S3Strategy

__all__ = [
    'QueryStrategy',
    'create_duckdb_connection',
    'get_query_strategy',
    'LocalFileStrategy',
    'S3Strategy',
]