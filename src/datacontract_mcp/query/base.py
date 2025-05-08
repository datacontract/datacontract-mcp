"""Base classes and utilities for query strategies."""

import duckdb
import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Dict, List, Any, Iterator, Optional

from ..models_datacontract import ServerFormat

logger = logging.getLogger("datacontract-mcp.query")

# Format normalization map for string to ServerFormat enum
FORMAT_MAP = {
    'csv': ServerFormat.CSV,
    'json': ServerFormat.JSON,
    'parquet': ServerFormat.PARQUET,
    'delta': ServerFormat.DELTA,
}

@contextmanager
def create_duckdb_connection() -> Iterator[duckdb.DuckDBPyConnection]:
    """Context manager for creating and cleaning up DuckDB connections."""
    conn = duckdb.connect(database=':memory:')
    try:
        yield conn
    finally:
        conn.close()


class DataQueryStrategy(ABC):
    """Base strategy for querying different data asset server types and formats."""
    
    def normalize_format(self, format_value: Any) -> Optional[ServerFormat]:
        """Normalize format value to ServerFormat enum.

        Args:
            format_value: Format value from server config

        Returns:
            Normalized ServerFormat enum or None if unsupported
        """
        # If already an enum, return it
        if isinstance(format_value, ServerFormat):
            return format_value

        # If string, try to normalize
        if isinstance(format_value, str):
            format_str = format_value.strip().lower()

            # Try direct conversion to enum
            try:
                return ServerFormat(format_str)
            except ValueError:
                # Try lookup in mapping
                if format_str in FORMAT_MAP:
                    return FORMAT_MAP[format_str]

                logger.warning(f"Unknown format '{format_value}', could not normalize")

        return None
    
    @abstractmethod
    def execute(self, model_key: str, query: str, server_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute the query and return results."""
        pass