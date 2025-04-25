"""Base classes and utilities for query strategies."""

import duckdb
import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Dict, List, Any, Iterator

logger = logging.getLogger("datacontract-mcp.query")

@contextmanager
def create_duckdb_connection() -> Iterator[duckdb.DuckDBPyConnection]:
    """Context manager for creating and cleaning up DuckDB connections."""
    conn = duckdb.connect(database=':memory:')
    try:
        yield conn
    finally:
        conn.close()


class QueryStrategy(ABC):
    """Base strategy for querying different server types and formats."""
    
    @abstractmethod
    def execute(self, model_key: str, query: str, server_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute the query and return results."""
        pass