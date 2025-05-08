"""Type definitions for query functionality."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Any, Optional


@dataclass
class QuerySource:
    """Represents a data source for federated queries."""
    product_id: str
    port_id: Optional[str] = None
    server: Optional[str] = None
    model: Optional[str] = None
    alias: Optional[str] = None


class QueryExecutor(ABC):
    """Abstract base class for query executors."""

    @abstractmethod
    def execute_query(self, query: str, **kwargs) -> List[Dict[str, Any]]:
        """
        Execute a query and return results.

        Args:
            query: SQL query to execute
            **kwargs: Additional query parameters

        Returns:
            Query results as a list of dictionaries
        """
        pass

    @abstractmethod
    def get_capabilities(self) -> Dict[str, Any]:
        """
        Get capabilities of this query executor.

        Returns:
            Dictionary of capabilities
        """
        pass
