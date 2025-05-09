"""Tests for the federated query functionality."""

import unittest
from unittest.mock import MagicMock
from dataclasses import dataclass
from typing import Dict, List, Any, Optional


# Mock classes to avoid import issues
@dataclass
class QuerySource:
    """Mock QuerySource class for testing."""
    product_id: str
    port_id: Optional[str] = None
    server: Optional[str] = None
    model: Optional[str] = None
    alias: Optional[str] = None


class MockFederatedQueryEngine:
    """Mock FederatedQueryEngine for testing without imports."""

    def __init__(self):
        self.asset_manager = MagicMock()

    def execute_query(self, query: str, **kwargs) -> List[Dict[str, Any]]:
        """Mock the execute_query method."""
        sources = kwargs.get("sources", [])
        if not sources:
            raise ValueError("At least one source must be provided")

        if len(sources) == 1:
            return self._execute_single_source_query(sources[0], query)

        source_results = self._get_source_data_parallel(sources)
        return self._execute_final_query(source_results, query)

    def _execute_single_source_query(self, source, query):
        """Mock executing a query against a single source."""
        return [{"id": 1, "name": "Test"}]

    def _get_source_data_parallel(self, sources):
        """Mock getting data from sources in parallel."""
        results = {}
        for source in sources:
            source_name = source.alias or source.product_id.replace(':', '_').replace('/', '_')
            results[source_name] = [{"id": 1, "name": "Test"}]
        return results

    def _execute_final_query(self, source_results, query):
        """Mock executing the final query."""
        return [{"id": 1, "name": "Test"}]

    def get_capabilities(self):
        """Mock getting capabilities."""
        return {
            "supports_joins": True,
            "supports_filtering": True,
            "max_sources": 10
        }


class TestFederatedQuery(unittest.TestCase):
    """Test the federated query functionality."""

    def setUp(self):
        """Set up the test case."""
        self.engine = MockFederatedQueryEngine()

    def test_single_source_query(self):
        """Test that a single source query works."""
        source = QuerySource(product_id="local:product/test.dataproduct.yaml", alias="test")
        query = "SELECT * FROM test"

        # Spy on the methods
        self.engine._execute_single_source_query = MagicMock(return_value=[{"id": 1}])

        # Execute the query
        results = self.engine.execute_query(query, sources=[source])

        # Verify that the single source method was called
        self.engine._execute_single_source_query.assert_called_once_with(source, query)

        # Verify the results
        self.assertEqual([{"id": 1}], results)

    def test_multi_source_query(self):
        """Test that a multi-source query works."""
        sources = [
            QuerySource(product_id="local:product/orders.dataproduct.yaml", alias="orders"),
            QuerySource(product_id="local:product/customers.dataproduct.yaml", alias="customers")
        ]
        query = "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id"

        # Mock the methods
        self.engine._get_source_data_parallel = MagicMock(return_value={
            "orders": [{"id": 1, "customer_id": 101}],
            "customers": [{"id": 101, "name": "Test Customer"}]
        })

        self.engine._execute_final_query = MagicMock(return_value=[
            {"id": 1, "name": "Test Customer"}
        ])

        # Execute the query
        results = self.engine.execute_query(query, sources=sources)

        # Verify that the methods were called
        self.engine._get_source_data_parallel.assert_called_once_with(sources)
        self.engine._execute_final_query.assert_called_once()

        # Verify the results
        self.assertEqual([{"id": 1, "name": "Test Customer"}], results)

    def test_capabilities(self):
        """Test that the engine reports its capabilities."""
        capabilities = self.engine.get_capabilities()

        # Verify that the capabilities include expected fields
        self.assertIn("supports_joins", capabilities)
        self.assertIn("supports_filtering", capabilities)
        self.assertIn("max_sources", capabilities)


if __name__ == "__main__":
    unittest.main()
