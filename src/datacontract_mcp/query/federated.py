"""Federated query engine for executing queries across multiple data products."""

import logging
import concurrent.futures
from typing import Dict, List, Any

from .types import QueryExecutor, QuerySource

logger = logging.getLogger("datacontract-mcp.query.federated")


class FederatedQueryEngine(QueryExecutor):
    """Engine for executing federated queries across multiple data sources."""

    def __init__(self, asset_manager):
        """
        Initialize the federated query engine.

        Args:
            asset_manager: DataAssetManager instance for accessing assets
        """
        self.asset_manager = asset_manager

    def execute_query(self, query: str, **kwargs) -> List[Dict[str, Any]]:
        """
        Execute a query with the specified parameters.

        Args:
            query: SQL query to execute
            **kwargs: Additional parameters:
                - sources: List of QuerySource objects

        Returns:
            Query results
        """
        sources = kwargs.get("sources", [])
        if not sources:
            raise ValueError("At least one source must be provided via the 'sources' parameter")

        return self._execute_federated_query(sources, query)

    def _execute_federated_query(self, sources: List[QuerySource], query: str) -> List[Dict[str, Any]]:
        """
        Execute a query across multiple data products.

        Args:
            sources: List of data sources to query
            query: SQL query to execute

        Returns:
            Query results
        """
        # Handle single source case using the existing path for efficiency
        if len(sources) == 1:
            source = sources[0]
            logger.info(f"Single source query detected, using direct query path for {source.product_id}")
            return self._execute_single_source_query(source, query)

        # For multiple sources, use the federated query execution path
        logger.info(f"Executing federated query across {len(sources)} sources")

        # Get source data in parallel
        source_results = self._get_source_data_parallel(sources)

        # Execute final query using DuckDB
        return self._execute_final_query(source_results, query)

    def _execute_single_source_query(self, source: QuerySource, query: str) -> List[Dict[str, Any]]:
        """Execute a query against a single data source."""
        try:
            # Dynamically import to avoid circular dependency
            from ..asset_identifier import AssetIdentifier

            identifier = AssetIdentifier.from_string(source.product_id)
            if not identifier.is_product():
                raise ValueError(f"Source identifier must be a data product: {source.product_id}")

            # Use the existing DataAssetManager query mechanism
            return self.asset_manager.query_product(
                identifier=identifier,
                query=query,
                port_id=source.port_id,
                server_key=source.server,
                model_key=source.model
            )
        except Exception as e:
            logger.error(f"Error executing single source query: {str(e)}")
            raise

    def _get_source_data_parallel(self, sources: List[QuerySource]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get data from all sources in parallel.

        Args:
            sources: List of data sources

        Returns:
            Dictionary mapping source aliases to their data
        """
        source_results = {}

        # Use concurrent.futures to execute queries in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(sources)) as executor:
            # Submit all queries - using SELECT * to get all data from each source
            future_to_source = {
                executor.submit(self._get_source_data, source): source
                for source in sources
            }

            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_source):
                source = future_to_source[future]
                try:
                    results = future.result()
                    # Use alias if provided, otherwise use qualified name
                    source_name = source.alias or self._get_qualified_name(source)
                    source_results[source_name] = results
                except Exception as e:
                    logger.error(f"Error loading data for source {source.product_id}: {str(e)}")
                    # In a production system, we might want to handle partial failures
                    raise

        return source_results

    def _get_source_data(self, source: QuerySource) -> List[Dict[str, Any]]:
        """
        Get all data from a single source.

        Args:
            source: Source configuration

        Returns:
            List of records from the source
        """
        try:
            # Dynamically import to avoid circular dependency
            from ..asset_identifier import AssetIdentifier

            identifier = AssetIdentifier.from_string(source.product_id)
            if not identifier.is_product():
                raise ValueError(f"Source identifier must be a data product: {source.product_id}")

            # Use a SELECT * query to get all data
            return self.asset_manager.query_product(
                identifier=identifier,
                query="SELECT * FROM source_data",
                port_id=source.port_id,
                server_key=source.server,
                model_key=source.model
            )
        except Exception as e:
            logger.error(f"Error getting source data: {str(e)}")
            raise

    def _execute_final_query(
        self,
        source_results: Dict[str, List[Dict[str, Any]]],
        query: str
    ) -> List[Dict[str, Any]]:
        """
        Execute the final query against the combined data sources.

        Args:
            source_results: Dictionary mapping source names to their data
            query: Query to execute against the combined data

        Returns:
            Query results
        """
        try:
            # Use DuckDB to execute the query against the combined data
            import duckdb

            con = duckdb.connect(":memory:")

            # Register each result set as a table
            for source_name, results in source_results.items():
                if not results:  # Skip empty results
                    logger.warning(f"No results from source {source_name}")
                    continue

                # Register as a view in DuckDB
                con.register(source_name, results)
                logger.debug(f"Registered source {source_name} with {len(results)} records")

            # Execute the query
            final_results = con.execute(query).fetchall()

            # Convert to list of dictionaries
            column_names = [desc[0] for desc in con.description]
            return [
                {column_names[i]: value for i, value in enumerate(row)}
                for row in final_results
            ]

        except Exception as e:
            logger.error(f"Error executing final query: {str(e)}")
            raise

    def get_capabilities(self) -> Dict[str, Any]:
        """
        Get capabilities of this query executor.

        Returns:
            Dictionary of capabilities
        """
        return {
            "supports_joins": True,
            "supports_filtering": True,
            "supports_aggregation": True,
            "supports_window_functions": True,
            "supports_subqueries": True,
            "max_sources": 10,  # Reasonable limit for most use cases
            "streaming": False,  # Current implementation loads all data into memory
        }

    @staticmethod
    def _get_qualified_name(source: QuerySource) -> str:
        """Get a qualified name for a source to use as a table name."""
        base = source.product_id.replace(':', '_').replace('/', '_')
        if source.port_id:
            base += f"_{source.port_id}"
        return base
