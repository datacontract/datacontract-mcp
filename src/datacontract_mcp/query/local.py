"""Query strategy for local file servers."""

import os
import logging
from typing import Dict, List, Any, Optional

from .base import DataQueryStrategy, create_duckdb_connection
from ..models_datacontract import ServerFormat

logger = logging.getLogger("datacontract-mcp.query.local")

# Get the data contracts source directory
datacontracts_source = os.getenv("DATAASSET_SOURCE", "")


class LocalFileQueryStrategy(DataQueryStrategy):
    """Strategy for querying local files."""

    def execute(self, model_key: str, query: str, server_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute the query on local files.

        Args:
            model_key: The name of the model to query
            query: The SQL query to execute
            server_config: The server configuration

        Returns:
            A list of query result records

        Raises:
            ValueError: If the server configuration is invalid
            FileNotFoundError: If the data file is not found
        """
        path = server_config.get('path')
        format_value = server_config.get('format')

        if not path:
            raise ValueError("Local server must specify a 'path'")
        if not format_value:
            raise ValueError("Local server must specify a 'format'")

        # Normalize format to ServerFormat enum
        format_enum = self.normalize_format(format_value)
        if not format_enum:
            raise ValueError(f"Unsupported format '{format_value}' for local server")

        # Get DuckDB import statement based on format
        duckdb_import = self._get_duckdb_import_statement(format_enum)
        if not duckdb_import:
            raise ValueError(f"No handler available for format '{format_enum}'")

        # Execute the query using the generic handler
        return self._execute_query(model_key, query, path, duckdb_import)

    # Use normalize_format from base class

    def _get_duckdb_import_statement(self, format_enum: ServerFormat) -> Optional[str]:
        """Get the appropriate DuckDB import statement for a format.

        Args:
            format_enum: The normalized format enum

        Returns:
            DuckDB SQL import statement or None if unsupported
        """
        # Map of formats to their DuckDB import statements
        format_handlers = {
            ServerFormat.CSV: """
                SELECT * FROM read_csv(
                    '{file_path}', 
                    auto_type_candidates=['BIGINT','VARCHAR','BOOLEAN','DOUBLE']
                )
            """,
            ServerFormat.JSON: """
                SELECT * FROM read_json(
                    '{file_path}', 
                    auto_detect=TRUE
                )
            """,
            ServerFormat.PARQUET: """
                SELECT * FROM read_parquet('{file_path}')
            """,
            # DELTA format not yet implemented
            ServerFormat.DELTA: None
        }

        return format_handlers.get(format_enum)

    def _execute_query(self, model_key: str, query: str, path: str,
                       duckdb_import: str) -> List[Dict[str, Any]]:
        """Execute a query using DuckDB with the given import statement.

        Args:
            model_key: The name to use for the model/table
            query: The SQL query to execute
            path: Path to the data file
            duckdb_import: DuckDB import statement with {file_path} placeholder

        Returns:
            Query results as list of dictionaries

        Raises:
            FileNotFoundError: If the data file doesn't exist
        """
        # Check if path is absolute or relative
        if os.path.isabs(path):
            file_path = path
        else:
            # Get full path by joining with datacontracts_source
            file_path = os.path.join(datacontracts_source, path)

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Data file {file_path} not found")

        # Use the context manager for DuckDB connection
        with create_duckdb_connection() as conn:
            # Format the import statement with the file path
            formatted_import = duckdb_import.format(file_path=file_path)

            # Create a table for the model
            create_table_sql = f'CREATE TABLE "{model_key}" AS {formatted_import};'
            conn.execute(create_table_sql)

            # Execute the query and convert results to records
            df = conn.execute(query).fetchdf()
            return df.to_dict(orient="records")
