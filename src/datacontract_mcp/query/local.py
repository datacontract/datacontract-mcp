"""Query strategy for local file servers."""

import os
import logging
from typing import Dict, List, Any

from .base import QueryStrategy, create_duckdb_connection
from ..models_datacontract import ServerFormat

logger = logging.getLogger("datacontract-mcp.query.local")

# Get the data contracts source directory
datacontracts_source = os.getenv("DATACONTRACTS_SOURCE", "")


class LocalFileStrategy(QueryStrategy):
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
        format = server_config.get('format')
        
        if not path:
            raise ValueError("Local server must specify a 'path'")
        if not format:
            raise ValueError("Local server must specify a 'format'")
        
        # Choose the appropriate format handler
        handlers = {
            ServerFormat.CSV: self._handle_csv,
            # Add handlers for other formats as needed
        }
        
        handler = handlers.get(format)
        if not handler:
            raise ValueError(f"Unsupported format '{format}' for local server")
        
        return handler(model_key, query, path)
    
    def _handle_csv(self, model_key: str, query: str, path: str) -> List[Dict[str, Any]]:
        """Handle CSV format from local file system.
        
        Args:
            model_key: The name of the model to query
            query: The SQL query to execute
            path: The path to the CSV file
            
        Returns:
            A list of query result records
            
        Raises:
            FileNotFoundError: If the CSV file is not found
        """
        # Full path to the data file
        file_path = os.path.join(datacontracts_source, path)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Data file {file_path} not found")
        
        # Use the context manager for DuckDB connection
        with create_duckdb_connection() as conn:
            # Create a table for the model
            sql = f"""
            CREATE TABLE "{model_key}" AS
            SELECT * FROM read_csv(
                '{file_path}', 
                auto_type_candidates=['BIGINT','VARCHAR','BOOLEAN','DOUBLE']
            );
            """
            conn.execute(sql)
            
            # Execute the query
            df = conn.execute(query).fetchdf()
            
            # Convert to records and return
            return df.to_dict(orient="records")