"""Data Contract core functionality."""

import duckdb
import logging
import os
import yaml

from pathlib import Path
from typing import Dict, List, Optional, Any

logger = logging.getLogger("datacontract-mcp.datacontract")

datacontracts_source = os.getenv("DATACONTRACTS_SOURCE", "")
if datacontracts_source == "":
    raise ValueError(f"DATACONTRACTS_SOURCE environment variable required. Working directory: {os.getcwd()}")


def load_contract_file(filename: str) -> str:
    """
    Load a data contract file from the configured source directory.

    Args:
        filename: Name of the data contract file

    Returns:
        File contents as string

    Raises:
        FileNotFoundError: If the file is not found
    """
    resource_path = Path(f"{datacontracts_source}/{filename}")

    if not resource_path.exists():
        raise FileNotFoundError(f"Data contract file {filename} not found at {resource_path}")

    with open(resource_path, "r", encoding="utf-8") as f:
        content = f.read()

    return content


def list_contract_files() -> List[str]:
    """
    List all available data contract files.

    Returns:
        List of filenames
    """
    files = []
    if not os.path.exists(datacontracts_source):
        logger.warning(f"Data contracts directory {datacontracts_source} does not exist")
        return files

    for fname in os.listdir(datacontracts_source):
        file_path = os.path.join(datacontracts_source, fname)
        if os.path.isfile(file_path) and fname.lower().endswith('.yaml'):
            files.append(fname)

    return files


def execute_query(
    filename: str,
    query: str,
    server_key: Optional[str] = None,
    model_key: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Execute a SQL query against a data contract's data source.

    Args:
        filename: Name of the data contract file
        query: SQL query to execute
        server_key: Optional key of the server to use
        model_key: Optional key of the model to use

    Returns:
        List of query result records

    Raises:
        ValueError: If contract is invalid or query execution fails
    """
    # Load and parse the contract file
    content = load_contract_file(filename)

    try:
        data_contract = yaml.safe_load(content)
    except Exception as e:
        raise ValueError(f"Error parsing YAML: {str(e)}")

    # Validate and extract server configuration
    if 'servers' not in data_contract:
        raise ValueError("Contract must have a 'servers' section")

    servers = data_contract['servers']
    if not isinstance(servers, dict) or len(servers) == 0:
        raise ValueError("'servers' section must be a non-empty dictionary")

    # Use provided server key or first available
    server_key = server_key or next(iter(servers))
    server = servers.get(server_key)
    if not server:
        raise ValueError(f"Server '{server_key}' not found in contract")

    # Get server configuration
    server_type = server.get('type')
    server_format = server.get('format')
    if not server_type:
        raise ValueError(f"Server '{server_key}' must specify a 'type'")
    if not server_format:
        raise ValueError(f"Server '{server_key}' must specify a 'format'")

    # Validate and extract model configuration
    if 'models' not in data_contract:
        raise ValueError("Contract must have a 'models' section")

    models = data_contract['models']
    if not isinstance(models, dict) or len(models) == 0:
        raise ValueError("'models' section must be a non-empty dictionary")

    # Use provided model key or first available
    model_key = model_key or next(iter(models))
    model = models.get(model_key)
    if not model:
        raise ValueError(f"Model '{model_key}' not found in contract")

    # Execute the query based on server type
    if server_type in ['file', 'local']:
        source = server.get('path')
        if not source:
            raise ValueError(f"Server '{server_key}' must specify a 'path'")

        if server_format == 'csv':
            # Full path to the data file
            path = os.path.join(datacontracts_source, source)
            if not os.path.exists(path):
                raise FileNotFoundError(f"Data file {path} not found")

            # Create in-memory DuckDB connection and load the CSV
            conn = duckdb.connect(database=':memory:')

            # Create a table for the model
            sql = f"""
            CREATE TABLE "{model_key}" AS
            SELECT * FROM read_csv(
                '{path}', 
                auto_type_candidates=['BIGINT','VARCHAR','BOOLEAN','DOUBLE']
            );
            """
            conn.execute(sql)

            # Execute the query
            df = conn.execute(query).fetchdf()

            # Convert to records
            return df.to_dict(orient="records")
        else:
            raise ValueError(f"Unsupported format '{server_format}' for {server_type} server")
    else:
        raise ValueError(f"Unsupported server type: {server_type}")
