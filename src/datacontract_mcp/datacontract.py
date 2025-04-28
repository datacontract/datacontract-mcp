"""Data Contract core functionality using Pydantic models."""

import logging

from typing import Dict, List, Optional, Any
from .models_datacontract import DataContract, QueryResult
from .query import get_query_strategy

from .asset_utils import (
    load_asset_file,
    parse_yaml_with_model,
    list_files_with_extension,
    AssetQueryError,
)

logger = logging.getLogger("datacontract-mcp.datacontract")


def load_contract_file(filename: str) -> str:
    """
    Load a data contract file from the configured source directory.

    Args:
        filename: Name of the data contract file

    Returns:
        File contents as string

    Raises:
        AssetLoadError: If the file is not found
    """
    return load_asset_file(filename)


def parse_contract(content: str) -> DataContract:
    """
    Parse a data contract string into a validated DataContract object.

    Args:
        content: Data contract YAML content

    Returns:
        Validated DataContract object

    Raises:
        AssetParseError: If contract is invalid or parsing fails
    """
    return parse_yaml_with_model(content, DataContract)


def get_contract(filename: str) -> DataContract:
    """
    Load and parse a data contract file.

    Args:
        filename: Name of the data contract file

    Returns:
        Validated DataContract object

    Raises:
        AssetLoadError: If the file is not found
        AssetParseError: If contract is invalid or parsing fails
    """
    content = load_contract_file(filename)
    return parse_contract(content)


def list_contract_files() -> List[str]:
    """
    List all available data contract files.

    Returns:
        List of filenames
    """
    return list_files_with_extension('.datacontract.yaml')


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
        AssetLoadError: If the file is not found
        AssetParseError: If contract is invalid
        AssetQueryError: If query execution fails
    """
    # Load and validate the contract using our Pydantic model
    contract = get_contract(filename)

    # Determine server to use
    if not (server_key := server_key or next(iter(contract.servers), None)):
        raise AssetQueryError("No servers defined in contract")
    
    if server_key not in contract.servers:
        raise AssetQueryError(f"Server '{server_key}' not found in contract")
    
    server = contract.servers[server_key]

    # Determine model to use
    if not (model_key := model_key or next(iter(contract.models), None)):
        raise AssetQueryError("No models defined in contract")
    
    if model_key not in contract.models:
        raise AssetQueryError(f"Model '{model_key}' not found in contract")

    try:
        # Get the appropriate query strategy and execute
        strategy = get_query_strategy(server.type)
        return strategy.execute(model_key, query, server.model_dump())
    except Exception as e:
        error_msg = f"Error executing query: {str(e)}"
        logger.error(error_msg)
        raise AssetQueryError(error_msg)


def query_contract(
    filename: str,
    query: str,
    server_key: Optional[str] = None,
    model_key: Optional[str] = None
) -> QueryResult:
    """
    Execute a query against a data contract and return structured results.

    Args:
        filename: Name of the data contract file
        query: SQL query to execute
        server_key: Optional key of the server to use
        model_key: Optional key of the model to use

    Returns:
        QueryResult object with records and metadata

    Raises:
        AssetLoadError: If the file is not found
        AssetParseError: If contract is invalid
        AssetQueryError: If query execution fails
    """
    # Get contract to determine defaults if needed
    contract = get_contract(filename)

    # Use defaults if not specified
    server_key = server_key or next(iter(contract.servers), None)
    model_key = model_key or next(iter(contract.models), None)

    # Execute query
    records = execute_query(
        filename=filename,
        query=query,
        server_key=server_key,
        model_key=model_key
    )

    # Return structured result
    return QueryResult(
        records=records,
        query=query,
        model_key=model_key,
        server_key=server_key
    )
