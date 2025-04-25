"""Data Contract core functionality using Pydantic models."""

import logging
import os
import yaml
from pathlib import Path
from typing import Dict, List, Optional, Any

# Import models
from .models_datacontract import DataContract, QueryResult

# Import query module
from .query import get_query_strategy

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


def parse_contract(content: str) -> DataContract:
    """
    Parse a data contract string into a validated DataContract object.

    Args:
        content: Data contract YAML content

    Returns:
        Validated DataContract object

    Raises:
        ValueError: If contract is invalid or parsing fails
    """
    try:
        # First parse with PyYAML to get the raw dictionary
        contract_dict = yaml.safe_load(content)

        # Then validate with Pydantic
        return DataContract.model_validate(contract_dict)

    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML: {str(e)}")
    except Exception as e:
        raise ValueError(f"Error validating data contract: {str(e)}")


def get_contract(filename: str) -> DataContract:
    """
    Load and parse a data contract file.

    Args:
        filename: Name of the data contract file

    Returns:
        Validated DataContract object

    Raises:
        FileNotFoundError: If the file is not found
        ValueError: If contract is invalid or parsing fails
    """
    content = load_contract_file(filename)
    return parse_contract(content)


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
    # Load and validate the contract using our Pydantic model
    contract = get_contract(filename)

    # Determine server to use
    server_key = server_key or next(iter(contract.servers))
    if server_key not in contract.servers:
        raise ValueError(f"Server '{server_key}' not found in contract")
    server = contract.servers[server_key]

    # Determine model to use
    model_key = model_key or next(iter(contract.models))
    if model_key not in contract.models:
        raise ValueError(f"Model '{model_key}' not found in contract")

    try:
        # Get the appropriate query strategy and execute
        strategy = get_query_strategy(server.type)
        return strategy.execute(model_key, query, server.model_dump())
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        raise ValueError(f"Error executing query: {str(e)}")


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
        ValueError: If contract is invalid or query execution fails
    """
    # Get contract to determine defaults if needed
    contract = get_contract(filename)

    # Use defaults if not specified
    server_key = server_key or next(iter(contract.servers))
    model_key = model_key or next(iter(contract.models))

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
