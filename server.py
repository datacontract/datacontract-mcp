from typing import Dict, Any, List
import yaml
from pydantic import BaseModel
from mcp.server.fastmcp import FastMCP
import requests
from urllib.parse import urlparse
import os
import duckdb
import pandas as pd
import boto3
from botocore.exceptions import ClientError

# Create an MCP server
mcp = FastMCP("DataContract")

def load_yaml_from_url(url: str, parse_yaml: bool = True) -> str | Dict[str, Any]:
    """
    Load YAML content from a URL or local file path.
    
    Args:
        url: URL or path to the YAML file
        parse_yaml: If True, parse the YAML into a dictionary. If False, return raw content.
        
    Returns:
        Either the parsed YAML dictionary or raw YAML content as a string
    """
    try:
        parsed_url = urlparse(url)
        
        if parsed_url.scheme in ('http', 'https'):
            # Load from HTTP/HTTPS URL
            response = requests.get(url)
            response.raise_for_status()
            content = response.text
        elif parsed_url.scheme == 'file' or not parsed_url.scheme:
            # Load from local file
            file_path = parsed_url.path if parsed_url.scheme == 'file' else url
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")
            with open(file_path, 'r') as f:
                content = f.read()
        else:
            raise ValueError(f"Unsupported URL scheme: {parsed_url.scheme}")
            
        return yaml.safe_load(content) if parse_yaml else content
    except Exception as e:
        raise Exception(f"Error loading YAML: {str(e)}")

@mcp.tool()
async def get_data_contract_yaml(data_contract_url: str) -> str:
    """
    Get the raw YAML content of a data contract.
    
    Args:
        data_contract_url: URL or path to the data contract YAML file
        
    Returns:
        Raw YAML content as a string
    """
    return load_yaml_from_url(data_contract_url, parse_yaml=False)

def load_data_from_s3(s3_path: str) -> pd.DataFrame:
    """
    Load data from an S3 location.
    
    Args:
        s3_path: S3 path in format s3://bucket/path/to/file
        
    Returns:
        DataFrame containing the loaded data
    """
    try:
        # Parse S3 path
        parsed = urlparse(s3_path)
        if parsed.scheme != 's3':
            raise ValueError(f"Invalid S3 path: {s3_path}")
            
        bucket = parsed.netloc
        key = parsed.path.lstrip('/')
        
        # Initialize S3 client
        s3 = boto3.client('s3')
        
        # Get object
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        
        # Load into DataFrame based on file extension
        if key.endswith('.json'):
            return pd.read_json(content, lines=True)
        elif key.endswith('.csv'):
            return pd.read_csv(content)
        else:
            raise ValueError(f"Unsupported file format in S3 path: {s3_path}")
            
    except ClientError as e:
        raise Exception(f"Error accessing S3: {str(e)}")

@mcp.tool()
async def query(
    data_contract_url: str,
    server_key: str,
    query: str,
    filters: Dict[str, Any] = {}
) -> Dict[str, Any]:
    """
    Execute a read-only query against a server, as defined in the original data contract YAML.
    The query must have the dialect of the server technology, e.g. SQL for snowflake, duckdb, postgres, etc.
    
    Args:
        data_contract_url: URL or path to the data contract YAML file
        server_key: Key of the server to query (e.g., 'production')
        query: Query describing what data to retrieve
        filters: Optional filters to apply to the query
        
    Returns:
        Dictionary containing the query results
    """
    try:
        # Load the data contract
        contract = load_yaml_from_url(data_contract_url, parse_yaml=True)
        
        # Extract server configuration
        if 'servers' not in contract:
            raise ValueError("Contract must have a 'servers' section")
            
        servers = contract['servers']
        if not isinstance(servers, dict):
            raise ValueError("'servers' section must be a dictionary with server keys as keys")
            
        # Find the server by key
        server = servers.get(server_key)
        if not server:
            raise ValueError(f"Server with key '{server_key}' not found in contract")
            
        # Get server type and format
        server_type = server.get('type')
        if not server_type:
            raise ValueError(f"Server '{server_key}' must specify a 'type'")
            
        format = server.get('format')
        if not format:
            raise ValueError(f"Server '{server_key}' must specify a 'format'")
            
        # Handle different server types
        if server_type == 's3':
            source = server.get('location')
            if not source:
                raise ValueError(f"Server '{server_key}' must specify a 'location'")
                
            # Load data from S3
            df = load_data_from_s3(source)
            
        elif server_type in ['file', 'local']:  # Support both 'file' and 'local' types
            source = server.get('path')
            if not source:
                raise ValueError(f"Server '{server_key}' must specify a 'path'")
                
            # Load data based on format
            if format == 'csv':
                # Use DuckDB to read CSV
                conn = duckdb.connect(database=':memory:')
                conn.execute(f"CREATE TABLE data AS SELECT * FROM read_csv('{source}', auto_type_candidates = ['BIGINT', 'VARCHAR', 'BOOLEAN', 'DOUBLE'])")
                
                # Build WHERE clause from filters
                where_clause = ""
                if filters:
                    conditions = []
                    for key, value in filters.items():
                        if isinstance(value, str):
                            conditions.append(f"{key} = '{value}'")
                        else:
                            conditions.append(f"{key} = {value}")
                    where_clause = "WHERE " + " AND ".join(conditions)
                
                # Execute query
                result = conn.execute(f"{query} {where_clause}").fetchdf()
                df = result
                
            elif format == 'json':
                df = pd.read_json(source)
            else:
                raise ValueError(f"Unsupported format '{format}' for {server_type} server")
            
        else:
            raise ValueError(f"Unsupported server type: {server_type}")
            
        # Convert result to dictionary format
        return {
            "status": "success",
            "data": df.to_dict(orient='records'),
            "query": query,
            "filters": filters,
            "server_key": server_key,
            "server_type": server_type,
            "format": format
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }

if __name__ == "__main__":
    mcp.run() 
