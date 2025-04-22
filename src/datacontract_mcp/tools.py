import duckdb
import json
import logging
import os
import yaml

from collections.abc import Sequence
from mcp.types import (
    EmbeddedResource,
    ImageContent,
    TextContent,
    Tool,
)
from pathlib import Path
from .resources import docs

logger = logging.getLogger("datacontract-mcp-server.resources.docs")

datacontracts_source = os.getenv("DATACONTRACTS_SOURCE", "")
if datacontracts_source == "":
    raise ValueError(f"DATACONTRACTS_SOURCE environment variable required. Working directory: {os.getcwd()}")

class ToolHandler():
    def __init__(self, tool_name: str):
        self.name = tool_name

    def get_tool_description(self) -> Tool:
        raise NotImplementedError()

    def run_tool(self, args: dict) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
        raise NotImplementedError()

class GetDataContractSchema(ToolHandler):
    def __init__(self):
        super().__init__('datacontracts_get_schema')

    def get_tool_description(self):
        return Tool(
            name=self.name,
            description="Get the Data Contract schema.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            },
        )

    def run_tool(self, args: dict) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
        datacontract_schema = docs.get_datacontract_schema()

        return [
            TextContent(
                type="text",
                text=json.dumps(datacontract_schema)
            )
        ]

class ListDataContracts(ToolHandler):
    def __init__(self):
        super().__init__('datacontracts_list_datacontracts')

    def get_tool_description(self):
        return Tool(
            name=self.name,
            description="Lists all available Data Contracts (by filename).",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            },
        )

    def run_tool(self, args: dict) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
        files = []

        datacontracts_dir = 'datacontracts'
        for fname in os.listdir(datacontracts_dir):
            if os.path.isfile(os.path.join(datacontracts_dir, fname)) and fname.lower().endswith('yaml'):
                files.append(fname)

        return [
            TextContent(
                type="text",
                text=json.dumps(files)
            )
        ]

class GetDataContract(ToolHandler):
    def __init__(self):
        super().__init__("datacontracts_get_datacontract")

    def get_tool_description(self):
        return Tool(
            name=self.name,
            description="Return the content of a single Data Contract.",
            inputSchema={
                "type": "object",
                "properties": {
                    "filename": {
                        "type": "string",
                        "description": "Filename of the Data Contract to retrieve."
                    },
                },
                "required": ["filename"]
            }
        )

    def run_tool(self, args: dict) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
        if "filename" not in args:
            raise RuntimeError("filename argument missing in arguments")

        content = _load_resource(args["filename"])

        return [
            TextContent(
                type="text",
                text=json.dumps(content)
            )
        ]

class QueryDataContract(ToolHandler):
    def __init__(self):
        super().__init__("datacontracts_query_datacontract")

    def get_tool_description(self):
        return Tool(
            name=self.name,
            description="Execute a ready-only query against source defined in a Data Contract YAML",
            inputSchema={
                "type": "object",
                "properties": {
                    "filename": {
                        "type": "string",
                        "description": "Filename of the Data Contract to retrieve."
                    },
                    "server": {
                        "type": "string",
                        "description": "The key of the server specified in the Data Contract which should be used to execute the query. If not specified, the first server will be used."
                    },
                    "model": {
                        "type": "string",
                        "description": "The key of the model specified in the Data Contract which should be used when executing the query. If not specified, the first model will be used."
                    },
                    "query": {
                        "type": "string",
                        "description": "The query in the specified language in the Data Contract which should be executed."
                    },
                },
                "required": ["filename", "query"]
            }
        )

    def run_tool(self, args: dict) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
        if "filename" not in args or "query" not in args:
            raise RuntimeError("filename and query arguments required")

        query = args["query"]
        content = _load_resource(args["filename"])

        try:
            data_contract = yaml.safe_load(content)
        except Exception as e:
            raise Exception(f"Error loading YAML: {str(e)}")

        # Extract server configuration
        if 'servers' not in data_contract:
            raise ValueError("Contract must have a 'servers' section")

        servers = data_contract['servers']
        if not isinstance(servers, dict):
            raise ValueError("'servers' section must be a dictionary with server keys as keys")

        server_key = args.get('server', next(iter(servers)))

        # Find the server by key
        server = servers.get(server_key)
        if not server:
            raise ValueError(f"Server with key '{server_key}' not found in contract")

        # Get server type and format
        server_type = server.get('type')
        if not server_type:
            raise ValueError(f"Server '{server_key}' must specify a 'type'")

        server_format = server.get('format')
        if not server_format:
            raise ValueError(f"Server '{server_key}' must specify a 'format'")

        models = data_contract['models']
        if not isinstance(models, dict):
            raise ValueError("'models' section must be a dictionary with models keys as keys")

        model_key = args.get('model', next(iter(models)))

        # Find the model by key
        model = models.get(model_key)
        if not model:
            raise ValueError(f"Model with key '{model_key}' not found in contract")

        # Handle different server types
        if server_type in ['file', 'local']:  # Support both 'file' and 'local' types
            source = server.get('path')
            if not source:
                raise ValueError(f"Server '{server_key}' must specify a 'path'")

            # Load data based on format
            if server_format == 'csv':
                path = os.path.join(datacontracts_source, source)
                sql = f"""
                CREATE TABLE "{model_key}" AS
                SELECT *
                FROM read_csv('{path}', auto_type_candidates=['BIGINT','VARCHAR','BOOLEAN','DOUBLE']);
                """

                # Use DuckDB to read CSV
                conn = duckdb.connect(database=':memory:')
                conn.execute(sql)

                # Build WHERE clause from filters
                where_clause = ""

                # Execute query
                df = conn.execute(f"{query} {where_clause}").fetchdf()
            else:
                raise ValueError(f"Unsupported format '{format}' for {server_type} server")

        else:
            raise ValueError(f"Unsupported server type: {server_type}")

        records = df.to_dict(orient="records")

        # Serialize to JSON
        payload = json.dumps(records)

        # Return a single TextContent with application/json in the text field
        return [
            TextContent(
                type="text",       # literal "text"
                text=payload       # a JSON string
            )
        ]

def _load_resource(filename: str) -> str:
    """
    Load a resource file.

    Args:
        filename: Resource filename

    Returns:
        File contents as string
    """

    try:
        # Try to load from package resources
        resource_path = Path(f"{datacontracts_source}/{filename}")

        if resource_path.exists():
            with open(resource_path, "r", encoding="utf-8") as f:
                content = f.read()
        else:
            raise FileNotFoundError(f"Resource {filename} not found")

        return content

    except Exception as e:
        logger.error(f"Error loading resource {filename}: {str(e)}")
        # Return fallback if loading fails
        return ""
