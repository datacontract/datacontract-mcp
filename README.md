# DataContract/DataProduct MCP Server

An MCP server for working with Data Contracts and Data Products - loading, validating, querying, and analyzing definitions.

## Installation

### Prerequisites

- Python 3.10 or higher (as specified in pyproject.toml)
- [uv](https://astral.sh/uv) (recommended)

See pyproject.toml for the full list of dependencies.

### Using uv (recommended)

```bash
uv pip install -e .
```

### Using pip

```bash
pip install -e .
```

## Running the Server

```bash
# Set environment variables
export DATACONTRACTS_SOURCE=/path/to/datacontracts/directory
export DATAPRODUCTS_SOURCE=/path/to/dataproducts/directory

# Run server directly
python -m datacontract_mcp.server

# Or use MCP CLI for development
mcp dev -m datacontract_mcp.server
```

## Adding to Claude Desktop (for local development)

Add this to your Claude Desktop configuration:

```json
{
  "mcpServers": {
    "datacontract": {
      "command": "uv",
      "args": ["run", "--directory", "<abs_repo_path>", "python", "-m", "datacontract_mcp.server"],
      "env": {
        "DATACONTRACTS_SOURCE": "<abs_repo_path>/datacontracts",
        "DATAPRODUCTS_SOURCE": "<abs_repo_path>/dataproducts"
      }
    }
  }
}
```

## Available Tools

### Data Contract Tools

### Get Data Contract Schema

Retrieves the official Data Contract JSON schema:

```
datacontracts_get_schema
```

### List Data Contracts

Lists all available Data Contracts in the datacontracts directory:

```
datacontracts_list_datacontracts
```

### Get Data Contract

Retrieves the content of a specific Data Contract:

```
datacontracts_get_datacontract(filename="orders.datacontract.yaml")
```

### Validate Data Contract

Validates a Data Contract and returns its structured representation:

```
datacontracts_validate(filename="orders.datacontract.yaml")
```

### Query Data Contract

Executes a read-only SQL query against data defined in a Data Contract:

```
datacontracts_query_datacontract(
    filename="orders.datacontract.yaml",
    query="SELECT * FROM \"orders\" LIMIT 10"
)
```

To include metadata (like server and model information) in the results, use the `include_metadata` parameter:

```
datacontracts_query_datacontract(
    filename="orders.datacontract.yaml", 
    query="SELECT * FROM \"orders\" LIMIT 10",
    server="local",   # Optional server key
    model="orders",   # Optional model key
    include_metadata=True
)
```

### Data Product Tools

### Get Data Product Schema

Retrieves the official Data Product JSON schema:

```
dataproducts_get_schema
```

### Get Data Product Example

Retrieves an example Data Product:

```
dataproducts_get_example
```

### List Data Products

Lists all available Data Products in the dataproducts directory:

```
dataproducts_list_dataproducts
```

### Get Data Product

Retrieves the content of a specific Data Product:

```
dataproducts_get_dataproduct(filename="shelf_warmers.dataproduct.yaml")
```

### Validate Data Product

Validates a Data Product and returns its structured representation:

```
dataproducts_validate(filename="shelf_warmers.dataproduct.yaml")
```

## Available Resources

The server provides access to these resources:

- `datacontract-ref://schema` - The official Data Contract JSON schema
- `datacontract-ref://example` - A concrete example Data Contract from the retail domain
- `dataproduct-ref://schema` - The official Data Product JSON schema
- `dataproduct-ref://example` - A concrete example Data Product

## Included Examples

### Data Contracts

The repository includes example data contracts for:

- `orders.datacontract.yaml` - Sample order data with CSV source
- `video_history.datacontract.yaml` - Sample video watching history with CSV source

### Data Products

The repository includes example data products for:

- `shelf_warmers.dataproduct.yaml` - Sample data product for products not selling for 3 months

## Development

### Environment Variables

- `DATACONTRACTS_SOURCE` - Directory containing Data Contract files (required)
- `DATAPRODUCTS_SOURCE` - Directory containing Data Product files (required)

### Testing Locally

```bash
DATACONTRACTS_SOURCE=<abs_repo_path>/datacontracts DATAPRODUCTS_SOURCE=<abs_repo_path>/dataproducts mcp dev -m datacontract_mcp.server
```

## License

MIT
