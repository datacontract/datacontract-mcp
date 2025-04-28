# DataProduct MCP Server

A server that helps you work with Data Products - making it easy to load, validate, query, and analyze your data assets, including their linked data contracts.

## What It Does

This MCP server provides a simple interface for working with Data Products:

- Loads and validates data products
- Analyzes linked data contracts within data products
- Provides tools to query and analyze your data
- Recognizes files automatically by their extensions (`.dataproduct.yaml` and `.datacontract.yaml`)
- Handles data assets in a consistent way

## Installation

### Prerequisites

- Python 3.10 or higher
- [uv](https://astral.sh/uv) (recommended) or pip

### Quick Install

With uv (recommended):
```bash
uv pip install -e .
```

With pip:
```bash
pip install -e .
```

## Running the Server

Set up where your data files are stored:

```bash
# Point to your data files
export DATAASSET_SOURCE=/path/to/assets/directory

# Run the server
python -m datacontract_mcp.server

# For development with MCP CLI
mcp dev -m datacontract_mcp.server
```

## Using with Claude Desktop

Add this configuration for local development:

```json
{
  "mcpServers": {
    "datacontract": {
      "command": "uv",
      "args": ["run", "--directory", "<abs_repo_path>", "python", "-m", "datacontract_mcp.server"],
      "env": {
        "DATAASSET_SOURCE": "<abs_repo_path>/dataassets"
      }
    }
  }
}
```

## Available Tools

### Data Product Tools

#### List Data Products

See all available Data Products:

```
dataproducts_list
```

#### Get Data Product

Get a specific Data Product:

```
dataproducts_get(filename="shelf_warmers.dataproduct.yaml")
```

#### Validate Data Product

Check if a Data Product is valid:

```
dataproducts_validate(filename="shelf_warmers.dataproduct.yaml")
```

#### Get Data Product Outputs

Get all output ports from a Data Product, along with their linked contracts:

```
dataproducts_get_outputs(filename="shelf_warmers.dataproduct.yaml")
```

#### Get Output Schema

Get the schema for a specific output port using its linked data contract:

```
dataproducts_get_output_schema(
    filename="shelf_warmers.dataproduct.yaml",
    port_id="shelf_warmers"
)
```

#### Query Data Product

Query data from a Data Product's output port:

```
dataproducts_query(
    filename="shelf_warmers.dataproduct.yaml",
    query="SELECT * FROM \"shelf_warmers\" LIMIT 10"
)
```

With additional options:

```
dataproducts_query(
    filename="shelf_warmers.dataproduct.yaml", 
    query="SELECT * FROM \"shelf_warmers\" LIMIT 10",
    port_id="shelf_warmers",   # Optional output port ID
    server="local",            # Optional server key
    model="shelf_warmers",     # Optional model key
    include_metadata=True
)
```

## Included Examples

The `examples` directory contains sample files for both data products and their supporting data contracts:

### Data Products

- `examples/shelf_warmers.dataproduct.yaml` - Shows which products haven't sold in 3 months
- `examples/orders.dataproduct.yaml` - Order data with order details
- `examples/video_history.dataproduct.yaml` - Video watching history

### Supporting Data Contracts

- `examples/shelf_warmers.datacontract.yaml` - Contract for shelf warmers data
- `examples/orders.datacontract.yaml` - Sample order data contract (CSV format)
- `examples/video_history.datacontract.yaml` - Video history data contract (CSV format)

## Available Resources

Access these built-in resources:

- `dataproduct-ref://schema` - The official Data Product JSON schema
- `dataproduct-ref://example` - A sample Data Product

## Configuration

### Environment Variables

- `DATAASSET_SOURCE` - Where to find your data assets (both products and contracts)

### AWS S3 Configuration

To access data from S3:

- `AWS_REGION` or `AWS_DEFAULT_REGION` - AWS region (default: `us-east-1`)
- `S3_BUCKETS` - List of allowed S3 buckets (comma-separated)
- `S3_MAX_BUCKETS` - Maximum number of buckets (default: 10)

Authentication options:
1. AWS Profile (for development): `AWS_PROFILE` or `S3_PROFILE`
2. Direct credentials: `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`
3. Default chain (environment variables, ~/.aws/credentials, IAM roles)

### Testing Locally

```bash
DATAASSET_SOURCE=<abs_repo_path>/dataassets mcp dev -m datacontract_mcp.server
```

## License

MIT
