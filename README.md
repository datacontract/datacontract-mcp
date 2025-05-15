# Data Product MCP Server

An MCP server designed to manage Data Products - enabling AI-assists to discover discover and query data products in a governed way.

## Overview

The Data Contract MCP Server provides an AI-friendly interface to data products and their associated data contracts. It enables LLM agents to locate, analyze, and query data effectively while adhering to data contract specifications.

## Key Features

- **Asset Management**: Load and organize data products and contracts from diverse sources
- **Contract Compliance**: Ensure data usage complies with data contracts
- **Smart Data Querying**: Query data using natural language through various storage systems
- **Federated Queries (Alpha)**: Join data across multiple data products from different sources
- **Flexible Identification**: Support for various identifier formats (asset identifiers, URNs, plain IDs)
- **Local & Remote Support**: Work with local files or remote systems like Data Mesh Manager
- **Plugin Architecture**: Extensible plugin system for adding new asset sources and data sources

## Use Cases

- AI assistants exploring available data products
- Analyzing data contract schemas and requirements
- Executing SQL queries against data products
- Joining data across multiple products for complex analyses
- Validating data quality against contract specifications
- Building data-aware AI applications

## Installation

### Prerequisites

- Python 3.11 or higher
- [uv](https://astral.sh/uv) (recommended) or pip

### Quick Install

Using uv (recommended):
```bash
uv pip install -e .
```

Using pip:
```bash
pip install -e .
```

## Running the Server

### Basic Usage

```bash
# Set location of data files
export DATAASSET_SOURCE=/path/to/assets/directory

# Start the server
python -m src.dataproduct_mcp.server
```

### Development Mode

```bash
# For local development with MCP CLI
mcp dev -m src.dataproduct_mcp.server

# For debugging (runs test query)
python -m src.dataproduct_mcp.server

# For server mode
python -m src.dataproduct_mcp.server --server
```

### Integration with Claude Code or Claude Desktop

Add this configuration to your Claude installation:

```json
{
  "mcpServers": {
    "dataproduct": {
      "command": "uv",
      "args": ["run", "--directory", "<path_to_folder>/dataproduct-mcp", "python", "-m", "dataproduct_mcp.server"],
      "env": {
        "DATAASSET_SOURCE": "<path_to_folder>/dataproduct-mcp/examples"
      }
    }
  }
}
```

## Tools and Capabilities

### Data Product Operations

| Tool | Description |
|------|-------------|
| `dataproducts_list` | List all available data products |
| `dataproducts_get` | Retrieve a specific data product by identifier |
| `dataproducts_get_output_schema` | Get schema for a data contract linked to a product output port |
| `dataproducts_query` | Execute SQL queries against one or multiple data product output ports |

### Examples

#### List Available Data Products
```
dataproducts_list
```

#### Get a Specific Data Product
```
dataproducts_get(identifier="local:product/orders.dataproduct.yaml")
```
```
dataproducts_get(identifier="datameshmanager:product/customers")
```

#### Get Schema for a Data Contract
```
dataproducts_get_output_schema(identifier="local:contract/orders.datacontract.yaml")
```
```
dataproducts_get_output_schema(identifier="datameshmanager:contract/snowflake_customers_latest_npii_v1")
```

#### Query a Single Data Product
```
dataproducts_query(
    sources=[
        {
            "product_id": "local:product/orders.dataproduct.yaml"
        }
    ],
    query="SELECT * FROM orders LIMIT 10;",
    include_metadata=True
)
```

#### Execute a Federated Query (Alpha)
```
dataproducts_query(
    sources=[
        {
            "product_id": "local:product/orders.dataproduct.yaml",
            "alias": "orders"
        },
        {
            "product_id": "local:product/video_history.dataproduct.yaml", 
            "alias": "videos"
        }
    ],
    query="SELECT o.customer_id, v.video_id, o.order_date, v.view_date FROM orders o JOIN videos v ON o.customer_id = v.user_id WHERE o.order_date > '2023-01-01'",
    include_metadata=True
)
```

**Note**: Querying multiple data products at once is still in alpha status and may have limitations.

## Included Examples

The `examples` directory contains sample files for both data products and their supporting data contracts:

### Sample Data Products
- `orders.dataproduct.yaml` - Customer order data
- `shelf_warmers.dataproduct.yaml` - Products with no sales in 3+ months
- `video_history.dataproduct.yaml` - Video viewing history data

### Sample Data Contracts
- `orders.datacontract.yaml` - Defines order data structure and rules
- `shelf_warmers.datacontract.yaml` - Contract for product inventory analysis
- `video_history.datacontract.yaml` - Specifications for video consumption data

## Architecture

The Data Contract MCP Server uses a plugin-based architecture that cleanly separates:

### Asset Sources (Metadata)
These plugins handle loading and listing data assets (products and contracts):
- **Local files**: Loads YAML files from a local directory
- **Data Mesh Manager**: Fetches assets from a Data Mesh Manager API

### Data Sources (Query Execution)
These plugins handle the actual data querying:
- **Local files**: Queries CSV, JSON, or Parquet files using DuckDB
- **S3**: Queries data in S3 buckets using DuckDB's S3 integration
- **Databricks**: Queries data from Databricks SQL warehouses

The plugin architecture makes it easy to add support for additional asset sources (e.g., Git repositories, other APIs) and data sources (e.g., databases, data warehouses).

## Configuration Options

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATAASSET_SOURCE` | Directory containing data assets | Current directory |
| `DATAMESH_MANAGER_API_KEY` | API key for Data Mesh Manager | None |
| `DATAMESH_MANAGER_HOST` | Host URL for Data Mesh Manager | `https://api.datamesh-manager.com` |

### AWS S3 Configuration (for S3 data sources)

- `AWS_REGION` / `AWS_DEFAULT_REGION` - AWS region (default: `us-east-1`)
- `S3_BUCKETS` - Allowed S3 buckets (comma-separated)
- Authentication via profile (`AWS_PROFILE`) or credentials (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`)

### Databricks Configuration (for Databricks data sources)

- `DATABRICKS_WORKSPACE_URL` - Databricks workspace URL (required)
- `DATABRICKS_TOKEN` - Personal access token for Databricks
- `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` - OAuth client credentials (alternative to token)
- `DATABRICKS_CATALOG` - Default catalog to use (optional)
- `DATABRICKS_SCHEMA` - Default schema to use (optional)
- `DATABRICKS_TIMEOUT` - Query execution timeout in seconds (default: 120)

## Development Setup

Python base interpreter should be 3.11.x.

```bash
# create venv
python3.11 -m venv venv
source venv/bin/activate

# Install Requirements
pip install --upgrade pip setuptools wheel
pip install -e '.[dev]'
pre-commit install
pre-commit run --all-files
pytest
```

### Use uv (recommended)

```bash
# make sure uv is installed
uv python pin 3.11
uv pip install -e '.[dev]'
uv run ruff check
uv run pytest
```

## Contribution

We are happy to receive your contributions. Propose your change in an issue or directly create a pull request with your improvements.

## Related Tools

- [Data Contract CLI](https://github.com/datacontract/datacontract-cli/) is an open-source command-line tool for working with data contracts.
- [Data Contract Manager](https://www.datacontract-manager.com/) is a commercial tool to manage data contracts. It contains a web UI, access management, and data governance for a full enterprise data marketplace.
- [Data Contract GPT](https://gpt.datacontract.com) is a custom GPT that can help you write data contracts.
- [Data Contract Editor](https://editor.datacontract.com) is an editor for Data Contracts, including a live html preview.
- [Data Contract Playground](https://data-catering.github.io/data-contract-playground/) allows you to validate and export your data contract to different formats within your browser.

## License

[MIT License](LICENSE)
