# DataContract MCP Server

An MCP server for executing semantic queries against data contracts.

## Installation

### Using uv (recommended)

1. Install uv if you haven't already:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. Install the required dependencies:
```bash
uv pip install -r requirements.txt
```

### Using pip

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

## Running the Server

```bash
python server.py
```

## Usage

The server provides a `query` tool that can be used to execute semantic queries against data contracts.

Example query:
```python
{
    "data_contract_url": "example_contract.yaml",  # Can be local file or URL
    "query": "Show me recent high-value orders from premium customers",
    "filters": {
        "order_date": "last 30 days",
        "total_amount": "> 1000"
    }
}
```

The `data_contract_url` parameter supports:
- Local file paths (e.g., "example_contract.yaml")
- File URLs (e.g., "file:///path/to/contract.yaml")
- HTTP/HTTPS URLs (e.g., "https://example.com/contract.yaml")

## Development

To test the server locally:
```bash
mcp dev server.py
```

## Features

- Load and parse data contracts from URLs or local files
- Execute semantic queries against contract definitions
- Apply filters to query results
- Error handling and validation

## License

MIT