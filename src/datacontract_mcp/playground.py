import os
import json

# Set environment variables before importing the server
os.environ["DATACONTRACTS_SOURCE"] = "/Users/andre/git/datacontract/datacontract-mcp/datacontracts"

# Import directly from our core modules
from datacontract_mcp import datacontract
from datacontract_mcp.resources import docs

def run_demo():
    """Run a demonstration using the core functionality directly"""
    print("Getting schema...")
    schema = docs.get_datacontract_schema()
    print(f"Schema retrieved: {len(schema)} characters")
    
    print("\nListing datacontracts...")
    contracts = datacontract.list_contract_files()
    print(f"Available contracts: {contracts}")
    
    print("\nLoading a contract...")
    contract_content = datacontract.load_contract_file("video_history.datacontract.yaml")
    print(f"Contract size: {len(contract_content)} characters")
    
    print("\nParsing and validating contract with Pydantic...")
    contract = datacontract.get_contract("video_history.datacontract.yaml")
    print(f"Contract ID: {contract.id}")
    print(f"Contract Title: {contract.info.title}")
    print(f"Contract Models: {list(contract.models.keys())}")
    print(f"Contract Servers: {list(contract.servers.keys())}")
    
    # Show field information
    if "video_history" in contract.models:
        model = contract.models["video_history"]
        print(f"\nModel fields for 'video_history': {list(model.fields.keys())}")
        # Show details for one field
        if "timestamp_started" in model.fields:
            field = model.fields["timestamp_started"]
            print(f"Field 'timestamp_started': type={field.type}, required={field.required}")
    
    print("\nQuerying datacontract with full metadata...")
    query_result = datacontract.query_contract(
        filename="video_history.datacontract.yaml", 
        query="SELECT * FROM video_history LIMIT 2"
    )
    print(f"Query executed on model: {query_result.model_key}")
    print(f"Query executed on server: {query_result.server_key}")
    print(f"Query: {query_result.query}")
    print(f"Records: {json.dumps(query_result.records, indent=2)}")

def main():
    """Main entry point for playground"""
    run_demo()


if __name__ == "__main__":
    main()
