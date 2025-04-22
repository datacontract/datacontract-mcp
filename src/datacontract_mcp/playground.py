import os

os.environ["DATACONTRACTS_SOURCE"] = "/Users/andre/git/datacontract/datacontract-mcp/datacontracts"

from datacontract_mcp.tools import QueryDataContract, GetDataContractSchema

def main():
    # schema_tool = GetDataContractSchema()
    # schema = schema_tool.run_tool({})

    args = {"filename": "video_history.datacontract.yaml", "query": "SELECT * FROM video_history LIMIT 5"}
    query_tool = QueryDataContract()
    query_tool.run_tool(args)


if __name__ == "__main__":
    main()
