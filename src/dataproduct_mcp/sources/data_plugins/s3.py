"""S3 data source plugin for querying data from AWS S3."""

import logging
import os
from typing import Dict, List, Any, Set

from ..data_source import DataSourcePlugin, ServerType

logger = logging.getLogger("dataproduct-mcp.sources.data_plugins.s3")


def _get_env_region() -> str:
    """Get AWS region from environment variables."""
    return os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))


def _get_env_buckets() -> Set[str]:
    """Get allowed bucket names from environment variables."""
    buckets_str = os.getenv("DATACONTRACT_S3_ALLOWED_BUCKETS", "")
    if not buckets_str:
        return set()
    return {bucket.strip() for bucket in buckets_str.split(",")}


def _get_env_max_buckets() -> int:
    """Get maximum number of buckets from environment variables."""
    try:
        return int(os.getenv("DATACONTRACT_S3_MAX_BUCKETS", "10"))
    except ValueError:
        return 10


def _get_env_credentials() -> Dict[str, Any]:
    """Get AWS credentials from environment variables."""
    return {
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "aws_session_token": os.getenv("AWS_SESSION_TOKEN"),
    }


@DataSourcePlugin.register(ServerType.S3)
class S3DataSource(DataSourcePlugin):
    """Plugin for querying data from AWS S3."""

    def __init__(self):
        """Initialize the S3 data source plugin."""
        self._region = _get_env_region()
        self._allowed_buckets = _get_env_buckets()
        self._max_buckets = _get_env_max_buckets()
        self._credentials = _get_env_credentials()
        self._endpoint_url = os.getenv("AWS_ENDPOINT_URL")

    @property
    def server_type(self) -> str:
        """The server type this plugin supports."""
        return ServerType.S3

    def execute(self, model_key: str, query: str, server_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute a query against S3 data.

        Args:
            model_key: The name of the model or table to query
            query: SQL query to execute
            server_config: Server configuration with bucket, path, format, etc.

        Returns:
            List of records as dictionaries

        Raises:
            ImportError: If required dependencies are not installed
            Exception: For any other errors during query execution
        """
        # Get bucket and path from server config
        bucket = server_config.get("bucket")
        path = server_config.get("path") or server_config.get("location") or server_config.get("key")

        if not bucket:
            raise ValueError("No bucket provided in server configuration")

        if not path:
            raise ValueError("No path provided in server configuration")

        # Check if bucket is allowed
        if self._allowed_buckets and bucket not in self._allowed_buckets:
            raise ValueError(f"Bucket '{bucket}' is not in the allowed buckets list")

        # Construct S3 URI
        s3_uri = f"s3://{bucket}/{path}"

        # Determine file format
        file_format = self._determine_file_format(path, server_config)

        # Execute the query using DuckDB with S3 integration
        return self._execute_duckdb_s3_query(s3_uri, file_format, model_key, query, server_config)

    def _determine_file_format(self, path: str, server_config: Dict[str, Any]) -> str:
        """Determine the format of a file based on its extension or configuration.

        Args:
            path: Path to the file in S3
            server_config: Server configuration

        Returns:
            File format string (e.g., 'csv', 'parquet')
        """
        # Check if format is explicitly specified in the config
        if format_str := server_config.get("format"):
            return format_str.lower()

        # Otherwise, infer from file extension
        extension = os.path.splitext(path)[1].lower()

        if extension in ['.csv', '.txt', '.tsv']:
            return 'csv'
        elif extension == '.parquet':
            return 'parquet'
        elif extension in ['.json', '.jsonl']:
            return 'json'
        elif extension == '.avro':
            return 'avro'
        elif extension == '.orc':
            return 'orc'
        else:
            # Default to Parquet if unknown
            logger.warning(f"Unknown file extension: {extension}, defaulting to Parquet")
            return 'parquet'

    def _execute_duckdb_s3_query(self, s3_uri: str, file_format: str, model_key: str, query: str,
                                server_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute a query against S3 data using DuckDB.

        Args:
            s3_uri: S3 URI of the file
            file_format: Format of the file
            model_key: Name to use for the table
            query: SQL query to execute
            server_config: Server configuration with additional options

        Returns:
            List of records as dictionaries
        """
        try:
            import duckdb

            # Create a new connection
            conn = duckdb.connect(database=":memory:")

            try:
                # Install httpfs extension if needed
                conn.install_extension("httpfs")
                conn.load_extension("httpfs")

                # Set AWS credentials
                self._set_s3_credentials(conn, server_config)

                # Create a table from the S3 file
                table_query = self._create_table_query(s3_uri, file_format, model_key)
                conn.execute(table_query)

                # Execute the query
                result = conn.execute(query)

                # Convert to list of dictionaries
                column_names = [col[0] for col in result.description]
                records = []

                for row in result.fetchall():
                    record = {}
                    for i, column in enumerate(column_names):
                        record[column] = row[i]
                    records.append(record)

                return records
            finally:
                # Close the connection
                conn.close()
        except ImportError as e:
            logger.error(f"Error importing duckdb: {e}")
            raise ImportError("DuckDB is required for S3 data querying. "
                            "Install with: pip install duckdb")
        except Exception as e:
            logger.error(f"Error executing S3 query: {e}")
            raise

    def _set_s3_credentials(self, conn: Any, server_config: Dict[str, Any]) -> None:
        """Set AWS credentials for DuckDB connection.

        Args:
            conn: DuckDB connection
            server_config: Server configuration
        """
        # Use credentials from server config if available, otherwise use default credentials
        credentials = server_config.get("credentials", self._credentials)
        region = server_config.get("region", self._region)
        endpoint_url = server_config.get("endpoint_url", self._endpoint_url)

        # Set AWS region
        if region:
            conn.execute(f"SET s3_region='{region}'")

        # Set S3 endpoint URL if specified
        if endpoint_url:
            conn.execute(f"SET s3_endpoint='{endpoint_url}'")

        # Set AWS credentials if provided
        if credentials:
            if access_key := credentials.get("aws_access_key_id"):
                conn.execute(f"SET s3_access_key_id='{access_key}'")

            if secret_key := credentials.get("aws_secret_access_key"):
                conn.execute(f"SET s3_secret_access_key='{secret_key}'")

            if session_token := credentials.get("aws_session_token"):
                conn.execute(f"SET s3_session_token='{session_token}'")

    def _create_table_query(self, s3_uri: str, file_format: str, model_key: str) -> str:
        """Create a SQL query to load data from S3 into a table.

        Args:
            s3_uri: S3 URI of the file
            file_format: Format of the file
            model_key: Name to use for the table

        Returns:
            SQL query to create the table
        """
        # Escape the model key to avoid SQL injection
        safe_model_key = model_key.replace('"', '""')

        if file_format == 'csv':
            return f'CREATE OR REPLACE TABLE "{safe_model_key}" AS SELECT * FROM read_csv(\'{s3_uri}\', auto_detect=TRUE);'
        elif file_format == 'parquet':
            return f'CREATE OR REPLACE TABLE "{safe_model_key}" AS SELECT * FROM read_parquet(\'{s3_uri}\');'
        elif file_format == 'json':
            return f'CREATE OR REPLACE TABLE "{safe_model_key}" AS SELECT * FROM read_json(\'{s3_uri}\', auto_detect=TRUE);'
        elif file_format == 'avro':
            return f'CREATE OR REPLACE TABLE "{safe_model_key}" AS SELECT * FROM read_avro(\'{s3_uri}\');'
        elif file_format == 'orc':
            return f'CREATE OR REPLACE TABLE "{safe_model_key}" AS SELECT * FROM read_orc(\'{s3_uri}\');'
        else:
            # Default to Parquet
            return f'CREATE OR REPLACE TABLE "{safe_model_key}" AS SELECT * FROM read_parquet(\'{s3_uri}\');'

    def is_available(self) -> bool:
        """Check if this data source is properly configured and available."""
        try:
            import duckdb

            # Check if we have credentials
            has_credentials = (
                self._credentials.get("aws_access_key_id") is not None and
                self._credentials.get("aws_secret_access_key") is not None
            )

            # If we have IAM role access (no credentials needed) or valid credentials
            return True
        except ImportError:
            return False

    def get_configuration(self) -> Dict[str, Any]:
        """Get the current configuration for this data source."""
        return {
            "region": self._region,
            "allowed_buckets": list(self._allowed_buckets),
            "max_buckets": self._max_buckets,
            "has_credentials": bool(self._credentials.get("aws_access_key_id")),
            "endpoint_url": self._endpoint_url,
        }

    def configure(self, config: Dict[str, Any]) -> None:
        """Configure this data source with specific values."""
        if "region" in config:
            self._region = config["region"]

        if "allowed_buckets" in config:
            self._allowed_buckets = set(config["allowed_buckets"])

        if "max_buckets" in config:
            self._max_buckets = int(config["max_buckets"])

        if "credentials" in config:
            self._credentials.update(config["credentials"])

        if "endpoint_url" in config:
            self._endpoint_url = config["endpoint_url"]
