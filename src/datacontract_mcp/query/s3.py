"""Query strategy for S3 data sources.

This module implements the query strategy for AWS S3 and S3-compatible storage.
It supports various data formats (CSV, Parquet, JSON, Delta Lake) and
provides several configuration options through environment variables.

Environment Variables:
    AWS_REGION or AWS_DEFAULT_REGION:
        The AWS region to use. Default: "us-east-1"

    S3_BUCKETS:
        Comma-separated list of allowed S3 buckets. If not set, all buckets are allowed.
        Example: "bucket1,bucket2,bucket3"

    S3_MAX_BUCKETS:
        Maximum number of allowed buckets. Default: 10

    AWS_ACCESS_KEY_ID or S3_ACCESS_KEY_ID:
        AWS access key ID for authentication

    AWS_SECRET_ACCESS_KEY or S3_SECRET_ACCESS_KEY:
        AWS secret access key for authentication

    AWS_SESSION_TOKEN or S3_SESSION_TOKEN:
        AWS session token for temporary credentials

    AWS_PROFILE or S3_PROFILE:
        AWS credential profile to use

Authentication Methods (in order of precedence):
    1. AWS profile: If AWS_PROFILE/S3_PROFILE is specified
    2. Explicit credentials: If AWS_ACCESS_KEY_ID/S3_ACCESS_KEY_ID and
       AWS_SECRET_ACCESS_KEY/S3_SECRET_ACCESS_KEY are provided
    3. Default credential chain: Environment variables, ~/.aws/credentials,
       IAM roles, etc.
"""

import boto3
import logging
import os
import tempfile
import urllib.parse
from typing import Dict, List, Any, Optional, Tuple

from .base import DataQueryStrategy, create_duckdb_connection
from ..models_datacontract import ServerFormat

logger = logging.getLogger("datacontract-mcp.query.s3")

# Environment variables for AWS configuration
AWS_REGION = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", ""))
S3_BUCKETS = set(filter(None, os.getenv("S3_BUCKETS", "").split(",")))
S3_MAX_BUCKETS = int(os.getenv("S3_MAX_BUCKETS", "10"))
S3_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", os.getenv("S3_ACCESS_KEY_ID", ""))
S3_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", os.getenv("S3_SECRET_ACCESS_KEY", ""))
S3_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN", os.getenv("S3_SESSION_TOKEN", ""))
S3_PROFILE = os.getenv("AWS_PROFILE", os.getenv("S3_PROFILE", ""))


class S3QueryStrategy(DataQueryStrategy):
    """Strategy for querying S3 data sources."""

    def execute(self, model_key: str, query: str, server_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute the query on S3 data sources.

        Args:
            model_key: The name of the model to query
            query: The SQL query to execute
            server_config: The server configuration

        Returns:
            A list of query result records

        Raises:
            ValueError: If the server configuration is invalid
            FileNotFoundError: If no matching files are found
        """
        # Extract and validate configuration
        location, format, endpoint_url = self._extract_config(server_config)

        # Parse S3 location
        bucket_name, prefix = self._parse_s3_location(location)

        # Create S3 client
        s3_client = self._create_s3_client(endpoint_url)

        # Choose the appropriate format handler
        handlers = {
            ServerFormat.CSV: self._handle_csv,
            ServerFormat.PARQUET: self._handle_parquet,
            ServerFormat.JSON: self._handle_json,
            ServerFormat.DELTA: self._handle_delta,
        }

        handler = handlers.get(format)
        if not handler:
            raise ValueError(f"Unsupported format '{format}' for S3 server")

        return handler(model_key, query, bucket_name, prefix, s3_client, server_config)

    def _extract_config(self, server_config: Dict[str, Any]) -> Tuple[str, str, Optional[str]]:
        """Extract and validate configuration parameters.

        Args:
            server_config: The server configuration

        Returns:
            A tuple of (location, format, endpoint_url)

        Raises:
            ValueError: If required configuration is missing
        """
        location = server_config.get('location')
        format = server_config.get('format')
        endpoint_url = server_config.get('endpointUrl')

        if not location:
            raise ValueError("S3 server must specify a 'location'")
        if not format:
            raise ValueError("S3 server must specify a 'format'")

        return location, format, endpoint_url

    def _parse_s3_location(self, location: str) -> Tuple[str, str]:
        """Parse S3 URL into bucket name and prefix.

        Args:
            location: The S3 URL

        Returns:
            A tuple of (bucket_name, prefix)

        Raises:
            ValueError: If the URL is not a valid S3 URL
            ValueError: If the bucket is not in the allowed list
        """
        parsed_url = urllib.parse.urlparse(location)
        if parsed_url.scheme != 's3':
            raise ValueError(f"Invalid S3 URL: {location}")

        bucket_name = parsed_url.netloc
        prefix = parsed_url.path.lstrip('/')

        # Security check: verify bucket is in allowed list if S3_BUCKETS is not empty
        if S3_BUCKETS and bucket_name not in S3_BUCKETS:
            allowed_buckets = ", ".join(S3_BUCKETS)
            raise ValueError(
                f"Access to bucket '{bucket_name}' is not allowed. "
                f"Allowed buckets: {allowed_buckets}. "
                f"To allow access, add the bucket to the S3_BUCKETS environment variable."
            )

        # Check bucket against max buckets limit when S3_MAX_BUCKETS is configured
        if S3_MAX_BUCKETS > 0 and len(S3_BUCKETS) > S3_MAX_BUCKETS:
            logger.warning(
                f"Too many buckets configured. Maximum allowed: {S3_MAX_BUCKETS}. "
                f"Current count: {len(S3_BUCKETS)}. "
                f"Adjust S3_MAX_BUCKETS environment variable to increase the limit."
            )

        return bucket_name, prefix

    def _create_s3_client(self, endpoint_url: Optional[str] = None) -> Any:
        """Create an S3 client with credentials from environment variables.

        Args:
            endpoint_url: Optional endpoint URL for S3-compatible storage

        Returns:
            An S3 client

        Notes:
            This method uses the following environment variables for configuration:
            - AWS_REGION or AWS_DEFAULT_REGION: AWS region to use
            - AWS_ACCESS_KEY_ID or S3_ACCESS_KEY_ID: Access key ID
            - AWS_SECRET_ACCESS_KEY or S3_SECRET_ACCESS_KEY: Secret access key
            - AWS_SESSION_TOKEN or S3_SESSION_TOKEN: Session token (optional)
            - AWS_PROFILE or S3_PROFILE: AWS credential profile to use (optional)

            If AWS_PROFILE/S3_PROFILE is set, a boto3 session is created with that profile.
            Otherwise, if AWS_ACCESS_KEY_ID/S3_ACCESS_KEY_ID and
            AWS_SECRET_ACCESS_KEY/S3_SECRET_ACCESS_KEY are set, they are used directly.

            If no explicit credentials are provided, boto3's default credential chain is used,
            which checks environment variables, ~/.aws/credentials, IAM roles, etc.
        """
        s3_kwargs = {}

        # Add endpoint URL if provided (for S3-compatible storage like MinIO)
        if endpoint_url:
            s3_kwargs['endpoint_url'] = endpoint_url

        # Always set region
        s3_kwargs['region_name'] = AWS_REGION

        # If a specific profile is specified, use a session with that profile
        if S3_PROFILE:
            logger.info(f"Creating S3 client using profile: {S3_PROFILE}")
            session = boto3.Session(profile_name=S3_PROFILE)
            return session.client('s3', **s3_kwargs)

        # If explicit credentials are provided, use them
        if S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY:
            logger.info("Creating S3 client using explicit credentials")
            s3_kwargs['aws_access_key_id'] = S3_ACCESS_KEY_ID
            s3_kwargs['aws_secret_access_key'] = S3_SECRET_ACCESS_KEY

            # Add session token if available
            if S3_SESSION_TOKEN:
                s3_kwargs['aws_session_token'] = S3_SESSION_TOKEN
        else:
            logger.info("Creating S3 client using default credential chain")

        return boto3.client('s3', **s3_kwargs)

    def _handle_csv(self, model_key: str, query: str, bucket_name: str, prefix: str,
                   s3_client: Any, server_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Handle CSV format from S3.

        Args:
            model_key: The name of the model to query
            query: The SQL query to execute
            bucket_name: The S3 bucket name
            prefix: The S3 object prefix
            s3_client: The S3 client
            server_config: The server configuration

        Returns:
            A list of query result records

        Raises:
            FileNotFoundError: If no matching files are found
        """
        logger.info(f"Querying S3 CSV data from {bucket_name}/{prefix}")

        # List objects using the prefix
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        if 'Contents' not in response or not response['Contents']:
            raise FileNotFoundError(f"No files found at s3://{bucket_name}/{prefix}")

        # Use the first file matching the prefix
        s3_key = response['Contents'][0]['Key']

        with tempfile.NamedTemporaryFile(suffix='.csv') as temp_file, create_duckdb_connection() as conn:
            s3_client.download_file(bucket_name, s3_key, temp_file.name)

            # Load CSV data into DuckDB
            sql = f"""
            CREATE TABLE "{model_key}" AS
            SELECT * FROM read_csv(
                '{temp_file.name}', 
                auto_type_candidates=['BIGINT','VARCHAR','BOOLEAN','DOUBLE']
            );
            """
            conn.execute(sql)

            # Execute the query
            df = conn.execute(query).fetchdf()

            # Convert to records and return
            return df.to_dict(orient="records")

    def _handle_parquet(self, model_key: str, query: str, bucket_name: str, prefix: str,
                       s3_client: Any, server_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Handle Parquet format from S3.

        Args:
            model_key: The name of the model to query
            query: The SQL query to execute
            bucket_name: The S3 bucket name
            prefix: The S3 object prefix
            s3_client: The S3 client
            server_config: The server configuration

        Returns:
            A list of query result records

        Raises:
            FileNotFoundError: If no matching files are found
        """
        logger.info(f"Querying S3 Parquet data from {bucket_name}/{prefix}")
        endpoint_url = server_config.get('endpointUrl')

        with create_duckdb_connection() as conn:
            try:
                # Try using direct S3 access with credentials from environment
                s3_path = f"s3://{bucket_name}/{prefix}"
                if endpoint_url:
                    # For custom endpoints, need to set httpfs options
                    conn.execute(f"SET s3_endpoint='{endpoint_url}'")

                # Create a table for the model
                sql = f"""
                CREATE TABLE "{model_key}" AS
                SELECT * FROM read_parquet('{s3_path}*');
                """
                conn.execute(sql)

                # Execute the query
                df = conn.execute(query).fetchdf()

                # Convert to records and return
                return df.to_dict(orient="records")

            except Exception as e:
                logger.warning(f"Direct S3 access failed: {str(e)}, falling back to download")
                return self._handle_parquet_download(model_key, query, bucket_name, prefix, s3_client)

    def _handle_parquet_download(self, model_key: str, query: str, bucket_name: str,
                               prefix: str, s3_client: Any) -> List[Dict[str, Any]]:
        """Handle Parquet format from S3 by downloading file.

        Args:
            model_key: The name of the model to query
            query: The SQL query to execute
            bucket_name: The S3 bucket name
            prefix: The S3 object prefix
            s3_client: The S3 client

        Returns:
            A list of query result records

        Raises:
            FileNotFoundError: If no matching files are found
        """
        # List objects using the prefix
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        if 'Contents' not in response or not response['Contents']:
            raise FileNotFoundError(f"No files found at s3://{bucket_name}/{prefix}")

        # Find first parquet file
        s3_key = None
        for item in response['Contents']:
            if item['Key'].endswith('.parquet'):
                s3_key = item['Key']
                break

        if not s3_key:
            raise FileNotFoundError(f"No parquet files found at s3://{bucket_name}/{prefix}")

        # Create a temporary file to store the parquet
        with tempfile.NamedTemporaryFile(suffix='.parquet') as temp_file, create_duckdb_connection() as conn:
            s3_client.download_file(bucket_name, s3_key, temp_file.name)

            # Load parquet data into DuckDB
            sql = f"""
            CREATE TABLE "{model_key}" AS
            SELECT * FROM read_parquet('{temp_file.name}');
            """
            conn.execute(sql)

            # Execute the query
            df = conn.execute(query).fetchdf()

            # Convert to records and return
            return df.to_dict(orient="records")

    def _handle_json(self, model_key: str, query: str, bucket_name: str, prefix: str,
                    s3_client: Any, server_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Handle JSON format from S3.

        Args:
            model_key: The name of the model to query
            query: The SQL query to execute
            bucket_name: The S3 bucket name
            prefix: The S3 object prefix
            s3_client: The S3 client
            server_config: The server configuration

        Returns:
            A list of query result records

        Raises:
            FileNotFoundError: If no matching files are found
        """
        logger.info(f"Querying S3 JSON data from {bucket_name}/{prefix}")

        # List objects using the prefix
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        if 'Contents' not in response or not response['Contents']:
            raise FileNotFoundError(f"No files found at s3://{bucket_name}/{prefix}")

        # Use the first file matching the prefix
        s3_key = response['Contents'][0]['Key']

        with tempfile.NamedTemporaryFile(suffix='.json') as temp_file, create_duckdb_connection() as conn:
            s3_client.download_file(bucket_name, s3_key, temp_file.name)

            # Load JSON data into DuckDB, handling delimiter option
            delimiter = server_config.get('delimiter')
            json_option = "auto"  # Default option

            if delimiter == "new_line":
                json_option = "newline_delimited"
            elif delimiter == "array":
                json_option = "array"

            sql = f"""
            CREATE TABLE "{model_key}" AS
            SELECT * FROM read_json(
                '{temp_file.name}',
                format='{json_option}'
            );
            """
            conn.execute(sql)

            # Execute the query
            df = conn.execute(query).fetchdf()

            # Convert to records and return
            return df.to_dict(orient="records")

    def _handle_delta(self, model_key: str, query: str, bucket_name: str, prefix: str,
                     s3_client: Any, server_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Handle Delta Lake format from S3.

        Args:
            model_key: The name of the model to query
            query: The SQL query to execute
            bucket_name: The S3 bucket name
            prefix: The S3 object prefix
            s3_client: The S3 client
            server_config: The server configuration

        Returns:
            A list of query result records

        Raises:
            ValueError: If Delta Lake extension is not available
        """
        logger.info(f"Querying S3 Delta Lake data from {bucket_name}/{prefix}")
        endpoint_url = server_config.get('endpointUrl')

        with create_duckdb_connection() as conn:
            # Ensure DuckDB has Delta Lake extension loaded
            try:
                conn.execute("LOAD 'delta';")
            except Exception as e:
                raise ValueError(f"Delta Lake extension not available: {str(e)}")

            # Try using direct S3 access with credentials from environment
            s3_path = f"s3://{bucket_name}/{prefix}"
            if endpoint_url:
                # For custom endpoints, need to set httpfs options
                conn.execute(f"SET s3_endpoint='{endpoint_url}'")

            # Create a table for the model using Delta Lake format
            sql = f"""
            CREATE TABLE "{model_key}" AS
            SELECT * FROM delta_scan('{s3_path}');
            """

            try:
                conn.execute(sql)

                # Execute the query
                df = conn.execute(query).fetchdf()

                # Convert to records and return
                return df.to_dict(orient="records")

            except Exception as e:
                logger.error(f"Failed to query Delta Lake format: {str(e)}")
                raise ValueError(f"Failed to query Delta Lake format: {str(e)}")
