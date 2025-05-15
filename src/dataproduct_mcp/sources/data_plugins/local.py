"""Local data source plugin for querying files via DuckDB."""

import logging
import os
from typing import Any, Dict, List

from ..data_source import DataSourcePlugin, ServerType

logger = logging.getLogger("dataproduct-mcp.sources.data_plugins.local")

# Global DuckDB connection pool
_duckdb_connections = {}
_last_used_time = {}


def create_duckdb_connection() -> Any:
    """Create a new DuckDB connection.

    Returns:
        DuckDB connection object
    """
    try:
        import duckdb
        return duckdb.connect(database=":memory:")
    except ImportError as e:
        logger.error(f"Error importing duckdb: {e}")
        raise ImportError("DuckDB is required for local data querying. "
                          "Install with: pip install duckdb")


def get_duckdb_pool(max_connections: int = 5, idle_timeout: int = 300) -> Dict[str, Any]:
    """Get or create a DuckDB connection pool.

    Args:
        max_connections: Maximum number of connections in the pool
        idle_timeout: Timeout in seconds for idle connections

    Returns:
        Dictionary with connection pool management functions
    """
    global _duckdb_connections, _last_used_time

    # Function to create a new connection
    def get_connection():
        import time

        # Check for available connections
        if len(_duckdb_connections) >= max_connections:
            # Find the oldest connection
            oldest_time = min(_last_used_time.values())
            oldest_conn = next(k for k, v in _last_used_time.items() if v == oldest_time)

            # Check if it's expired
            if time.time() - oldest_time > idle_timeout:
                logger.debug("Closing idle connection")
                conn = _duckdb_connections.pop(oldest_conn)
                del _last_used_time[oldest_conn]
                try:
                    conn.close()
                except Exception:
                    pass
            else:
                # All connections in use and not expired, wait for one to become available
                logger.warning("Connection pool exhausted, consider increasing max_connections")

        # Create a new connection
        conn = create_duckdb_connection()
        conn_id = id(conn)
        _duckdb_connections[conn_id] = conn
        _last_used_time[conn_id] = time.time()
        return conn, conn_id

    # Function to release a connection back to the pool
    def release_connection(conn_id: int):
        import time
        if conn_id in _duckdb_connections:
            _last_used_time[conn_id] = time.time()

    # Function to cleanup the pool
    def cleanup():
        import time
        current_time = time.time()
        expired = []

        # Find expired connections
        for conn_id, last_used in _last_used_time.items():
            if current_time - last_used > idle_timeout:
                expired.append(conn_id)

        # Close and remove expired connections
        for conn_id in expired:
            try:
                _duckdb_connections[conn_id].close()
            except Exception:
                pass
            del _duckdb_connections[conn_id]
            del _last_used_time[conn_id]

        logger.debug(f"Cleaned up {len(expired)} expired connections, {len(_duckdb_connections)} remaining")

    return {
        "get_connection": get_connection,
        "release_connection": release_connection,
        "cleanup": cleanup,
        "size": lambda: len(_duckdb_connections)
    }


@DataSourcePlugin.register(ServerType.LOCAL)
@DataSourcePlugin.register(ServerType.FILE)  # Register FILE as an alias for LOCAL
class LocalDataSource(DataSourcePlugin):
    """Plugin for querying local files via DuckDB."""

    def __init__(self):
        """Initialize the local data source plugin."""
        self._pool = None
        self._connection_pooling_enabled = True
        self._max_connections = 5
        self._idle_timeout = 300  # 5 minutes

    @property
    def server_type(self) -> str:
        """The server type this plugin supports."""
        return ServerType.LOCAL

    def execute(self, model_key: str, query: str, server_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute a query against a local file.

        Args:
            model_key: The name of the model or table to query
            query: SQL query to execute
            server_config: Server configuration with file path, format, etc.

        Returns:
            List of records as dictionaries

        Raises:
            ImportError: If DuckDB is not installed
            Exception: For any other errors during query execution
        """
        # Get file path from server config
        file_path = server_config.get("path") or server_config.get("location")

        if not file_path:
            raise ValueError("No file path provided in server configuration")

        # Resolve the file path
        if not os.path.isabs(file_path):
            # If DATAASSET_SOURCE is set, try looking for the file in that directory first
            dataasset_source = os.environ.get("DATAASSET_SOURCE")
            if dataasset_source:
                # Build the absolute path using DATAASSET_SOURCE
                data_source_path = os.path.join(dataasset_source, file_path)
                # If the file exists in DATAASSET_SOURCE directory, use that path
                if os.path.exists(data_source_path):
                    logger.info(f"Using file from DATAASSET_SOURCE: {data_source_path}")
                    file_path = data_source_path
                else:
                    # If not found in DATAASSET_SOURCE, use the current directory
                    base_dir = server_config.get("base_directory") or os.getcwd()
                    file_path = os.path.join(base_dir, file_path)
            else:
                # If DATAASSET_SOURCE is not set, use the current directory
                base_dir = server_config.get("base_directory") or os.getcwd()
                file_path = os.path.join(base_dir, file_path)

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        # Determine file format
        file_format = self._determine_file_format(file_path, server_config)

        # Execute the query using DuckDB
        return self._execute_duckdb_query(file_path, file_format, model_key, query)

    def _determine_file_format(self, file_path: str, server_config: Dict[str, Any]) -> str:
        """Determine the format of a file based on its extension or configuration.

        Args:
            file_path: Path to the file
            server_config: Server configuration

        Returns:
            File format string (e.g., 'csv', 'parquet')
        """
        # Check if format is explicitly specified in the config
        if format_str := server_config.get("format"):
            return format_str.lower()

        # Otherwise, infer from file extension
        extension = os.path.splitext(file_path)[1].lower()

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
            # Default to CSV if unknown
            logger.warning(f"Unknown file extension: {extension}, defaulting to CSV")
            return 'csv'

    def _execute_duckdb_query(self, file_path: str, file_format: str, model_key: str, query: str) -> List[Dict[str, Any]]:
        """Execute a query using DuckDB.

        Args:
            file_path: Path to the file
            file_format: Format of the file
            model_key: Name to use for the table
            query: SQL query to execute

        Returns:
            List of records as dictionaries
        """
        try:
            conn = None
            conn_id = None

            try:
                # Get a connection from the pool if enabled
                if self._connection_pooling_enabled and self._pool:
                    conn, conn_id = self._pool["get_connection"]()
                else:
                    # Otherwise, create a new connection
                    conn = create_duckdb_connection()

                # Create a table from the file
                table_query = self._create_table_query(file_path, file_format, model_key)
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
                # Release the connection back to the pool if we got it from there
                if self._connection_pooling_enabled and self._pool and conn_id:
                    self._pool["release_connection"](conn_id)
                elif conn and not self._connection_pooling_enabled:
                    # Close the connection if not using pooling
                    conn.close()
        except ImportError as e:
            logger.error(f"Error importing duckdb: {e}")
            raise ImportError("DuckDB is required for local data querying. "
                            "Install with: pip install duckdb")
        except Exception as e:
            logger.error(f"Error executing DuckDB query: {e}")
            raise

    def _create_table_query(self, file_path: str, file_format: str, model_key: str) -> str:
        """Create a SQL query to load data from a file into a table.

        Args:
            file_path: Path to the file
            file_format: Format of the file
            model_key: Name to use for the table

        Returns:
            SQL query to create the table
        """
        # Escape the model key to avoid SQL injection
        safe_model_key = model_key.replace('"', '""')

        if file_format == 'csv':
            # Use auto_type_candidates to handle different data types
            return f'CREATE OR REPLACE TABLE "{safe_model_key}" AS SELECT * FROM read_csv(\'{file_path}\', auto_type_candidates=[\'BIGINT\',\'VARCHAR\',\'BOOLEAN\',\'DOUBLE\']);'
        elif file_format == 'parquet':
            return f'CREATE OR REPLACE TABLE "{safe_model_key}" AS SELECT * FROM read_parquet(\'{file_path}\');'
        elif file_format == 'json':
            return f'CREATE OR REPLACE TABLE "{safe_model_key}" AS SELECT * FROM read_json(\'{file_path}\', auto_detect=TRUE);'
        elif file_format == 'avro':
            return f'CREATE OR REPLACE TABLE "{safe_model_key}" AS SELECT * FROM read_avro(\'{file_path}\');'
        elif file_format == 'orc':
            return f'CREATE OR REPLACE TABLE "{safe_model_key}" AS SELECT * FROM read_orc(\'{file_path}\');'
        else:
            # Default to CSV with auto_type_candidates 
            return f'CREATE OR REPLACE TABLE "{safe_model_key}" AS SELECT * FROM read_csv(\'{file_path}\', auto_type_candidates=[\'BIGINT\',\'VARCHAR\',\'BOOLEAN\',\'DOUBLE\']);'

    def is_available(self) -> bool:
        """Check if this data source is properly configured and available."""
        try:
            import importlib.util
            duckdb_spec = importlib.util.find_spec("duckdb")
            return duckdb_spec is not None
        except ImportError:
            return False

    def get_configuration(self) -> Dict[str, Any]:
        """Get the current configuration for this data source."""
        return {
            "connection_pooling": self._connection_pooling_enabled,
            "max_connections": self._max_connections,
            "idle_timeout": self._idle_timeout,
        }

    def configure(self, config: Dict[str, Any]) -> None:
        """Configure this data source with specific values."""
        if "connection_pooling" in config:
            self._connection_pooling_enabled = bool(config["connection_pooling"])

        if "max_connections" in config:
            self._max_connections = int(config["max_connections"])

        if "idle_timeout" in config:
            self._idle_timeout = int(config["idle_timeout"])

        # Setup connection pool if enabled
        if self._connection_pooling_enabled:
            self._pool = get_duckdb_pool(
                max_connections=self._max_connections,
                idle_timeout=self._idle_timeout
            )
        else:
            self._pool = None
