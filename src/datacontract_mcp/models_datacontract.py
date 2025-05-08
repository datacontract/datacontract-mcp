"""Pydantic models for Data Contract specification."""

from enum import Enum
from typing import Dict, List, Optional, Any, Literal
from pydantic import BaseModel, ConfigDict, field_validator, model_validator, Field


class ServerType(str, Enum):
    """Server types defined in the Data Contract specification."""
    BIGQUERY = "bigquery"
    S3 = "s3"
    SFTP = "sftp"
    REDSHIFT = "redshift"
    AZURE = "azure"
    SQLSERVER = "sqlserver"
    SNOWFLAKE = "snowflake"
    DATABRICKS = "databricks"
    DATAFRAME = "dataframe"
    GLUE = "glue"
    POSTGRES = "postgres"
    ORACLE = "oracle"
    KAFKA = "kafka"
    PUBSUB = "pubsub"
    KINESIS = "kinesis"
    TRINO = "trino"
    LOCAL = "local"
    FILE = "file"  # Sometimes used as alternative for local


class ServerFormat(str, Enum):
    """Data formats supported in Data Contract servers."""
    PARQUET = "parquet"
    DELTA = "delta"
    JSON = "json"
    CSV = "csv"


class FieldType(str, Enum):
    """Field types defined in the Data Contract specification."""
    NUMBER = "number"
    DECIMAL = "decimal"
    NUMERIC = "numeric"
    INT = "int"
    INTEGER = "integer"
    LONG = "long"
    BIGINT = "bigint"
    FLOAT = "float"
    DOUBLE = "double"
    STRING = "string"
    TEXT = "text"
    VARCHAR = "varchar"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    TIMESTAMP_TZ = "timestamp_tz"
    TIMESTAMP_NTZ = "timestamp_ntz"
    DATE = "date"
    ARRAY = "array"
    MAP = "map"
    OBJECT = "object"
    RECORD = "record"
    STRUCT = "struct"
    BYTES = "bytes"
    NULL = "null"


class ModelType(str, Enum):
    """Model types defined in the Data Contract specification."""
    TABLE = "table"
    VIEW = "view"
    OBJECT = "object"


class ContactInfo(BaseModel):
    """Contact information in a data contract."""
    name: Optional[str] = None
    url: Optional[str] = None
    email: Optional[str] = None


class InfoSection(BaseModel):
    """Info section of a data contract."""
    title: str
    version: str
    description: Optional[str] = None
    status: Optional[str] = None
    owner: Optional[str] = None
    contact: Optional[ContactInfo] = None


class BaseServer(BaseModel):
    """Base server configuration in a data contract."""
    model_config = ConfigDict(extra="allow")

    type: ServerType
    description: Optional[str] = None
    environment: Optional[str] = None


class LocalServer(BaseServer):
    """Local/file server configuration."""
    type: Literal[ServerType.LOCAL, ServerType.FILE]
    path: str
    format: ServerFormat
    delimiter: Optional[str] = None
    description: Optional[str] = None

    @field_validator("format")
    def validate_format(cls, v):
        """Validate that format is valid for local/file server."""
        if v not in [ServerFormat.CSV, ServerFormat.JSON, ServerFormat.PARQUET, ServerFormat.DELTA]:
            raise ValueError(f"Format {v} not supported for local/file server")
        return v


class S3Server(BaseServer):
    """AWS S3 server configuration."""
    type: Literal[ServerType.S3]
    location: str  # S3 URL starting with 's3://'
    format: Optional[ServerFormat] = None
    delimiter: Optional[str] = None
    endpointUrl: Optional[str] = None
    
    @field_validator("location")
    def validate_location(cls, v):
        """Validate that location is a valid S3 URL."""
        if not v.startswith("s3://"):
            raise ValueError("S3 location must start with 's3://'")
        return v
        
    @field_validator("format")
    def validate_format(cls, v):
        """Validate that format is valid for S3 server if provided."""
        if v is not None and v not in [ServerFormat.CSV, ServerFormat.JSON, ServerFormat.PARQUET, ServerFormat.DELTA]:
            raise ValueError(f"Format {v} not supported for S3 server")
        return v
        
    @field_validator("delimiter")
    def validate_delimiter(cls, v, info):
        """Validate that delimiter is only used with JSON format."""
        values = info.data
        if v is not None and values.get("format") != ServerFormat.JSON:
            raise ValueError("Delimiter is only supported for JSON format")
        return v


class FieldDefinition(BaseModel):
    """Field definition in a data contract model."""
    model_config = ConfigDict(extra="allow")

    description: Optional[str] = None
    type: Optional[FieldType] = None
    required: Optional[bool] = False
    unique: Optional[bool] = False
    primaryKey: Optional[bool] = False
    references: Optional[str] = None
    examples: Optional[List[Any]] = None
    minLength: Optional[int] = None
    maxLength: Optional[int] = None
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    format: Optional[str] = None


class Model(BaseModel):
    """Model definition in a data contract."""
    model_config = ConfigDict(extra="allow")

    description: Optional[str] = None
    type: Optional[ModelType] = ModelType.TABLE
    fields: Dict[str, FieldDefinition] = Field(default_factory=dict)
    primaryKey: Optional[List[str]] = None
    title: Optional[str] = None


class TermsSection(BaseModel):
    """Terms section in a data contract."""
    model_config = ConfigDict(extra="allow")

    usage: Optional[str] = None
    limitations: Optional[str] = None
    billing: Optional[str] = None
    noticePeriod: Optional[str] = None


class DataContract(BaseModel):
    """Top-level Data Contract structure."""
    model_config = ConfigDict(extra="allow")

    dataContractSpecification: str
    id: str
    info: InfoSection
    servers: Dict[str, BaseServer]
    models: Dict[str, Model]
    terms: Optional[TermsSection] = None
    examples: Optional[Dict[str, Any]] = None
    tags: Optional[List[str]] = None

    @model_validator(mode='after')
    def validate_servers(self):
        """
        Validate server configurations based on their type.
        """
        for key, server in self.servers.items():
            # For local/file servers, ensure they have path and format
            if server.type in [ServerType.LOCAL, ServerType.FILE]:
                if not hasattr(server, 'path') or not server.path:
                    raise ValueError(f"Server '{key}' of type {server.type} must have a 'path'")
                if not hasattr(server, 'format') or not server.format:
                    raise ValueError(f"Server '{key}' of type {server.type} must have a 'format'")
            
            # For S3 servers, ensure they have location
            elif server.type == ServerType.S3:
                if not hasattr(server, 'location') or not server.location:
                    raise ValueError(f"Server '{key}' of type {server.type} must have a 'location'")

        return self


class QueryResult(BaseModel):
    """Result of a Data Contract query."""
    model_config = ConfigDict(extra="allow")

    records: List[Dict[str, Any]]
    query: str
    model_key: str
    server_key: str