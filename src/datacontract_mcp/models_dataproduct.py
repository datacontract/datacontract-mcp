"""Pydantic models for Data Product specification."""

from typing import Dict, List, Optional, Any
from pydantic import BaseModel, ConfigDict


class DataProductInfo(BaseModel):
    """Information about a data product."""
    model_config = ConfigDict(extra="allow")
    
    title: str
    owner: str
    domain: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None
    archetype: Optional[str] = None
    maturity: Optional[str] = None


class ServerDetails(BaseModel):
    """Server details for data product output port."""
    model_config = ConfigDict(extra="allow")
    
    project: Optional[str] = None
    dataset: Optional[str] = None
    account: Optional[str] = None
    database: Optional[str] = None
    schema: Optional[str] = None
    host: Optional[str] = None
    topic: Optional[str] = None
    location: Optional[str] = None
    delimiter: Optional[str] = None
    format: Optional[str] = None
    table: Optional[str] = None
    view: Optional[str] = None
    share: Optional[str] = None


class InputPort(BaseModel):
    """Input port definition in a data product."""
    model_config = ConfigDict(extra="allow")
    
    id: str
    name: str
    sourceSystemId: str
    description: Optional[str] = None
    type: Optional[str] = None
    location: Optional[str] = None
    links: Optional[Dict[str, str]] = None
    custom: Optional[Dict[str, str]] = None
    tags: Optional[List[str]] = None


class OutputPort(BaseModel):
    """Output port definition in a data product."""
    model_config = ConfigDict(extra="allow")
    
    id: str
    name: str
    description: Optional[str] = None
    type: Optional[str] = None
    status: Optional[str] = None
    location: Optional[str] = None
    server: Optional[ServerDetails] = None
    links: Optional[Dict[str, str]] = None
    custom: Optional[Dict[str, str]] = None
    containsPii: Optional[bool] = False
    autoApprove: Optional[bool] = False
    dataContractId: Optional[str] = None
    tags: Optional[List[str]] = None


class DataProduct(BaseModel):
    """Top-level Data Product structure."""
    model_config = ConfigDict(extra="allow")
    
    dataProductSpecification: Optional[str] = "0.0.1"
    id: str
    info: DataProductInfo
    inputPorts: Optional[List[InputPort]] = None
    outputPorts: Optional[List[OutputPort]] = None
    links: Optional[Dict[str, str]] = None
    custom: Optional[Dict[str, Any]] = None
    tags: Optional[List[str]] = None