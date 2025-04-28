"""Factory for creating query strategies based on server type."""

from typing import TYPE_CHECKING
from ..models_datacontract import ServerType

if TYPE_CHECKING:
    from .base import DataQueryStrategy


def get_query_strategy(server_type: ServerType) -> "DataQueryStrategy":
    """Factory function to get the appropriate query strategy for a server type.
    
    Args:
        server_type: The type of server to create a strategy for
        
    Returns:
        A query strategy instance appropriate for the given server type
        
    Raises:
        ValueError: If the server type is not supported
    """
    from .local import LocalFileQueryStrategy
    from .s3 import S3QueryStrategy
    
    strategies = {
        ServerType.LOCAL: LocalFileQueryStrategy(),
        ServerType.FILE: LocalFileQueryStrategy(),
        ServerType.S3: S3QueryStrategy(),
        # Add other server types as needed
    }
    
    strategy = strategies.get(server_type)
    if not strategy:
        raise ValueError(f"Unsupported server type: {server_type}")
        
    return strategy