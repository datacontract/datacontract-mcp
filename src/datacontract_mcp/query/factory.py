"""Factory for creating query strategies based on server type."""

from typing import TYPE_CHECKING
from ..models_datacontract import ServerType

if TYPE_CHECKING:
    from .base import QueryStrategy
    from .local import LocalFileStrategy
    from .s3 import S3Strategy


def get_query_strategy(server_type: ServerType) -> "QueryStrategy":
    """Factory function to get the appropriate query strategy for a server type.
    
    Args:
        server_type: The type of server to create a strategy for
        
    Returns:
        A query strategy instance appropriate for the given server type
        
    Raises:
        ValueError: If the server type is not supported
    """
    from .local import LocalFileStrategy
    from .s3 import S3Strategy
    
    strategies = {
        ServerType.LOCAL: LocalFileStrategy(),
        ServerType.FILE: LocalFileStrategy(),
        ServerType.S3: S3Strategy(),
        # Add other server types as needed
    }
    
    strategy = strategies.get(server_type)
    if not strategy:
        raise ValueError(f"Unsupported server type: {server_type}")
        
    return strategy