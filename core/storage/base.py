"""Base storage interface"""

from abc import ABC, abstractmethod
from typing import List, Optional


class BaseStorage(ABC):
    """Abstract base class for all storage backends"""
    
    def __init__(self, config: dict, role: str = None):
        self.config = config
        self.role = role
    
    @abstractmethod
    def put(self, path: str, data: bytes | str) -> None:
        """Store data at path"""
        pass
    
    @abstractmethod
    def get(self, path: str) -> Optional[bytes]:
        """Retrieve data from path"""
        pass
    
    @abstractmethod
    def head(self, path: str) -> bool:
        """Check if path exists"""
        pass
    
    @abstractmethod
    def delete(self, path: str) -> None:
        """Delete data at path"""
        pass
    
    @abstractmethod
    def list(self, dir_path: str) -> List[str]:
        """List files in directory"""
        pass
    
    def put_chunk(self, request_id: str, chunk_num: int, data: bytes, direction: str = 'c') -> None:
        """Store binary chunk data"""
        pass
    
    def get_chunk(self, request_id: str, chunk_num: int, direction: str = 's') -> Optional[bytes]:
        """Retrieve binary chunk data"""
        return None
    
    def head_chunk(self, request_id: str, chunk_num: int, direction: str = 's') -> bool:
        """Check if chunk exists"""
        return False
    
    def delete_chunk(self, request_id: str, chunk_num: int, direction: str = 's') -> None:
        """Delete chunk"""
        pass
    
    def get_pending_request(self) -> Optional[tuple]:
        """Get next pending request. Returns (request_id, data) or None."""
        return None
