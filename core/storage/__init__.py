"""Storage backends factory"""

from .base import BaseStorage
from .yanotes import YaNotesStorage

STORAGE_TYPES = {
    'yanotes': YaNotesStorage,
}


def get_storage(mode: str, config: dict, role: str = None) -> BaseStorage:
    """Factory function to get storage backend by mode"""
    if mode not in STORAGE_TYPES:
        raise ValueError(f"Unknown storage mode: {mode}. Available: {list(STORAGE_TYPES.keys())}")
    
    storage_class = STORAGE_TYPES[mode]
    return storage_class(config, role=role)


__all__ = ['BaseStorage', 'get_storage', 'STORAGE_TYPES']
