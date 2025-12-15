"""Configuration management"""

import json
from pathlib import Path
from typing import Optional

DEFAULT_SETTINGS = {
    "proxy_mode": "http",       # 'http', 'socks5', or 'both'
    "proxy_port": 8080,         # HTTP proxy port
    "socks5_port": 1080,        # SOCKS5 proxy port
    "timeout": 120,
    "chunk_size": 500000,
    "chunk_idle_timeout": 0.1,
    "poll_interval": 0.1,
    "cleanup_chunks": True,
    "tunnel_idle_timeout": 120,
}

DATA_DIR = Path(__file__).parent.parent / "data"
CONFIG_FILE = DATA_DIR / "config.json"


class Config:
    """Application configuration"""
    
    def __init__(self, mode: str, storage: dict, settings: dict = None):
        self.mode = mode
        self.storage = storage
        self.settings = {**DEFAULT_SETTINGS, **(settings or {})}
    
    @classmethod
    def load(cls) -> Optional['Config']:
        """Load config from file"""
        if not CONFIG_FILE.exists():
            return None
        
        try:
            data = json.loads(CONFIG_FILE.read_text())
            return cls(
                mode=data["mode"],
                storage=data["storage"],
                settings=data.get("settings"),
            )
        except Exception:
            return None
    
    def save(self):
        """Save config to file"""
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        data = {
            "mode": self.mode,
            "storage": self.storage,
            "settings": self.settings,
        }
        CONFIG_FILE.write_text(json.dumps(data, indent=2))
    
    def get(self, key: str, default=None):
        """Get setting value"""
        return self.settings.get(key, default)
