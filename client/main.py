"""TumanVPN Client entry point"""

import sys
import socketserver
import datetime
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.config import Config
from core.storage import get_storage
from client.proxy import create_handler


def log(msg: str):
    ts = datetime.datetime.now().strftime('%H:%M:%S.%f')[:-3]
    print(f"[{ts}] {msg}")


def main():
    config = Config.load()
    if not config:
        print("[!] Конфиг не найден. Запустите ./setup.sh")
        sys.exit(1)
    
    # Initialize storage (pass cleanup_chunks from settings)
    storage_config = {**config.storage, 'cleanup_chunks': config.get('cleanup_chunks', True)}
    storage = get_storage(config.mode, storage_config, role='client')
    
    # Create proxy handler
    handler = create_handler(storage, config.settings)
    
    port = config.get('proxy_port', 8080)
    
    log(f"[*] TumanVPN Client started")
    log(f"[*] Proxy: http://127.0.0.1:{port}")
    log(f"[*] Storage mode: {config.mode}")
    log(f"[*] Timeout: {config.get('timeout')}s")
    log(f"[*] Configure your browser to use proxy 127.0.0.1:{port}")
    print()
    
    # Start server
    socketserver.ThreadingTCPServer.allow_reuse_address = True
    
    with socketserver.ThreadingTCPServer(("0.0.0.0", port), handler) as httpd:
        log(f"[*] Multithreaded mode - each connection in separate thread")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            log("\n[*] Stopped")


if __name__ == "__main__":
    main()
