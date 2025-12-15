"""TumanVPN Client entry point"""

import sys
import socketserver
import datetime
import threading
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.config import Config
from core.storage import get_storage
from client.proxy import create_handler
from client.socks5 import Socks5Server


def log(msg: str):
    ts = datetime.datetime.now().strftime('%H:%M:%S.%f')[:-3]
    print(f"[{ts}] {msg}")


def main():
    config = Config.load()
    if not config:
        print("[!] Конфиг не найден. Запустите ./setup.sh")
        sys.exit(1)
    
    # Initialize storage
    storage_config = {**config.storage, 'cleanup_chunks': config.get('cleanup_chunks', True)}
    storage = get_storage(config.mode, storage_config, role='client')
    
    # Get proxy mode from config
    proxy_mode = config.get('proxy_mode', 'http')  # 'http', 'socks5', or 'both'
    http_port = config.get('proxy_port', 8080)
    socks5_port = config.get('socks5_port', 1080)
    
    log(f"[*] TumanVPN Client started")
    log(f"[*] Storage mode: {config.mode}")
    log(f"[*] Proxy mode: {proxy_mode}")
    log(f"[*] Timeout: {config.get('timeout')}s")
    print()
    
    threads = []
    
    # Start HTTP proxy
    if proxy_mode in ('http', 'both'):
        handler = create_handler(storage, config.settings)
        socketserver.ThreadingTCPServer.allow_reuse_address = True
        http_server = socketserver.ThreadingTCPServer(("0.0.0.0", http_port), handler)
        
        http_thread = threading.Thread(target=http_server.serve_forever, daemon=True)
        http_thread.start()
        threads.append(('HTTP', http_server, http_thread))
        
        log(f"[*] HTTP Proxy: http://0.0.0.0:{http_port}")
    
    # Start SOCKS5 proxy
    if proxy_mode in ('socks5', 'both'):
        socks5_server = Socks5Server(socks5_port, storage, config.settings)
        
        socks5_thread = threading.Thread(target=socks5_server.start, daemon=True)
        socks5_thread.start()
        threads.append(('SOCKS5', socks5_server, socks5_thread))
        
        log(f"[*] SOCKS5 Proxy: socks5://0.0.0.0:{socks5_port}")
    
    print()
    log(f"[*] Ready! Configure your client to use the proxy")
    
    if proxy_mode == 'socks5':
        log(f"[*] For tun2socks: tun2socks -device tun0 -proxy socks5://127.0.0.1:{socks5_port}")
    
    print()
    
    # Wait for interrupt
    try:
        while True:
            for name, server, thread in threads:
                if not thread.is_alive():
                    log(f"[!] {name} server died, restarting...")
            threading.Event().wait(1)
    except KeyboardInterrupt:
        log("\n[*] Stopping...")
        for name, server, thread in threads:
            if hasattr(server, 'shutdown'):
                server.shutdown()
            elif hasattr(server, 'stop'):
                server.stop()
        log("[*] Stopped")


if __name__ == "__main__":
    main()
