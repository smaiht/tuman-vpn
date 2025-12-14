"""TumanVPN Server (Worker) entry point"""

import sys
import time
import threading
import datetime
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.config import Config
from core.storage import get_storage
from server.handler import RequestHandler


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
    storage = get_storage(config.mode, storage_config, role='server')
    
    # Create request handler
    handler = RequestHandler(storage, config.settings)
    
    poll_interval = config.get('poll_interval', 0.3)
    
    log(f"[*] TumanVPN Server started")
    log(f"[*] Storage mode: {config.mode}")
    log(f"[*] Poll interval: {poll_interval}s")
    print()
    
    active_threads = []
    
    while True:
        try:
            # Event-driven: YaNotes
            pending = storage.get_pending_request()
            if pending:
                request_id, data = pending
                thread = threading.Thread(
                    target=handler.process_request,
                    args=(request_id, data),
                    daemon=True
                )
                thread.start()
                active_threads.append(thread)
            
            # Cleanup finished threads
            active_threads = [t for t in active_threads if t.is_alive()]
            
            time.sleep(poll_interval)
            
        except KeyboardInterrupt:
            log("\n[*] Stopping...")
            log(f"[*] Waiting for {len(active_threads)} threads...")
            for t in active_threads:
                t.join(timeout=5)
            break
        except Exception as e:
            log(f"[!] Main loop error: {e}")
            time.sleep(1)


if __name__ == "__main__":
    main()
