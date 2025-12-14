"""HTTP Proxy server"""

import http.server
import json
import uuid
import time
import threading
import datetime
from urllib.parse import urlparse
from core.storage.base import BaseStorage
from .tunnel import TunnelHandler


def generate_request_id() -> str:
    """Generate unique request ID: timestamp_ms (13 digits) + random (3 hex chars)"""
    return f"{int(time.time() * 1000)}{uuid.uuid4().hex[:3]}"

# Blocked domains
BLOCKED_DOMAINS = [
    # 'detectportal.firefox.com',
    # 'firefox.settings.services.mozilla.com',
    # 'incoming.telemetry.mozilla.org',
    # 'safebrowsing.googleapis.com',
    # 'google-analytics.com',
    # 'googletagmanager.com',
    # 'mc.yandex.ru',
    # 'doubleclick.net',
    # 'googlesyndication.com',
    # 'connect.facebook.net',

    # 'log.strm.yandex.ru',
    # 'log.rutube.ru',
    # 'tns-counter.ru',
    # 'ads.mozilla.org',
    # 'avatars.mds.yandex.net',
    # 'pretarg.adhigh.net',
    # 'banners.adfox.ru',
    # '941221.log.rutube.ru',

    # 'download-installer.cdn.mozilla.net',
    # 'normandy.cdn.mozilla.net',
    # 'services.addons.mozilla.org',
    # '',
]

PRINT_LOCK = threading.Lock()
SEEN_DOMAINS = set()


def log_with_time(*args):
    with PRINT_LOCK:
        ts = datetime.datetime.now().strftime('%H:%M:%S.%f')[:-3]
        print(f"[{ts}]", *args)


def log_new_domain(domain: str):
    if domain not in SEEN_DOMAINS:
        SEEN_DOMAINS.add(domain)


def is_blocked(host: str) -> bool:
    for blocked in BLOCKED_DOMAINS:
        if blocked in host:
            return True
    return False


class ProxyHandler(http.server.BaseHTTPRequestHandler):
    """HTTP Proxy request handler"""
    
    storage: BaseStorage = None
    config: dict = None
    cleanup_chunks: bool = True
    tunnel_handler: TunnelHandler = None
    
    def _do_request(self, method: str):
        # Extract domain
        try:
            parsed = urlparse(self.path)
            domain = parsed.netloc or parsed.path.split('/')[0]
            log_new_domain(domain)
        except:
            domain = self.path
        
        if is_blocked(domain):
            log_with_time(f"[🚫] Blocked {method} {self.path[:60]}")
            return
        
        request_id = generate_request_id()
        t0 = time.time()
        
        # Read body
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length) if content_length else b''
        
        request_data = {
            'id': request_id,
            'method': method,
            'url': self.path,
            'headers': dict(self.headers),
            'body': body.decode('utf-8', errors='ignore') if body else ''
        }
        
        # Send request
        try:
            self.storage.put(f"outbox/{request_id}.json", json.dumps(request_data))
        except Exception as e:
            log_with_time(f"[!] Upload error: {e}")
            self.send_error(502, f"Failed to upload request: {e}")
            return
        
        t1 = time.time()
        log_with_time(f"[→] {method} {self.path[:60]} (id={request_id}) [{int((t1-t0)*1000)}ms]")
        
        # Wait for response
        timeout = self.config.get('timeout', 120)
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                if self.storage.head(f"inbox/{request_id}.json"):
                    content = self.storage.get(f"inbox/{request_id}.json")
                    
                    try:
                        response_data = json.loads(content)
                    except:
                        time.sleep(0.5)
                        continue
                    
                    if self.cleanup_chunks:
                        self.storage.delete(f"inbox/{request_id}.json")
                    
                    # Send response to client
                    self.send_response(response_data.get('status', 200))
                    for key, value in response_data.get('headers', {}).items():
                        if key.lower() not in ['transfer-encoding', 'content-encoding', 'content-length']:
                            try:
                                self.send_header(key, value)
                            except:
                                pass
                    
                    body = response_data.get('body', '').encode('utf-8')
                    self.send_header('Content-Length', len(body))
                    self.end_headers()
                    self.wfile.write(body)
                    
                    elapsed = time.time() - start_time
                    log_with_time(f"[←] {response_data.get('status')} ({len(body)}b) [{elapsed:.2f}s]")
                    return
            except Exception as e:
                pass
            
            time.sleep(0.1)
        
        self.send_error(504, "Gateway Timeout")
        log_with_time(f"[!] Timeout for {request_id}")
    
    def do_GET(self):
        self._do_request('GET')
    
    def do_POST(self):
        self._do_request('POST')
    
    def do_PUT(self):
        self._do_request('PUT')
    
    def do_DELETE(self):
        self._do_request('DELETE')
    
    def do_HEAD(self):
        self._do_request('HEAD')
    
    def do_CONNECT(self):
        request_id = generate_request_id()
        
        try:
            host, port = self.path.split(':')
            port = int(port)
        except:
            self.send_error(400, "Bad CONNECT request")
            return
        
        log_new_domain(host)
        
        if is_blocked(host):
            log_with_time(f"[🚫] Blocked CONNECT {host}:{port}")
            return
        
        self.tunnel_handler.handle(request_id, host, port, self.connection, self)
    
    def log_message(self, format, *args):
        pass  # Silent mode


def create_handler(storage: BaseStorage, config: dict):
    """Create proxy handler class with storage and config"""
    
    class ConfiguredProxyHandler(ProxyHandler):
        pass
    
    ConfiguredProxyHandler.storage = storage
    ConfiguredProxyHandler.config = config
    ConfiguredProxyHandler.cleanup_chunks = config.get('cleanup_chunks', True)
    ConfiguredProxyHandler.tunnel_handler = TunnelHandler(storage, config)
    
    return ConfiguredProxyHandler
