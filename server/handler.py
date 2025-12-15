"""HTTP request handler for server side"""

import json
import time
import traceback
import requests
import threading
from core.storage.base import BaseStorage
from .tunnel import ServerTunnelHandler


class RequestHandler:
    """Handles HTTP requests on server side"""
    
    def __init__(self, storage: BaseStorage, config: dict):
        self.storage = storage
        self.config = config
        self.cleanup_chunks = config.get('cleanup_chunks', True)
        self.tunnel_handler = ServerTunnelHandler(storage, config)
        self._print_lock = threading.Lock()
    
    def _log(self, msg: str):
        import datetime
        with self._print_lock:
            ts = datetime.datetime.now().strftime('%H:%M:%S.%f')[:-3]
            print(f"[{ts}] {msg}")
    
    def process_request(self, request_id: str, data: bytes):
        """Process incoming request"""
        try:
            req = json.loads(data)
            method = req.get('method')
            
            self._log(f"[→] {method} request (id={request_id})")
            
            if method == 'CONNECT':
                self.tunnel_handler.handle(req)
                return
            
            if method == 'CLOSE':
                # Tunnel close request - just log it, tunnel already closed
                self._log(f"[←] Tunnel closed by client (id={request_id})")
                return
            
            self._execute_http(req, request_id)
            
        except Exception as e:
            self._log(f"[!] Error processing request {request_id}: {e}")
            traceback.print_exc()
    
    def _execute_http(self, req: dict, request_id: str):
        """Execute HTTP request and send response"""
        t_exec = time.time()
        url = req.get('url')
        method = req.get('method')
        headers = req.get('headers', {})
        body = req.get('body', '')
        
        # Remove problematic headers
        for h in ['Host', 'Connection', 'Proxy-Connection', 'Content-Length']:
            headers.pop(h, None)
        
        self._log(f"    [🌐] Executing: {method} {url[:60]}")
        
        try:
            resp = requests.request(
                method=method,
                url=url,
                headers=headers,
                data=body.encode('utf-8') if body else None,
                timeout=30,
                allow_redirects=True
            )
            
            self._log(f"    [✓] {resp.status_code} ({len(resp.content)}b) [{int((time.time()-t_exec)*1000)}ms]")
            
            response_data = {
                'id': request_id,
                'status': resp.status_code,
                'headers': dict(resp.headers),
                'body': resp.text
            }
            
        except Exception as e:
            self._log(f"    [!] Request failed: {e}")
            
            response_data = {
                'id': request_id,
                'status': 502,
                'headers': {'Content-Type': 'text/plain'},
                'body': f'Error executing request: {str(e)}'
            }
        
        # Send response
        t_upload = time.time()
        self.storage.put(f"inbox/{request_id}.json", json.dumps(response_data))
        
        self._log(f"    [←] Response uploaded [{int((time.time()-t_upload)*1000)}ms]")
