"""CONNECT tunnel handler for server side"""

import json
import time
import socket
import select
import threading
import traceback
from core.storage.base import BaseStorage


class ServerTunnelHandler:
    """Handles HTTPS CONNECT tunnels on server side"""
    
    def __init__(self, storage: BaseStorage, config: dict):
        self.storage = storage
        self.config = config
        self.chunk_size = config.get('chunk_size', 7800)
        self.chunk_idle_timeout = config.get('chunk_idle_timeout', 0.1)
        self.cleanup_chunks = config.get('cleanup_chunks', True)
        self.tunnel_idle_timeout = config.get('tunnel_idle_timeout', 120)  # seconds
        self.active_tunnels = {}
        self._tunnels_lock = threading.Lock()
        self._print_lock = threading.Lock()
    
    def _log(self, msg: str):
        import datetime
        with self._print_lock:
            ts = datetime.datetime.now().strftime('%H:%M:%S.%f')[:-3]
            print(f"[{ts}] {msg}")
    
    def handle(self, req: dict):
        """Handle CONNECT request in background thread"""
        thread = threading.Thread(
            target=self._tunnel_worker,
            args=(req,),
            daemon=True
        )
        thread.start()
        return thread
    
    def _tunnel_worker(self, req: dict):
        request_id = req['id']
        host = req['host']
        port = req['port']
        
        try:
            self._log(f"[🔒] CONNECT tunnel: {host}:{port} (id={request_id})")
            
            # Connect to target
            t_connect = time.time()
            target_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            target_socket.settimeout(10)
            target_socket.connect((host, port))
            target_socket.setblocking(0)
            
            self._log(f"    [✓] Connected to {host}:{port} ({int((time.time()-t_connect)*1000)}ms)")
            
            # Send success response
            response_data = {
                'id': request_id,
                'status': 200,
                'headers': {},
                'body': 'Connection Established'
            }
            self.storage.put(f"inbox/{request_id}.json", json.dumps(response_data))
            
            # Track tunnel
            with self._tunnels_lock:
                self.active_tunnels[request_id] = {
                    'socket': target_socket,
                    'host': host,
                    'port': port,
                    'start': time.time(),
                    'bytes_sent': 0,
                    'bytes_received': 0
                }
            
            # Tunnel loop
            self._tunnel_loop(request_id, host, port, target_socket)
            
        except Exception as e:
            self._log(f"[!] Tunnel error ({host}:{port}): {e}")
            traceback.print_exc()
            with self._tunnels_lock:
                if request_id in self.active_tunnels:
                    del self.active_tunnels[request_id]
    
    def _tunnel_loop(self, request_id: str, host: str, port: int, target_socket):
        buffer_from_target = b''
        last_chunk_received = 0
        last_chunk_sent = 0
        last_data_time = time.time()
        last_activity_time = time.time()  # for idle timeout
        
        while True:
            chunk_found = False
            
            # Check for client data
            expected_chunk = last_chunk_received + 1
            try:
                if self.storage.head_chunk(request_id, expected_chunk, 'c'):
                    data = self.storage.get_chunk(request_id, expected_chunk, 'c')
                    if data:
                        # Temporarily set blocking for large sends
                        target_socket.setblocking(1)
                        try:
                            target_socket.sendall(data)
                        finally:
                            target_socket.setblocking(0)
                        self.active_tunnels[request_id]['bytes_sent'] += len(data)
                        self._log(f"    [{request_id}] CLIENT→{host}:{port} chunk {expected_chunk} ({len(data)}b)")
                        if self.cleanup_chunks:
                            self.storage.delete_chunk(request_id, expected_chunk, 'c')
                        last_chunk_received = expected_chunk
                        last_activity_time = time.time()
                        chunk_found = True
            except Exception as e:
                if "404" not in str(e) and "does not exist" not in str(e):
                    self._log(f"    [{request_id}] Send to target error: {e}")
            
            # Check for target data
            try:
                readable, _, _ = select.select([target_socket], [], [], 0.001)
                
                if readable:
                    data = target_socket.recv(self.chunk_size)
                    if not data:
                        self._log(f"    [!] Target closed: {host}:{port}")
                        break
                    
                    if len(buffer_from_target) + len(data) > self.chunk_size:
                        if len(buffer_from_target) > 0:
                            last_chunk_sent += 1
                            self._send_chunk(request_id, last_chunk_sent, buffer_from_target, host, port)
                            self.active_tunnels[request_id]['bytes_received'] += len(buffer_from_target)
                            chunk_found = True
                        buffer_from_target = data
                    else:
                        buffer_from_target += data
                    
                    last_data_time = time.time()
                    last_activity_time = time.time()
                
                # Send on idle timeout
                if len(buffer_from_target) > 0:
                    idle_time = time.time() - last_data_time
                    if idle_time >= self.chunk_idle_timeout:
                        last_chunk_sent += 1
                        self._log(f"    [{request_id}] IDLE {idle_time:.2f}s → sending {len(buffer_from_target)}b")
                        self._send_chunk(request_id, last_chunk_sent, buffer_from_target, host, port)
                        self.active_tunnels[request_id]['bytes_received'] += len(buffer_from_target)
                        buffer_from_target = b''
                        last_data_time = time.time()
                        chunk_found = True
                    
            except socket.error as e:
                self._log(f"    [!] Socket error: {e}")
                break
            
            # Check idle timeout
            if time.time() - last_activity_time > self.tunnel_idle_timeout:
                self._log(f"    [{request_id}] Idle timeout ({self.tunnel_idle_timeout}s)")
                break
            
            if not chunk_found:
                time.sleep(0.05)
        
        # Flush remaining buffer before closing
        if len(buffer_from_target) > 0:
            last_chunk_sent += 1
            self._send_chunk(request_id, last_chunk_sent, buffer_from_target, host, port)
            self.active_tunnels[request_id]['bytes_received'] += len(buffer_from_target)
        
        # Cleanup
        target_socket.close()
        tunnel_info = self.active_tunnels.get(request_id, {})
        elapsed = time.time() - tunnel_info.get('start', time.time())
        
        self._log(f"[✓] Tunnel closed: {host}:{port} (id={request_id})")
        self._log(f"    [⏱️] Duration: {elapsed:.1f}s")
        self._log(f"    [📊] Sent: {tunnel_info.get('bytes_sent', 0)}b | Recv: {tunnel_info.get('bytes_received', 0)}b")
        
        if self.cleanup_chunks:
            for i in range(1, last_chunk_received + 1):
                try:
                    self.storage.delete_chunk(request_id, i, 'c')
                except:
                    pass
        
        with self._tunnels_lock:
            if request_id in self.active_tunnels:
                del self.active_tunnels[request_id]
    
    def _send_chunk(self, request_id: str, chunk_num: int, data: bytes, host: str = '', port: int = 0):
        self.storage.put_chunk(request_id, chunk_num, data, 's')
        self._log(f"    [{request_id}] {host}:{port}→CLIENT chunk {chunk_num} ({len(data)}b)")
