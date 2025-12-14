"""CONNECT tunnel handler for HTTPS connections"""

import json
import time
import select
import threading
from core.storage.base import BaseStorage


class TunnelHandler:
    """Handles HTTPS CONNECT tunnels"""
    
    def __init__(self, storage: BaseStorage, config):
        self.storage = storage
        self.config = config
        self.chunk_size = config.get('chunk_size', 7800)
        self.chunk_idle_timeout = config.get('chunk_idle_timeout', 0.1)
        self.cleanup_chunks = config.get('cleanup_chunks', True)
        self.tunnel_idle_timeout = config.get('tunnel_idle_timeout', 120)  # seconds
        self._print_lock = threading.Lock()
    
    def _log(self, msg: str):
        import datetime
        with self._print_lock:
            ts = datetime.datetime.now().strftime('%H:%M:%S.%f')[:-3]
            print(f"[{ts}] {msg}")
    
    def handle(self, request_id: str, host: str, port: int, client_socket, handler):
        """
        Handle CONNECT tunnel.
        
        Args:
            request_id: Unique request ID
            host: Target host
            port: Target port
            client_socket: Client socket connection
            handler: HTTP handler instance for sending response
        """
        t0 = time.time()
        self._log(f"[→] CONNECT {host}:{port} (id={request_id})")
        
        # Send tunnel request to server
        request_data = {
            'id': request_id,
            'method': 'CONNECT',
            'host': host,
            'port': port
        }
        
        try:
            self.storage.put(f"outbox/{request_id}.json", json.dumps(request_data))
        except Exception as e:
            self._log(f"[!] Upload error: {e}")
            handler.send_error(502, f"Failed to upload CONNECT request")
            return
        
        # Wait for server confirmation
        t_wait_start = time.time()
        tunnel_ready = False
        
        while time.time() - t_wait_start < 30:
            try:
                if self.storage.head(f"inbox/{request_id}.json"):
                    content = self.storage.get(f"inbox/{request_id}.json")
                    response_data = json.loads(content)
                    
                    if response_data.get('status') == 200:
                        tunnel_ready = True
                        if self.cleanup_chunks:
                            self.storage.delete(f"inbox/{request_id}.json")
                        break
            except:
                pass
            time.sleep(0.3)
        
        t_setup = time.time()
        
        if not tunnel_ready:
            handler.send_error(504, "Gateway Timeout - tunnel setup failed")
            self._log(f"[!] CONNECT timeout after {t_setup - t0:.1f}s")
            return
        
        # Send OK to client
        handler.send_response(200, 'Connection Established')
        handler.send_header('Proxy-agent', 'TumanVPN')
        handler.end_headers()
        
        self._log(f"[🔒] Tunnel ready: {host}:{port} (setup: {(t_setup - t0)*1000:.0f}ms)")
        
        # Bidirectional tunnel
        client_socket.setblocking(0)
        
        buffer_to_server = b''
        chunk_id_sent = 0
        chunk_id_received = 0
        bytes_sent = 0
        bytes_received = 0
        t_tunnel_start = time.time()
        last_data_time = time.time()
        last_activity_time = time.time()  # for idle timeout
        
        while True:
            chunk_found = False
            
            # Check for client data
            readable, _, _ = select.select([client_socket], [], [], 0.001)
            
            if readable:
                try:
                    data = client_socket.recv(self.chunk_size)
                    if not data:
                        break
                    
                    if len(buffer_to_server) + len(data) > self.chunk_size:
                        if len(buffer_to_server) > 0:
                            chunk_id_sent += 1
                            self._send_chunk(request_id, chunk_id_sent, buffer_to_server)
                            bytes_sent += len(buffer_to_server)
                            chunk_found = True
                        buffer_to_server = data
                    else:
                        buffer_to_server += data
                    
                    last_data_time = time.time()
                    last_activity_time = time.time()
                except:
                    break
            
            # Send buffered data on idle timeout
            if len(buffer_to_server) > 0:
                idle_time = time.time() - last_data_time
                if idle_time >= self.chunk_idle_timeout:
                    chunk_id_sent += 1
                    self._log(f"    [{request_id}] IDLE {idle_time:.2f}s → sending {len(buffer_to_server)}b")
                    self._send_chunk(request_id, chunk_id_sent, buffer_to_server)
                    bytes_sent += len(buffer_to_server)
                    buffer_to_server = b''
                    last_data_time = time.time()
                    chunk_found = True
            
            # Check for server data
            try:
                expected_chunk = chunk_id_received + 1
                if self.storage.head_chunk(request_id, expected_chunk, 's'):
                    data = self.storage.get_chunk(request_id, expected_chunk, 's')
                    if data:
                        # Temporarily set blocking for large sends
                        client_socket.setblocking(1)
                        try:
                            client_socket.sendall(data)
                        finally:
                            client_socket.setblocking(0)
                        bytes_received += len(data)
                        chunk_id_received = expected_chunk
                        self._log(f"    [{request_id}] SERVER→BROWSER chunk {chunk_id_received} ({len(data)}b)")
                        if self.cleanup_chunks:
                            self.storage.delete_chunk(request_id, expected_chunk, 's')
                        chunk_found = True
                        last_activity_time = time.time()
            except Exception as e:
                if "404" not in str(e) and "does not exist" not in str(e):
                    self._log(f"    [{request_id}] Send error: {e}")
            
            # Check idle timeout
            if time.time() - last_activity_time > self.tunnel_idle_timeout:
                self._log(f"    [{request_id}] Idle timeout ({self.tunnel_idle_timeout}s)")
                break
            
            if not chunk_found:
                time.sleep(0.05)
        
        # Flush remaining buffer before closing
        if len(buffer_to_server) > 0:
            chunk_id_sent += 1
            self._send_chunk(request_id, chunk_id_sent, buffer_to_server)
            bytes_sent += len(buffer_to_server)
        
        t_total = time.time() - t0
        t_tunnel = time.time() - t_tunnel_start
        self._log(f"[✓] Tunnel closed: {host}:{port} (id={request_id})")
        self._log(f"    [⏱️] setup: {(t_setup - t0)*1000:.0f}ms | tunnel: {t_tunnel:.1f}s")
        self._log(f"    [📊] sent: {bytes_sent}b | received: {bytes_received}b")
        
        # Cleanup
        if self.cleanup_chunks:
            for i in range(1, chunk_id_received + 1):
                try:
                    self.storage.delete_chunk(request_id, i, 's')
                except:
                    pass
    
    def _send_chunk(self, request_id: str, chunk_num: int, data: bytes):
        self.storage.put_chunk(request_id, chunk_num, data, 'c')
        self._log(f"    [{request_id}] BROWSER→SERVER chunk {chunk_num} ({len(data)}b)")
