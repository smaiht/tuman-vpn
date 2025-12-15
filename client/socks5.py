"""SOCKS5 Proxy server for TumanVPN"""

import socket
import struct
import threading
import datetime
import json
import time
import uuid
from core.storage.base import BaseStorage
from .tunnel import TunnelHandler

# SOCKS5 constants
SOCKS_VERSION = 5
AUTH_NONE = 0
CMD_CONNECT = 1
ATYP_IPV4 = 1
ATYP_DOMAIN = 3
ATYP_IPV6 = 4

PRINT_LOCK = threading.Lock()


def log(msg: str):
    with PRINT_LOCK:
        ts = datetime.datetime.now().strftime('%H:%M:%S.%f')[:-3]
        print(f"[{ts}] {msg}")


def generate_request_id() -> str:
    return f"{int(time.time() * 1000)}{uuid.uuid4().hex[:3]}"


class Socks5Handler:
    """Handles a single SOCKS5 connection"""
    
    def __init__(self, client_socket: socket.socket, storage: BaseStorage, config: dict):
        self.client = client_socket
        self.storage = storage
        self.config = config
        self.timeout = config.get('timeout', 120)
        self.cleanup_chunks = config.get('cleanup_chunks', True)
        self.tunnel_handler = TunnelHandler(storage, config)
    
    def handle(self):
        try:
            # 1. Greeting
            if not self._handle_greeting():
                return
            
            # 2. Request
            cmd, addr_type, dest_addr, dest_port = self._handle_request()
            if cmd is None:
                return
            
            if cmd != CMD_CONNECT:
                self._send_reply(7)  # Command not supported
                return
            
            # 3. Connect through tunnel
            log(f"[→] SOCKS5 CONNECT {dest_addr}:{dest_port}")
            
            request_id = generate_request_id()
            
            # For HTTPS-like connections, use tunnel
            self._handle_connect(request_id, dest_addr, dest_port)
            
        except Exception as e:
            log(f"[!] SOCKS5 error: {e}")
        finally:
            try:
                self.client.close()
            except:
                pass
    
    def _handle_greeting(self) -> bool:
        """Handle SOCKS5 greeting"""
        # Read version and auth methods
        data = self.client.recv(2)
        if len(data) < 2:
            return False
        
        version, nmethods = struct.unpack('!BB', data)
        if version != SOCKS_VERSION:
            return False
        
        # Read auth methods
        methods = self.client.recv(nmethods)
        
        # We only support no auth
        if AUTH_NONE not in methods:
            self.client.send(struct.pack('!BB', SOCKS_VERSION, 0xFF))
            return False
        
        # Accept no auth
        self.client.send(struct.pack('!BB', SOCKS_VERSION, AUTH_NONE))
        return True
    
    def _handle_request(self):
        """Handle SOCKS5 request, return (cmd, addr_type, dest_addr, dest_port)"""
        # Read request header
        data = self.client.recv(4)
        if len(data) < 4:
            return None, None, None, None
        
        version, cmd, _, addr_type = struct.unpack('!BBBB', data)
        
        if version != SOCKS_VERSION:
            return None, None, None, None
        
        # Read destination address
        if addr_type == ATYP_IPV4:
            addr_data = self.client.recv(4)
            dest_addr = socket.inet_ntoa(addr_data)
        elif addr_type == ATYP_DOMAIN:
            domain_len = self.client.recv(1)[0]
            dest_addr = self.client.recv(domain_len).decode('utf-8')
        elif addr_type == ATYP_IPV6:
            addr_data = self.client.recv(16)
            dest_addr = socket.inet_ntop(socket.AF_INET6, addr_data)
        else:
            self._send_reply(8)  # Address type not supported
            return None, None, None, None
        
        # Read port
        port_data = self.client.recv(2)
        dest_port = struct.unpack('!H', port_data)[0]
        
        return cmd, addr_type, dest_addr, dest_port
    
    def _send_reply(self, status: int, bind_addr: str = '0.0.0.0', bind_port: int = 0):
        """Send SOCKS5 reply"""
        reply = struct.pack('!BBBB', SOCKS_VERSION, status, 0, ATYP_IPV4)
        reply += socket.inet_aton(bind_addr)
        reply += struct.pack('!H', bind_port)
        self.client.send(reply)
    
    def _handle_connect(self, request_id: str, host: str, port: int):
        """Handle CONNECT command - tunnel through Yandex Notes"""
        t0 = time.time()
        
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
            log(f"[!] Upload error: {e}")
            self._send_reply(1)  # General failure
            return
        
        # Wait for server confirmation
        tunnel_ready = False
        t_wait_start = time.time()
        
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
        
        if not tunnel_ready:
            log(f"[!] Tunnel setup failed for {host}:{port}")
            self._send_reply(4)  # Host unreachable
            return
        
        # Send success reply
        self._send_reply(0)
        
        t_setup = time.time()
        log(f"[🔒] SOCKS5 tunnel ready: {host}:{port} ({int((t_setup-t0)*1000)}ms)")
        
        # Bidirectional data transfer
        self._tunnel_data(request_id, host, port)
    
    def _tunnel_data(self, request_id: str, host: str, port: int):
        """Bidirectional tunnel data transfer"""
        self.client.setblocking(0)
        
        chunk_size = self.config.get('chunk_size', 7800)
        chunk_idle_timeout = self.config.get('chunk_idle_timeout', 0.1)
        tunnel_idle_timeout = self.config.get('tunnel_idle_timeout', 120)
        
        buffer_to_server = b''
        chunk_id_sent = 0
        chunk_id_received = 0
        bytes_sent = 0
        bytes_received = 0
        last_data_time = time.time()
        last_activity_time = time.time()
        
        while True:
            chunk_found = False
            
            # Read from client
            try:
                data = self.client.recv(chunk_size)
                if data:
                    buffer_to_server += data
                    last_data_time = time.time()
                    last_activity_time = time.time()
                elif data == b'':
                    break  # Client closed
            except BlockingIOError:
                pass
            except:
                break
            
            # Send buffered data on size or idle
            if buffer_to_server:
                should_send = (len(buffer_to_server) >= chunk_size or 
                              time.time() - last_data_time >= chunk_idle_timeout)
                if should_send:
                    chunk_id_sent += 1
                    self.storage.put_chunk(request_id, chunk_id_sent, buffer_to_server, 'c')
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
                        self.client.setblocking(1)
                        try:
                            self.client.sendall(data)
                        finally:
                            self.client.setblocking(0)
                        bytes_received += len(data)
                        chunk_id_received = expected_chunk
                        if self.cleanup_chunks:
                            self.storage.delete_chunk(request_id, expected_chunk, 's')
                        chunk_found = True
                        last_activity_time = time.time()
            except:
                pass
            
            # Check idle timeout
            if time.time() - last_activity_time > tunnel_idle_timeout:
                log(f"    [{request_id}] Idle timeout")
                break
            
            if not chunk_found:
                time.sleep(0.05)
        
        # Flush remaining
        if buffer_to_server:
            chunk_id_sent += 1
            self.storage.put_chunk(request_id, chunk_id_sent, buffer_to_server, 'c')
            bytes_sent += len(buffer_to_server)
        
        log(f"[✓] SOCKS5 tunnel closed: {host}:{port}")
        log(f"    [📊] sent: {bytes_sent}b | received: {bytes_received}b")


class Socks5Server:
    """SOCKS5 proxy server"""
    
    def __init__(self, port: int, storage: BaseStorage, config: dict):
        self.port = port
        self.storage = storage
        self.config = config
        self.server_socket = None
        self.running = False
    
    def start(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('0.0.0.0', self.port))
        self.server_socket.listen(100)
        self.running = True
        
        log(f"[*] SOCKS5 server listening on 0.0.0.0:{self.port}")
        
        while self.running:
            try:
                client_socket, addr = self.server_socket.accept()
                handler = Socks5Handler(client_socket, self.storage, self.config)
                thread = threading.Thread(target=handler.handle, daemon=True)
                thread.start()
            except Exception as e:
                if self.running:
                    log(f"[!] Accept error: {e}")
    
    def stop(self):
        self.running = False
        if self.server_socket:
            self.server_socket.close()
