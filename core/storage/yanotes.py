"""Yandex Notes storage backend (pool-based batching)

>> = SEND (отправлено на Яндекс)
<< = RECV (получено от Яндекса)

chars = символы в snippet (base65536)
bytes = реальные байты данных (~chars * 2)
"""

import http.cookiejar as cookiejar
import os
import queue
import threading
import time
import re
import random
import hashlib
from concurrent.futures import ThreadPoolExecutor
import traceback
from typing import List, Optional

import requests
import base65536
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from .base import BaseStorage


# Config
MAX_SNIPPET_CHARS = 2_000_000  # 2M chars (~4MB bytes)
BATCH_TIMEOUT = 0.3           # Отправляем batch каждые 300ms
POLL_INTERVAL = 0.1           # Polling каждые 100ms
STALE_TIMEOUT_MS = 10 * 60 * 1000  # 10 минут - для долгих видео (чанки не накапливаются, они потребляются)
DEBUG_QUEUE = False           # Логировать каждый item в очереди

TYPE_DATA = 'DATA'
TYPE_RQST = 'RQST'
TYPE_RESP = 'RESP'

BASE_URL = "https://cloud-api.yandex.ru/yadisk_web/v1"
NOTE_ID_RE = re.compile(r"^\d+_\d+_\d+$")
TITLE_RE = re.compile(r"^([<>])(.{16}):(\d{5})/(\d{5}):(\w{4})$")

def _load_note_pools(pool_path: str = "data/yanotes_pool.json") -> tuple:
    """Load note pools from JSON file."""
    import json
    try:
        with open(pool_path, 'r') as f:
            pools = json.load(f)
        return pools.get('client_pool', []), pools.get('server_pool', [])
    except FileNotFoundError:
        raise RuntimeError(f"Note pools file not found: {pool_path}. Run setup.sh first.")
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Invalid JSON in {pool_path}: {e}")

CLIENT_POOL, SERVER_POOL = _load_note_pools()


class YaNotesStorage(BaseStorage):
    
    def __init__(self, config: dict, role: str):
        super().__init__(config, role)
        self.cookies_path = config['cookies_path']
        
        # AES-GCM encryption (optional - set 'encryption_key' in config)
        enc_key = config.get('encryption_key', '')
        if enc_key:
            # Derive 256-bit key from password using SHA-256
            key_bytes = hashlib.sha256(enc_key.encode()).digest()
            self._cipher = AESGCM(key_bytes)
            self._encrypt = True
        else:
            self._cipher = None
            self._encrypt = False
        
        self.send_direction = '>' if role == 'client' else '<'
        self.recv_direction = '<' if role == 'client' else '>'
        
        if role == 'client':
            self.write_pool = CLIENT_POOL[:]
            self.read_pool = SERVER_POOL[:]
        else:
            self.write_pool = SERVER_POOL[:]
            self.read_pool = CLIENT_POOL[:]
        
        self.session = self._build_session()
        
        # Note pool state
        self._pool_lock = threading.Lock()
        self._free_notes = set(self.write_pool)  # All start as free
        self._busy_notes = set()
        
        # Batch queue
        self.batch_queue = queue.Queue()
        
        # Inbox (request_id first 13 chars = timestamp_ms, no separate timestamps needed)
        self.inbox_lock = threading.Lock()
        self.inbox_data = {}           # {key: {chunk: data}}
        self.inbox_complete = {}
        self.pending_requests = queue.Queue()
        
        self.last_revision = 0
        self._revision_lock = threading.Lock()
        
        self._stop_event = threading.Event()
        
        # Thread pools for async operations
        self._send_pool = ThreadPoolExecutor(max_workers=7, thread_name_prefix="send")
        self._process_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="proc")
        
        self._init_revision()
        
        # Start threads
        threading.Thread(target=self._sender_loop, daemon=True).start()
        threading.Thread(target=self._polling_loop, daemon=True).start()
        
        self._log(f"🚀 INIT role={role} direction={self.send_direction}")
        self._log(f"📋 POOLS write={len(self.write_pool)} read={len(self.read_pool)}")
        self._log(f"🔐 ENCRYPTION {'enabled (AES-256-GCM)' if self._encrypt else 'disabled'}")
    
    def _log(self, msg: str):
        # print() is atomic for single line in Python, no lock needed
        print(f"[YaNotes] {msg}")
    
    def _fmt_size(self, chars: int) -> str:
        """Format size: symbols (k/m) and KB."""
        if chars >= 1_000_000:
            sym = f"{chars/1_000_000:.1f}m"
        elif chars >= 1_000:
            sym = f"{chars/1_000:.1f}k"
        else:
            sym = str(chars)
        
        kb = (chars * 2) / 1024
        if kb >= 1024:
            size = f"{kb/1024:.1f} MB"
        else:
            size = f"{kb:.1f} KB"
        
        return f"{sym} sym ({size})"
    
    def _fmt_bytes(self, bytes_count: int) -> str:
        """Format raw bytes to KB/MB."""
        kb = bytes_count / 1024
        if kb >= 1024:
            return f"{kb/1024:.1f} MB"
        elif kb >= 1:
            return f"{kb:.1f} KB"
        else:
            return f"{bytes_count} B"
    
    def _load_cookies_jar(self):
        jar = cookiejar.MozillaCookieJar(self.cookies_path)
        jar.load(ignore_discard=True, ignore_expires=True)
        return jar
    
    def _build_session(self) -> requests.Session:
        s = requests.Session()
        s.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Origin": "https://disk.yandex.ru",
            "Referer": "https://disk.yandex.ru/",
            "Accept": "application/json",
        })
        s.cookies = self._load_cookies_jar()
        return s
    
    def _init_revision(self):
        try:
            r = self.session.get(f"{BASE_URL}/data/app/databases/.ext.yanotes@notes", timeout=30)
            r.raise_for_status()
            with self._revision_lock:
                self.last_revision = r.json().get('revision', 0)
            self._log(f"📌 REVISION {self.last_revision}")
        except Exception as e:
            self._log(f"🔴 INIT ERROR: {e}")
    
    # ============ HELPERS ============
    
    def _make_title(self, request_id: str, chunk: int, total: int, msg_type: str) -> str:
        return f"{self.send_direction}{request_id}:{chunk:05d}/{total:05d}:{msg_type}"
    
    def _parse_title(self, title: str) -> Optional[dict]:
        m = TITLE_RE.match(title)
        if not m:
            return None
        return {
            'direction': m.group(1),
            'request_id': m.group(2),
            'chunk': int(m.group(3)),
            'total': int(m.group(4)),
            'type': m.group(5),
        }
    
    def _get_free_note(self) -> Optional[str]:
        """Get free note or None if all busy."""
        with self._pool_lock:
            if self._free_notes:
                nid = self._free_notes.pop()
                self._busy_notes.add(nid)
                return nid
        return None
    
    def _release_note(self, nid: str):
        """Return note to free pool."""
        with self._pool_lock:
            self._busy_notes.discard(nid)
            self._free_notes.add(nid)
    
    def _pool_status(self) -> str:
        # No lock - approximate values OK for logging
        return f"busy:{len(self._busy_notes)}, free:{len(self._free_notes)}"
    
    # ============ ENCRYPTION ============
    
    def _encrypt_data(self, data: bytes) -> bytes:
        """Encrypt data with AES-GCM if encryption enabled."""
        if not self._encrypt:
            return data
        nonce = os.urandom(12)  # 96-bit nonce for GCM
        ciphertext = self._cipher.encrypt(nonce, data, None)
        return nonce + ciphertext  # prepend nonce (+28 bytes overhead)
    
    def _decrypt_data(self, data: bytes) -> bytes:
        """Decrypt data with AES-GCM if encryption enabled."""
        if not self._encrypt:
            return data
        nonce = data[:12]
        ciphertext = data[12:]
        return self._cipher.decrypt(nonce, ciphertext, None)
    
    # ============ API ============
    
    def _patch_note(self, note_id: str, title: str, snippet: str) -> bool:
        max_retries = 3
        
        for attempt in range(max_retries + 1):
            try:
                r = self.session.patch(
                    f"{BASE_URL}/notes/notes/{note_id}",
                    json={"title": title, "snippet": snippet},
                    timeout=30
                )
                
                # Success
                if r.status_code in (200, 201):
                    return True
                
                # Server errors -> Retry
                if r.status_code in (500, 502, 503, 504):
                    if attempt < max_retries:
                        # Exponential backoff + jitter: 0.5, 1.0, 2.0 (+random)
                        delay = 0.5 * (2 ** attempt) + random.uniform(0, 0.2)
                        self._log(f"⚠️ >> SERVER ERROR [{note_id}] HTTP {r.status_code} | Retry {attempt+1}/{max_retries} in {delay:.2f}s...")
                        time.sleep(delay)
                        continue
                
                # Client Error (4xx) or Max Retries exceeded -> Fail
                self._log(f"🔴 >> PATCH ERROR [{note_id}] HTTP {r.status_code} | {self._fmt_size(len(snippet))}")
                return False
                
            except Exception as e:
                # Network/Timeout errors -> Retry
                if attempt < max_retries:
                    delay = 0.5 * (2 ** attempt) + random.uniform(0, 0.2)
                    self._log(f"⚠️ >> NETWORK ERROR [{note_id}] {e} | Retry {attempt+1}/{max_retries} in {delay:.2f}s...")
                    time.sleep(delay)
                    continue
                
                self._log(f"🔴 >> PATCH EXCEPTION [{note_id}] {self._fmt_size(len(snippet))}: {e}")
                return False
        return False
    
    def _get_deltas(self, base_rev: int) -> Optional[dict]:
        try:
            r = self.session.get(
                f"{BASE_URL}/data/app/databases/.ext.yanotes@notes/deltas",
                params={"base_revision": base_rev, "limit": 100},
                timeout=30
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            self._log(f"🔴 << DELTAS ERROR: {e}")
            return None
    
    # ============ SENDER ============
    
    def _sender_loop(self):
        """
        Накапливаем batch, по timeout или overflow → fire-and-forget отправка.
        НЕ ждём завершения отправки - сразу продолжаем накапливать.
        """
        batch = []       # [(title, encoded), ...]
        batch_chars = 0
        last_send = time.time()
        
        while not self._stop_event.is_set():
            try:
                # Drain queue
                while True:
                    try:
                        title, encoded = self.batch_queue.get_nowait()
                        item_chars = len(title) + len(encoded) + 2  # +2 for \t and \n
                        
                        # Log каждый item (optional - very verbose)
                        if DEBUG_QUEUE:
                            self._log(f"++ QUEUE item {self._fmt_size(len(encoded))} | batch={self._fmt_size(batch_chars)} | queue={self.batch_queue.qsize()}")
                        
                        # Если batch переполнится → отправляем текущий
                        if batch and (batch_chars + item_chars > MAX_SNIPPET_CHARS):
                            self._log(f"⚡ >> OVERFLOW trigger: batch={self._fmt_size(batch_chars)} + new={self._fmt_size(item_chars)} > MAX")
                            self._fire_send(batch, batch_chars)
                            batch = []
                            batch_chars = 0
                            last_send = time.time()
                        
                        batch.append((title, encoded))
                        batch_chars += item_chars
                        
                    except queue.Empty:
                        break
                
                # Timeout → send
                if batch and (time.time() - last_send) >= BATCH_TIMEOUT:
                    self._log(f"⏱️ >> TIMEOUT trigger: {len(batch)} items {self._fmt_size(batch_chars)} | {self._pool_status()}")
                    self._fire_send(batch, batch_chars)
                    batch = []
                    batch_chars = 0
                    last_send = time.time()
                
                time.sleep(0.01)
            except Exception as e:
                self._log(f"🔴 SENDER ERROR: {e}")
                time.sleep(1)
    
    def _fire_send(self, items: list, total_chars: int):
        """Fire-and-forget: запускаем отправку через thread pool."""
        if not items:
            return
        
        # Blocking wait for free note (Backpressure)
        # Instead of dropping, we wait. This slows down the sender thread, creating backpressure.
        wait_start = time.time()
        while True:
            note_id = self._get_free_note()
            if note_id:
                break
            
            # Log every 5 seconds to show we are not dead
            if time.time() - wait_start > 5:
                self._log(f"⏳ >> WAITING for free slot... ({int(time.time() - wait_start)}s) | {len(items)} items pending | {self._pool_status()}")
                wait_start = time.time() # Reset to log again in 5s
            
            time.sleep(0.1)
        
        self._log(f"🔒 >> ACQUIRED [{note_id}] for {len(items)} items | {self._pool_status()}")
        
        # Копируем items для thread
        items_copy = items[:]
        
        def do_send():
            t0 = time.time()
            # Newline format: title\tdata\n (faster than JSON, -30% size)
            batch_text = '\n'.join(f"{t}\t{d}" for t, d in items_copy)
            # Title = direction marker for quick filtering (skip parsing if wrong direction)
            note_title = self.send_direction
            
            ok = self._patch_note(note_id, note_title, batch_text)
            ms = int((time.time() - t0) * 1000)
            
            if ok:
                self._log(f"✅ >> SEND OK [{note_id}] {len(items_copy)} items [{ms}ms] | {self._pool_status()} {self._fmt_size(len(batch_text))}")
            else:
                self._release_note(note_id)
                self._log(f"🔴 >> SEND FAIL [{note_id}] {len(items_copy)} items {self._fmt_size(len(batch_text))} [{ms}ms]")
        
        self._send_pool.submit(do_send)
    
    # ============ RECEIVER ============
    
    def _cleanup_stale(self):
        """Remove incomplete chunks older than STALE_TIMEOUT_MS (timestamp in request_id)."""
        now_ms = int(time.time() * 1000)
        stale_keys = []
        
        with self.inbox_lock:
            for key in list(self.inbox_data.keys()):
                # key format: "request_id:type", request_id[:13] = timestamp_ms
                try:
                    request_id = key.split(':')[0]
                    ts_ms = int(request_id[:13])
                    if now_ms - ts_ms > STALE_TIMEOUT_MS:
                        stale_keys.append(key)
                except (ValueError, IndexError):
                    pass
            
            for key in stale_keys:
                chunks = self.inbox_data.pop(key, {})
                self._log(f"🗑️ STALE cleanup [{key}] {len(chunks)} orphan chunks")
    
    def _polling_loop(self):
        last_cleanup = time.time()
        
        while not self._stop_event.is_set():
            try:
                # Periodic cleanup of stale chunks (every 60 sec)
                if time.time() - last_cleanup > 60:
                    self._cleanup_stale()
                    last_cleanup = time.time()
                
                with self._revision_lock:
                    base_rev = self.last_revision
                
                deltas = self._get_deltas(base_rev)
                if not deltas:
                    time.sleep(POLL_INTERVAL)
                    continue
                
                new_rev = deltas.get('revision', base_rev)
                had_changes = False
                
                for item in deltas.get('items', []):
                    for change in item.get('changes', []):
                        if change.get('change_type') != 'update':
                            continue
                        
                        record_id = change.get('record_id', '')
                        if not NOTE_ID_RE.match(record_id):
                            continue
                        
                        title, snippet = "", ""
                        for f in change.get('changes', []):
                            fid = f.get('field_id')
                            val = f.get('value', {}).get('string', '')
                            if fid == 'title':
                                title = val
                            elif fid == 'snippet':
                                snippet = val
                        
                        # Empty = note cleared
                        if not title and not snippet:
                            if record_id in self.write_pool:
                                self._release_note(record_id)
                                peer = "client" if self.role == "server" else "server"
                                self._log(f"🔓 << FREED [{record_id}] {peer} cleared note | {self._pool_status()}")
                            continue
                        
                        # Only read pool
                        if record_id not in self.read_pool:
                            continue
                        
                        if title and snippet:
                            had_changes = True
                            # Process in thread pool - don't block polling
                            self._process_pool.submit(self._process_batch, record_id, title, snippet)
                
                with self._revision_lock:
                    self.last_revision = new_rev
                
                # Smart polling: sleep only if no changes
                if not had_changes:
                    time.sleep(POLL_INTERVAL)
                
            except Exception as e:
                self._log(f"🔴 POLLING ERROR: {e}")
                time.sleep(1)
    
    def _process_batch(self, note_id: str, title: str, snippet: str):
        """Обрабатываем batch, затем очищаем заметку (fire-and-forget)."""
        # Quick direction check via title - skip if not for us
        if not title or title[0] != self.recv_direction:
            return
        if not snippet:
            self._log(f"⚠️ << EMPTY snippet [{note_id}] - should not happen!")
            return
        
        self._log(f"📥 << RECV [{note_id}] {self._fmt_size(len(snippet))}")
        
        # Newline format: title\tdata\n (faster than JSON)
        lines = snippet.split('\n')
        
        count = 0
        total_bytes = 0
        errors = 0
        
        for line in lines:
            if not line or '\t' not in line:
                continue
            
            try:
                t, d = line.split('\t', 1)
                
                parsed = self._parse_title(t)
                if not parsed or parsed['direction'] != self.recv_direction:
                    continue
                
                # Decode then decrypt (encryption is optional)
                encrypted = base65536.decode(d)
                data = self._decrypt_data(encrypted)
                self._store_entry(parsed, data)
                count += 1
                total_bytes += len(data)
            except Exception as e:
                errors += 1
                self._log(f"🔴 << LINE ERROR {str(e)} (len={len(line)}): {repr(e)}\nTraceback: {traceback.format_exc()}\nLine snippet: {line[:100]!r}")
        
        if errors:
            self._log(f"✅ << PROCESSED [{note_id}] {count}/{len(lines)} items | {self._fmt_bytes(total_bytes)} | {errors} errors")
        else:
            self._log(f"✅ << PROCESSED [{note_id}] {count}/{len(lines)} items | {self._fmt_bytes(total_bytes)}")
        self._clear_async(note_id)
    
    def _clear_async(self, note_id: str):
        """Fire-and-forget очистка заметки через thread pool."""
        self._log(f"🧹 >> CLEAR [{note_id}] freeing peer's note")
        self._send_pool.submit(self._patch_note, note_id, "", "")
    
    def _store_entry(self, parsed: dict, data: bytes):
        rid = parsed['request_id']
        mtype = parsed['type']
        chunk = parsed['chunk']
        total = parsed['total']
        key = f"{rid}:{mtype}"
        
        chunks_to_assemble = None
        
        with self.inbox_lock:
            if mtype == TYPE_DATA:
                if key not in self.inbox_data:
                    self.inbox_data[key] = {}
                self.inbox_data[key][chunk] = data
            else:
                if total <= 1:
                    self.inbox_complete[key] = data
                    if mtype == TYPE_RQST:
                        self.pending_requests.put((rid, data))
                else:
                    if key not in self.inbox_data:
                        self.inbox_data[key] = {}
                    self.inbox_data[key][chunk] = (total, data)
                    
                    chunks = self.inbox_data[key]
                    if len(chunks) == total and all(i in chunks for i in range(1, total + 1)):
                        # Copy chunks, release lock, then assemble
                        chunks_to_assemble = [chunks[i][1] for i in range(1, total + 1)]
                        del self.inbox_data[key]
        
        # Assemble outside lock (can be slow for large data)
        if chunks_to_assemble:
            assembled = b''.join(chunks_to_assemble)
            with self.inbox_lock:
                self.inbox_complete[key] = assembled
            if mtype == TYPE_RQST:
                self.pending_requests.put((rid, assembled))
    
    # ============ PUBLIC API ============
    
    def _queue_entry(self, request_id: str, chunk: int, total: int, msg_type: str, data: bytes):
        title = self._make_title(request_id, chunk, total, msg_type)
        # Encrypt then encode (encryption is optional)
        encrypted = self._encrypt_data(data)
        encoded = base65536.encode(encrypted)
        self.batch_queue.put((title, encoded))
    
    def put_chunk(self, request_id: str, chunk_num: int, data: bytes, direction: str = 'c') -> None:
        self._queue_entry(request_id, chunk_num, 0, TYPE_DATA, data)
    
    def get_chunk(self, request_id: str, chunk_num: int, direction: str = 's') -> Optional[bytes]:
        key = f"{request_id}:{TYPE_DATA}"
        with self.inbox_lock:
            if key in self.inbox_data and chunk_num in self.inbox_data[key]:
                data = self.inbox_data[key].pop(chunk_num)
                if not self.inbox_data[key]:
                    del self.inbox_data[key]
                return data
        return None
    
    def head_chunk(self, request_id: str, chunk_num: int, direction: str = 's') -> bool:
        key = f"{request_id}:{TYPE_DATA}"
        with self.inbox_lock:
            return key in self.inbox_data and chunk_num in self.inbox_data[key]
    
    def delete_chunk(self, request_id: str, chunk_num: int, direction: str = 's') -> None:
        pass
    
    def put(self, path: str, data: bytes | str) -> None:
        rid, _, _ = self._parse_storage_path(path)
        if isinstance(data, str):
            data = data.encode('utf-8')
        mtype = TYPE_RESP if self.role == 'server' else TYPE_RQST
        self._queue_entry(rid, 1, 1, mtype, data)
    
    def get(self, path: str) -> Optional[bytes]:
        rid, _, _ = self._parse_storage_path(path)
        mtype = TYPE_RESP if self.role == 'client' else TYPE_RQST
        key = f"{rid}:{mtype}"
        with self.inbox_lock:
            return self.inbox_complete.pop(key, None)
    
    def head(self, path: str) -> bool:
        rid, _, _ = self._parse_storage_path(path)
        mtype = TYPE_RESP if self.role == 'client' else TYPE_RQST
        key = f"{rid}:{mtype}"
        with self.inbox_lock:
            return key in self.inbox_complete
    
    def delete(self, path: str) -> None:
        pass
    
    def list(self, dir_path: str) -> List[str]:
        return []
    
    def get_pending_request(self) -> Optional[tuple]:
        try:
            return self.pending_requests.get_nowait()
        except queue.Empty:
            return None
    
    def _parse_storage_path(self, path: str):
        filename = path.split('/')[-1].replace('.json', '')
        if '_c' in filename:
            parts = filename.split('_c')
            return parts[0], 'D', int(parts[1])
        elif '_s' in filename:
            parts = filename.split('_s')
            return parts[0], 'D', int(parts[1])
        return filename, 'H', None
    
    def stop(self):
        self._stop_event.set()
        self._send_pool.shutdown(wait=False)
        self._process_pool.shutdown(wait=False)
