#!/usr/bin/env python3
"""YaNotes setup helper - check access and create notes."""

import asyncio
import http.cookiejar as cookiejar
import json
import sys
from pathlib import Path

import httpx

BASE_URL = "https://cloud-api.yandex.ru/yadisk_web/v1"
COOKIES_PATH = "data/diskcookies.txt"
POOL_PATH = "data/yanotes_pool.json"

CLIENT_NOTES = 100
SERVER_NOTES = 100


def load_cookies() -> httpx.Cookies:
    jar = cookiejar.MozillaCookieJar(COOKIES_PATH)
    jar.load(ignore_discard=True, ignore_expires=True)
    cookies = httpx.Cookies()
    for c in jar:
        cookies.set(c.name, c.value, domain=c.domain, path=c.path)
    return cookies


def get_headers() -> dict:
    return {
        "User-Agent": "Mozilla/5.0",
        "Origin": "https://disk.yandex.ru",
        "Referer": "https://disk.yandex.ru/",
        "Accept": "application/json",
    }


def check_access(note_id: str, cookies: httpx.Cookies) -> bool:
    """Check access to note via PATCH request."""
    try:
        with httpx.Client(base_url=BASE_URL, headers=get_headers(), cookies=cookies, timeout=30) as client:
            r = client.patch(
                f"/notes/notes/{note_id}",
                json={"title": "access_check", "snippet": ""}
            )
            return r.status_code in (200, 201)
    except Exception:
        return False


async def create_notes(count: int, cookies: httpx.Cookies, prefix: str) -> list:
    """Create notes and return their IDs."""
    sem = asyncio.Semaphore(10)
    
    async def create_one(client: httpx.AsyncClient, i: int) -> str:
        async with sem:
            r = await client.post(
                "/notes/notes",
                json={"title": f"{prefix}_{i:03d}", "snippet": "", "tags": []}
            )
            r.raise_for_status()
            obj = r.json()
            if isinstance(obj, list) and obj:
                obj = obj[0]
            return obj.get("id", "")
    
    async with httpx.AsyncClient(base_url=BASE_URL, headers=get_headers(), cookies=cookies, timeout=30) as client:
        tasks = [create_one(client, i) for i in range(1, count + 1)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    
    return [r for r in results if isinstance(r, str) and r]


def main():
    if len(sys.argv) < 2:
        print("Usage: yanotes_setup.py [check|create]")
        sys.exit(1)
    
    cmd = sys.argv[1]
    
    if cmd == "check":
        # Check if pool file exists and verify access
        pool_path = Path(POOL_PATH)
        
        if not pool_path.exists():
            print("NO_POOL")
            sys.exit(0)
        
        try:
            with open(pool_path) as f:
                pools = json.load(f)
            
            client_pool = pools.get("client_pool", [])
            server_pool = pools.get("server_pool", [])
            
            print(f"POOL_FOUND:{len(client_pool)}:{len(server_pool)}")
            
            # Check access to first note from each pool
            cookies = load_cookies()
            
            client_ok = False
            server_ok = False
            
            if client_pool:
                client_ok = check_access(client_pool[0], cookies)
            if server_pool:
                server_ok = check_access(server_pool[0], cookies)
            
            if client_ok and server_ok:
                print("ACCESS_OK")
            elif client_ok:
                print("ACCESS_CLIENT_ONLY")
            elif server_ok:
                print("ACCESS_SERVER_ONLY")
            else:
                print("ACCESS_NONE")
                
        except Exception as e:
            print(f"ERROR:{e}")
            sys.exit(1)
    
    elif cmd == "create":
        # Create notes for both pools
        print(f"Creating {CLIENT_NOTES} client notes and {SERVER_NOTES} server notes...")
        
        cookies = load_cookies()
        
        print("Creating client notes...")
        client_ids = asyncio.run(create_notes(CLIENT_NOTES, cookies, "client"))
        print(f"  Created: {len(client_ids)}")
        
        print("Creating server notes...")
        server_ids = asyncio.run(create_notes(SERVER_NOTES, cookies, "server"))
        print(f"  Created: {len(server_ids)}")
        
        if len(client_ids) < CLIENT_NOTES or len(server_ids) < SERVER_NOTES:
            print(f"ERROR: Failed to create all notes")
            sys.exit(1)
        
        # Save to pool file
        pools = {
            "client_pool": client_ids,
            "server_pool": server_ids
        }
        
        with open(POOL_PATH, 'w') as f:
            json.dump(pools, f, indent=2)
        
        print(f"Saved to {POOL_PATH}")
        print("DONE")
    
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
