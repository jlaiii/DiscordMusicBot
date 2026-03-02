from __future__ import annotations

import aiosqlite
import asyncio
import os
import time
from typing import Optional, List, Dict

DB_PATH = os.environ.get("DMBOT_PLAYLIST_DB", "playlists.db")

_init_lock = asyncio.Lock()

async def init_db(db_path: Optional[str] = None):
    """Initialize the playlists DB and create tables if needed."""
    global DB_PATH
    if db_path:
        DB_PATH = db_path
    async with _init_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS playlists (
                    id INTEGER PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    visibility TEXT NOT NULL CHECK(visibility IN ('public','private')) DEFAULT 'public',
                    created_at INTEGER NOT NULL
                )
                """
            )
            await db.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_playlists_owner_name ON playlists(owner_id, name)
                """
            )
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS playlist_items (
                    id INTEGER PRIMARY KEY,
                    playlist_id INTEGER NOT NULL,
                    position INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    webpage_url TEXT,
                    source_url TEXT,
                    duration REAL,
                    is_live INTEGER DEFAULT 0,
                    added_at INTEGER NOT NULL,
                    FOREIGN KEY(playlist_id) REFERENCES playlists(id) ON DELETE CASCADE
                )
                """
            )
            await db.execute("PRAGMA foreign_keys = ON")
            await db.commit()

async def create_playlist(owner_id: str, name: str, visibility: str = "public") -> int:
    await init_db()
    ts = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO playlists(owner_id, name, visibility, created_at) VALUES (?, ?, ?, ?)",
            (owner_id, name, visibility, ts),
        )
        await db.commit()
        return cur.lastrowid

async def delete_playlist(owner_id: str, name: str) -> bool:
    await init_db()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA foreign_keys = ON")
        cur = await db.execute("DELETE FROM playlists WHERE owner_id = ? AND name = ?", (owner_id, name))
        await db.commit()
        return cur.rowcount > 0

async def edit_playlist(owner_id: str, name: str, new_name: Optional[str] = None, visibility: Optional[str] = None) -> bool:
    await init_db()
    if not new_name and not visibility:
        return False
    async with aiosqlite.connect(DB_PATH) as db:
        parts = []
        params = []
        if new_name:
            parts.append("name = ?")
            params.append(new_name)
        if visibility:
            parts.append("visibility = ?")
            params.append(visibility)
        params.extend([owner_id, name])
        q = f"UPDATE playlists SET {', '.join(parts)} WHERE owner_id = ? AND name = ?"
        cur = await db.execute(q, params)
        await db.commit()
        return cur.rowcount > 0

async def list_playlists_for_user(viewer_id: str) -> List[Dict]:
    """Return playlists visible to the viewer: their own playlists and public playlists."""
    await init_db()
    async with aiosqlite.connect(DB_PATH) as db:
        rows = []
        # owner's playlists
        async with db.execute("SELECT id, owner_id, name, visibility, created_at FROM playlists WHERE owner_id = ? ORDER BY created_at DESC", (viewer_id,)) as cur:
            async for r in cur:
                rows.append({"id": r[0], "owner_id": r[1], "name": r[2], "visibility": r[3], "created_at": r[4]})
        # public playlists by others
        async with db.execute("SELECT id, owner_id, name, visibility, created_at FROM playlists WHERE visibility = 'public' AND owner_id != ? ORDER BY created_at DESC", (viewer_id,)) as cur:
            async for r in cur:
                rows.append({"id": r[0], "owner_id": r[1], "name": r[2], "visibility": r[3], "created_at": r[4]})
        return rows

async def _get_playlist_id(owner_id: str, name: str) -> Optional[int]:
    await init_db()
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id FROM playlists WHERE owner_id = ? AND name = ?", (owner_id, name)) as cur:
            row = await cur.fetchone()
            return row[0] if row else None

async def get_playlist_by_name_any(owner_or_name: str, name: str) -> Optional[Dict]:
    """Find playlist by owner and name, or if owner_or_name is 'public' then search public by name."""
    await init_db()
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id, owner_id, name, visibility, created_at FROM playlists WHERE owner_id = ? AND name = ?", (owner_or_name, name)) as cur:
            row = await cur.fetchone()
            if row:
                return {"id": row[0], "owner_id": row[1], "name": row[2], "visibility": row[3], "created_at": row[4]}
        # try searching public by name only
        async with db.execute("SELECT id, owner_id, name, visibility, created_at FROM playlists WHERE visibility = 'public' AND name = ?", (name,)) as cur:
            row = await cur.fetchone()
            if row:
                return {"id": row[0], "owner_id": row[1], "name": row[2], "visibility": row[3], "created_at": row[4]}
    return None

async def add_item(owner_id: str, playlist_name: str, title: str, webpage_url: Optional[str], source_url: Optional[str], duration: Optional[float], is_live: bool) -> bool:
    await init_db()
    pid = await _get_playlist_id(owner_id, playlist_name)
    if pid is None:
        return False
    ts = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        # avoid adding exact-duplicate items based on webpage_url, source_url, or title
        try:
            async with db.execute(
                "SELECT 1 FROM playlist_items WHERE playlist_id = ? AND ( (webpage_url IS NOT NULL AND webpage_url = ?) OR (source_url IS NOT NULL AND source_url = ?) OR LOWER(title) = LOWER(?) ) LIMIT 1",
                (pid, webpage_url, source_url, title),
            ) as cur:
                row = await cur.fetchone()
                if row:
                    return False
        except Exception:
            # If the duplicate-check query fails for any reason, fall back to insert to avoid data loss
            pass

        # compute next position
        async with db.execute("SELECT COALESCE(MAX(position), 0) + 1 FROM playlist_items WHERE playlist_id = ?", (pid,)) as cur:
            row = await cur.fetchone()
            pos = row[0] if row else 1
        await db.execute(
            "INSERT INTO playlist_items(playlist_id, position, title, webpage_url, source_url, duration, is_live, added_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (pid, pos, title, webpage_url, source_url, duration, 1 if is_live else 0, ts),
        )
        await db.commit()
        return True

async def remove_item(owner_id: str, playlist_name: str, index: int) -> bool:
    await init_db()
    pid = await _get_playlist_id(owner_id, playlist_name)
    if pid is None:
        return False
    async with aiosqlite.connect(DB_PATH) as db:
        # find item by position
        async with db.execute("SELECT id FROM playlist_items WHERE playlist_id = ? AND position = ?", (pid, index)) as cur:
            row = await cur.fetchone()
            if not row:
                return False
            item_id = row[0]
        await db.execute("DELETE FROM playlist_items WHERE id = ?", (item_id,))
        # reindex positions
        async with db.execute("SELECT id FROM playlist_items WHERE playlist_id = ? ORDER BY position", (pid,)) as cur:
            rows = await cur.fetchall()
        for i, r in enumerate(rows, start=1):
            await db.execute("UPDATE playlist_items SET position = ? WHERE id = ?", (i, r[0]))
        await db.commit()
        return True

async def view_playlist(viewer_id: str, playlist_name: str) -> Optional[Dict]:
    """Return playlist metadata and ordered items if visible to viewer."""
    await init_db()
    # first try find exact owner match; if not found, try public by name
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id, owner_id, name, visibility, created_at FROM playlists WHERE name = ?", (playlist_name,)) as cur:
            rows = await cur.fetchall()
            # pick the one owned by viewer first if exists
            pid = None
            picked = None
            for r in rows:
                if str(r[1]) == str(viewer_id):
                    picked = r
                    break
            if not picked and rows:
                # pick first public one
                for r in rows:
                    if r[3] == 'public':
                        picked = r
                        break
            if not picked:
                return None
            pid = picked[0]
            meta = {"id": picked[0], "owner_id": picked[1], "name": picked[2], "visibility": picked[3], "created_at": picked[4]}
            items = []
            async with db.execute("SELECT position, title, webpage_url, source_url, duration, is_live FROM playlist_items WHERE playlist_id = ? ORDER BY position", (pid,)) as cur2:
                async for it in cur2:
                    items.append({"position": it[0], "title": it[1], "webpage_url": it[2], "source_url": it[3], "duration": it[4], "is_live": bool(it[5])})
            meta["items"] = items
            return meta
