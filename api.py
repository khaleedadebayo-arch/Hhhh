#!/usr/bin/env python3
"""
Kraven KOL Bot — Super Admin Mini App API
Broadcast control only. Full Telegram sync via direct Bot API calls.
"""

import hashlib
import hmac
import json
import os
import urllib.parse
import httpx
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN   = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

FOUNDER_IDS: set[int] = {6066940244, 7902074220}
TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
KRAVEN_CHANNEL = os.getenv("KRAVEN_CHANNEL", "https://t.me/kraven")
KRAVEN_FOOTER  = f"\n\n— Powered by [Kraven KOL Network]({KRAVEN_CHANNEL})"

# ─── DB POOL ──────────────────────────────────────────────────────────────────

_pool: ThreadedConnectionPool | None = None

def get_pool():
    global _pool
    if _pool is None:
        _pool = ThreadedConnectionPool(minconn=2, maxconn=10, dsn=DATABASE_URL)
    return _pool

class _Conn:
    def __init__(self, conn):
        self._conn = conn
    def execute(self, sql, params=None):
        cur = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        return cur
    def commit(self):   self._conn.commit()
    def rollback(self): self._conn.rollback()

def get_db():
    pool = get_pool()
    raw  = pool.getconn()
    conn = _Conn(raw)
    try:
        yield conn
        raw.commit()
    except Exception:
        raw.rollback()
        raise
    finally:
        pool.putconn(raw)

# ─── TELEGRAM DIRECT CALLS ────────────────────────────────────────────────────

async def tg_send(chat_id: int, text: str) -> bool:
    """Send a message directly to a Telegram chat via Bot API. Returns True on success."""
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(f"{TELEGRAM_API}/sendMessage", json={
            "chat_id":    chat_id,
            "text":       text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        })
        return r.json().get("ok", False)

# ─── AUTH ─────────────────────────────────────────────────────────────────────

def validate_init_data(init_data: str) -> dict:
    try:
        parsed       = dict(urllib.parse.parse_qsl(init_data, keep_blank_values=True))
        recv_hash    = parsed.pop("hash", None)
        if not recv_hash:
            raise HTTPException(status_code=401, detail="Missing hash")
        check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
        secret       = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        computed     = hmac.new(secret, check_string.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(computed, recv_hash):
            raise HTTPException(status_code=401, detail="Invalid initData")
        return json.loads(parsed.get("user", "{}"))
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=401, detail="Malformed initData")

def require_superadmin(init_data: str) -> int:
    user    = validate_init_data(init_data)
    user_id = user.get("id")
    if not user_id:
        raise HTTPException(status_code=401, detail="No user ID")
    if user_id in FOUNDER_IDS:
        return user_id
    for conn in get_db():
        row = conn.execute("SELECT 1 FROM super_admins WHERE user_id=%s", (user_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=403, detail="Super-admin only")
    return user_id

# ─── MODELS ───────────────────────────────────────────────────────────────────

class AuthReq(BaseModel):
    init_data: str

class BroadcastReq(BaseModel):
    init_data: str
    message:   str

class VIPReq(BaseModel):
    init_data:  str
    chat_id:    int
    chat_title: str = ""

class AdminReq(BaseModel):
    init_data:      str
    target_user_id: int

# ─── APP ──────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    get_pool()
    yield
    if _pool:
        _pool.closeall()

app = FastAPI(title="Kraven SA Panel", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

# ─── ENDPOINTS ────────────────────────────────────────────────────────────────

@app.post("/api/auth")
def auth(req: AuthReq):
    user_id = require_superadmin(req.init_data)
    user    = validate_init_data(req.init_data)
    return {
        "user_id":    user_id,
        "username":   user.get("username", ""),
        "first_name": user.get("first_name", ""),
        "is_founder": user_id in FOUNDER_IDS,
    }


@app.post("/api/stats")
def stats(req: AuthReq):
    require_superadmin(req.init_data)
    for conn in get_db():
        total   = conn.execute("SELECT COUNT(*) AS n FROM known_groups").fetchone()["n"]
        vip     = conn.execute("SELECT COUNT(*) AS n FROM known_groups WHERE vip_excluded=TRUE").fetchone()["n"]
        groups  = conn.execute(
            "SELECT chat_id, chat_title, vip_excluded, joined_at FROM known_groups ORDER BY joined_at DESC"
        ).fetchall()
        total_b = conn.execute("SELECT COUNT(*) AS n FROM broadcast_log").fetchone()["n"]
        last_b  = conn.execute(
            "SELECT sent_at, groups_count FROM broadcast_log ORDER BY id DESC LIMIT 1"
        ).fetchone()
    return {
        "total_groups":      total,
        "vip_excluded":      vip,
        "active_recipients": total - vip,
        "total_broadcasts":  total_b,
        "last_broadcast":    str(last_b["sent_at"])[:16] if last_b else None,
        "last_reach":        last_b["groups_count"] if last_b else 0,
        "groups": [
            {
                "chat_id":      g["chat_id"],
                "title":        g["chat_title"] or f"Group {g['chat_id']}",
                "vip_excluded": bool(g["vip_excluded"]),
                "joined_at":    str(g["joined_at"])[:10],
            }
            for g in groups
        ],
    }


@app.post("/api/broadcast/history")
def broadcast_history(req: AuthReq):
    require_superadmin(req.init_data)
    for conn in get_db():
        rows = conn.execute(
            "SELECT sent_by, groups_count, sent_at, message FROM broadcast_log ORDER BY id DESC LIMIT 20"
        ).fetchall()
    return {"history": [
        {
            "sent_by":     r["sent_by"],
            "groups":      r["groups_count"],
            "sent_at":     str(r["sent_at"])[:16],
            "message":     r["message"],
        }
        for r in rows
    ]}


@app.post("/api/broadcast/send")
async def broadcast_send(req: BroadcastReq):
    sender_id = require_superadmin(req.init_data)
    if not req.message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    formatted = f"📢 *Sponsored by Kraven KOL Network*\n\n{req.message}{KRAVEN_FOOTER}"

    # Fetch groups outside DB context so we don't hold connection during HTTP calls
    for conn in get_db():
        groups = [
            r["chat_id"]
            for r in conn.execute(
                "SELECT chat_id FROM known_groups WHERE vip_excluded = FALSE"
            ).fetchall()
        ]

    if not groups:
        raise HTTPException(status_code=404, detail="No eligible groups on record")

    sent   = 0
    failed = 0
    for chat_id in groups:
        ok = await tg_send(chat_id, formatted)
        if ok:
            sent += 1
        else:
            failed += 1

    # Log result
    for conn in get_db():
        conn.execute(
            "INSERT INTO broadcast_log (sent_by, message, groups_count) VALUES (%s,%s,%s)",
            (sender_id, req.message.strip(), sent)
        )

    return {"ok": True, "sent": sent, "failed": failed, "total": len(groups)}


@app.post("/api/vip/list")
def vip_list(req: AuthReq):
    require_superadmin(req.init_data)
    for conn in get_db():
        rows = conn.execute(
            "SELECT chat_id, chat_title, vip_excluded FROM known_groups ORDER BY chat_title"
        ).fetchall()
    return {"groups": [
        {"chat_id": r["chat_id"], "title": r["chat_title"] or f"Group {r['chat_id']}",
         "vip_excluded": bool(r["vip_excluded"])}
        for r in rows
    ]}


@app.post("/api/vip/toggle")
def vip_toggle(req: VIPReq):
    require_superadmin(req.init_data)
    for conn in get_db():
        current = conn.execute(
            "SELECT vip_excluded FROM known_groups WHERE chat_id=%s", (req.chat_id,)
        ).fetchone()
        if not current:
            raise HTTPException(status_code=404, detail="Group not found in records")
        new_val = not bool(current["vip_excluded"])
        conn.execute(
            "UPDATE known_groups SET vip_excluded=%s WHERE chat_id=%s",
            (new_val, req.chat_id)
        )
    return {"ok": True, "vip_excluded": new_val}


@app.post("/api/admins/list")
def admins_list(req: AuthReq):
    require_superadmin(req.init_data)
    for conn in get_db():
        rows = conn.execute(
            "SELECT user_id, added_by, added_at FROM super_admins ORDER BY added_at"
        ).fetchall()
    return {
        "founders": sorted(FOUNDER_IDS),
        "admins": [
            {"user_id": r["user_id"], "added_by": r["added_by"],
             "added_at": str(r["added_at"])[:10]}
            for r in rows
        ],
    }


@app.post("/api/admins/add")
def admins_add(req: AdminReq):
    sender_id = require_superadmin(req.init_data)
    if req.target_user_id in FOUNDER_IDS:
        raise HTTPException(status_code=400, detail="Already a founder")
    for conn in get_db():
        result = conn.execute(
            "INSERT INTO super_admins (user_id, added_by) VALUES (%s,%s) ON CONFLICT DO NOTHING",
            (req.target_user_id, sender_id)
        )
        existed = result.rowcount == 0
    return {"ok": True, "already_existed": existed}


@app.post("/api/admins/remove")
def admins_remove(req: AdminReq):
    require_superadmin(req.init_data)
    if req.target_user_id in FOUNDER_IDS:
        raise HTTPException(status_code=400, detail="Cannot remove a founder")
    for conn in get_db():
        conn.execute("DELETE FROM super_admins WHERE user_id=%s", (req.target_user_id,))
    return {"ok": True}


@app.get("/")
def serve():
    return FileResponse("webapp.html")

@app.get("/health")
def health():
    return {"ok": True, "time": datetime.now(tz=timezone.utc).isoformat()}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=False)
