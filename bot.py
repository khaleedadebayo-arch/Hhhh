#!/usr/bin/env python3
"""
KOL Campaign Manager Bot
Full-featured Telegram bot for managing KOL link-drop sessions,
queue enforcement, auto-moderation, and campaign tracking.

Database: PostgreSQL (migrated from SQLite)
"""

import os
import re
import logging
from contextlib import contextmanager
from datetime import datetime
from html import escape
from io import BytesIO
from urllib.parse import urlsplit, urlunsplit

import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool
from telegram import Update, ChatPermissions, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
from dotenv import load_dotenv

load_dotenv()

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("KOLBot")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# ─── CONFIG ───────────────────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set in .env")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set in .env")

DEFAULT_QUEUE_SIZE = int(os.getenv("DEFAULT_QUEUE_SIZE", 15))

TWITTER_RE = re.compile(
    r"https?://(www\.)?(twitter\.com|x\.com)/\S+", re.IGNORECASE
)
ANY_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
TAGALL_MAX_LEN = 4000
PRIVATE_MENU_PREFIX = "menu:"


# ─── CONNECTION POOL ──────────────────────────────────────────────────────────

_pool: ThreadedConnectionPool | None = None


def get_pool() -> ThreadedConnectionPool:
    global _pool
    if _pool is None:
        _pool = ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            dsn=DATABASE_URL,
        )
    return _pool


class _ConnWrapper:
    """
    Thin wrapper around a psycopg2 connection that adds a SQLite-compatible
    conn.execute(sql, params) API, so every call site works unchanged.
    """
    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql: str, params=None):
        cur = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        return cur

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()


@contextmanager
def db():
    """
    Yield a _ConnWrapper that exposes conn.execute() just like sqlite3.
    Automatically commits on success, rolls back on exception, and returns
    the connection to the pool when done.
    """
    pool = get_pool()
    raw = pool.getconn()
    conn = _ConnWrapper(raw)
    try:
        yield conn
        raw.commit()
    except Exception:
        raw.rollback()
        raise
    finally:
        pool.putconn(raw)


# ─── DATABASE SCHEMA ──────────────────────────────────────────────────────────

def init_db():
    with db() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id     BIGINT  NOT NULL,
            chat_id     BIGINT  NOT NULL,
            username    TEXT    DEFAULT '',
            full_name   TEXT    DEFAULT '',
            warnings    INTEGER DEFAULT 0,
            whitelisted INTEGER DEFAULT 0,
            banned      INTEGER DEFAULT 0,
            total_links INTEGER DEFAULT 0,
            points      INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, chat_id)
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS link_queue (
            id        SERIAL PRIMARY KEY,
            chat_id   BIGINT  NOT NULL,
            thread_id BIGINT  NOT NULL DEFAULT 0,
            user_id   BIGINT  NOT NULL,
            username  TEXT    DEFAULT '',
            link      TEXT    NOT NULL,
            posted_at TIMESTAMPTZ DEFAULT NOW()
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS topic_settings (
            chat_id         BIGINT  NOT NULL,
            thread_id       BIGINT  NOT NULL DEFAULT 0,
            queue_size      INTEGER DEFAULT 15,
            session_active  INTEGER DEFAULT 0,
            points_per_link INTEGER DEFAULT 10,
            PRIMARY KEY (chat_id, thread_id)
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS campaigns (
            id          SERIAL PRIMARY KEY,
            chat_id     BIGINT  NOT NULL,
            thread_id   BIGINT  NOT NULL DEFAULT 0,
            name        TEXT    NOT NULL,
            description TEXT    DEFAULT '',
            target      INTEGER DEFAULT 100,
            reward      TEXT    DEFAULT 'TBA',
            deadline    TEXT    DEFAULT 'Open-ended',
            active      INTEGER DEFAULT 1,
            created_by  BIGINT,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS campaign_submissions (
            id           SERIAL PRIMARY KEY,
            campaign_id  INTEGER NOT NULL,
            user_id      BIGINT  NOT NULL,
            username     TEXT    DEFAULT '',
            link         TEXT    NOT NULL,
            verified     INTEGER DEFAULT 0,
            submitted_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (campaign_id, link)
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS rewards (
            id       SERIAL PRIMARY KEY,
            chat_id  BIGINT NOT NULL,
            user_id  BIGINT NOT NULL,
            username TEXT   DEFAULT '',
            amount   TEXT   NOT NULL,
            reason   TEXT   DEFAULT '',
            paid_at  TIMESTAMPTZ DEFAULT NOW()
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS cmd_permissions (
            chat_id   BIGINT  NOT NULL,
            thread_id BIGINT  NOT NULL DEFAULT 0,
            command   TEXT    NOT NULL,
            enabled   INTEGER DEFAULT 0,
            PRIMARY KEY (chat_id, thread_id, command)
        )
        """)

    logger.info("Database initialised (PostgreSQL).")


# ─── DB HELPERS ───────────────────────────────────────────────────────────────

def upsert_user(conn, user_id: int, chat_id: int, username: str, full_name: str):
    conn.execute(
        """
        INSERT INTO users (user_id, chat_id, username, full_name)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (user_id, chat_id) DO UPDATE
            SET username  = EXCLUDED.username,
                full_name = EXCLUDED.full_name
        """,
        (user_id, chat_id, username, full_name),
    )


def fetch_user(conn, user_id: int, chat_id: int):
    return conn.execute(
        "SELECT * FROM users WHERE user_id=%s AND chat_id=%s", (user_id, chat_id)
    ).fetchone()


def fetch_settings(conn, chat_id: int, thread_id: int = 0) -> dict:
    row = conn.execute(
        "SELECT * FROM topic_settings WHERE chat_id=%s AND thread_id=%s",
        (chat_id, thread_id),
    ).fetchone()
    if not row:
        conn.execute(
            """
            INSERT INTO topic_settings (chat_id, thread_id, queue_size, session_active, points_per_link)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (chat_id, thread_id) DO NOTHING
            """,
            (chat_id, thread_id, DEFAULT_QUEUE_SIZE, 0, 10),
        )
        return {
            "chat_id": chat_id,
            "thread_id": thread_id,
            "queue_size": DEFAULT_QUEUE_SIZE,
            "session_active": 0,
            "points_per_link": 10,
        }
    return dict(row)


def update_chat_settings(
    conn,
    chat_id: int,
    thread_id: int = 0,
    *,
    queue_size: int | None = None,
    session_active: int | None = None,
    points_per_link: int | None = None,
):
    """Update chat settings without resetting fields that were not provided."""
    current = fetch_settings(conn, chat_id, thread_id)
    conn.execute(
        """
        INSERT INTO topic_settings (chat_id, thread_id, queue_size, session_active, points_per_link)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (chat_id, thread_id) DO UPDATE
            SET queue_size      = EXCLUDED.queue_size,
                session_active  = EXCLUDED.session_active,
                points_per_link = EXCLUDED.points_per_link
        """,
        (
            chat_id,
            thread_id,
            current["queue_size"] if queue_size is None else queue_size,
            current["session_active"] if session_active is None else session_active,
            current["points_per_link"] if points_per_link is None else points_per_link,
        ),
    )


def queue_progress(conn, chat_id: int, thread_id: int, user_id: int, queue_size: int):
    """
    Returns (count_after, can_post).
    count_after = how many UNIQUE other users posted after this user's last post.
    can_post    = True if count_after >= queue_size OR user has never posted.
    """
    last = conn.execute(
        """SELECT id FROM link_queue
           WHERE chat_id=%s AND thread_id=%s AND user_id=%s
           ORDER BY id DESC LIMIT 1""",
        (chat_id, thread_id, user_id),
    ).fetchone()

    if not last:
        return (queue_size, True)

    count = conn.execute(
        """SELECT COUNT(DISTINCT user_id) AS n FROM link_queue
           WHERE chat_id=%s AND thread_id=%s AND id > %s AND user_id != %s""",
        (chat_id, thread_id, last["id"], user_id),
    ).fetchone()["n"]

    return (count, count >= queue_size)


def active_campaign(conn, chat_id: int, thread_id: int = 0):
    return conn.execute(
        """SELECT * FROM campaigns
           WHERE chat_id=%s AND thread_id=%s AND active=1
           ORDER BY id DESC LIMIT 1""",
        (chat_id, thread_id),
    ).fetchone()


def latest_campaign(conn, chat_id: int, thread_id: int = 0):
    return conn.execute(
        """SELECT * FROM campaigns
           WHERE chat_id=%s AND thread_id=%s
           ORDER BY id DESC LIMIT 1""",
        (chat_id, thread_id),
    ).fetchone()


def username_to_user(conn, username: str, chat_id: int):
    return conn.execute(
        "SELECT * FROM users WHERE username=%s AND chat_id=%s",
        (username.lstrip("@"), chat_id),
    ).fetchone()


def normalize_twitter_link(link: str) -> str:
    """Rewrite any supported Twitter/X URL into a canonical x.com form."""
    cleaned = link.strip()
    parts = urlsplit(cleaned)
    path = parts.path.rstrip("/") or "/"
    return urlunsplit(("https", "x.com", path, "", ""))


def find_existing_link(conn, chat_id: int, thread_id: int, normalized_link: str):
    """Return the first existing submission whose normalized link matches."""
    rows = conn.execute(
        "SELECT username, link FROM link_queue WHERE chat_id=%s AND thread_id=%s ORDER BY id ASC",
        (chat_id, thread_id),
    ).fetchall()
    for row in rows:
        if normalize_twitter_link(row["link"]) == normalized_link:
            return row
    return None


def build_tagall_mentions(rows) -> list[str]:
    mentions = []
    for row in rows:
        label = f"@{row['username']}" if row["username"] else row["full_name"] or f"user_{row['user_id']}"
        mentions.append(
            f'<a href="tg://user?id={row["user_id"]}">{escape(label)}</a>'
        )
    return mentions


def chunk_tagall_messages(mentions: list[str], intro: str = "") -> list[str]:
    messages = []
    prefix = f"{escape(intro.strip())}\n\n" if intro.strip() else ""
    current = prefix

    for mention in mentions:
        separator = "" if current.endswith("\n\n") or not current else " "
        candidate = f"{current}{separator}{mention}"
        if len(candidate) > TAGALL_MAX_LEN:
            if current:
                messages.append(current)
            current = mention
            prefix = ""
        else:
            current = candidate

    if current:
        messages.append(current)

    return messages


def format_drop_announcement(tg_user, link: str, wait_count: int | None = None) -> str:
    if tg_user.username:
        mention = escape(f"@{tg_user.username}")
    else:
        mention = (
            f'<a href="tg://user?id={tg_user.id}">{escape(tg_user.full_name or str(tg_user.id))}</a>'
        )
    message = f"🚀 New link from {mention}:\n\n{escape(link)}"
    if wait_count is not None:
        message += (
            f"\n\n⏳ {mention}, you must wait for {wait_count} more posts "
            "before your next link."
        )
    return message


def build_private_menu(section: str = "home") -> tuple[str, InlineKeyboardMarkup]:
    common_rows = []

    if section == "home":
        text = (
            "KOL Campaign Manager\n\n"
            "Use this private chat as a control panel and reference.\n"
            "Group actions like sessions and campaigns still run inside your group/topic.\n\n"
            "Pick a section below."
        )
        rows = [
            [
                InlineKeyboardButton("User Commands", callback_data=f"{PRIVATE_MENU_PREFIX}user"),
                InlineKeyboardButton("Sessions", callback_data=f"{PRIVATE_MENU_PREFIX}sessions"),
            ],
            [
                InlineKeyboardButton("Campaigns", callback_data=f"{PRIVATE_MENU_PREFIX}campaigns"),
                InlineKeyboardButton("Admin Tools", callback_data=f"{PRIVATE_MENU_PREFIX}admin"),
            ],
        ]
        return text, InlineKeyboardMarkup(rows)

    if section == "user":
        text = (
            "*User Commands*\n\n"
            "/mystatus — show your queue progress in the current topic\n"
            "/leaderboard — show top posters in the group\n"
            "/campaignstatus — show the active campaign for the current topic\n"
            "/mycampaignstats — show your stats in the active topic campaign\n"
            "/stats — show group stats plus current topic session settings"
        )
    elif section == "sessions":
        text = (
            "*Session Commands*\n\n"
            "/startsession or /startsession15 — start a 15-link session in the current topic\n"
            "/startsession28 — start a 28-link session in the current topic\n"
            "/stopsession — stop the current topic session\n"
            "/setqueue [n] — manually change queue size for the current topic\n"
            "/setpoints [n] — change points per link for the current topic"
        )
    elif section == "campaigns":
        text = (
            "*Campaign Commands*\n\n"
            "/newcampaign Name | Description | Target | Reward | Deadline\n"
            "/endcampaign — end the active campaign in the current topic\n"
            "/exportlinks — download a text file with every submitted link in this topic campaign, so you can review them, verify work, or send the list to whoever is paying\n"
            "/verifysub @user — verify submissions in the current topic campaign\n"
            "/removesub @user [partial link] — remove a submission from the current topic campaign"
        )
    else:
        text = (
            "*Admin Tools*\n\n"
            "/warn, /ban, /unban, /unmute, /reset, /whitelist\n"
            "/tagall [message] — mention tracked users\n"
            "/enablecmd, /disablecmd, /cmdstatus — toggle user commands per topic\n"
            "/logpayout @user [amount] [reason] — save a record that you paid someone, how much you paid, and why you paid them\n"
            "/payouts — show the latest payout records, so you can quickly check who has already been paid and who has not"
        )

    common_rows = [
        [
            InlineKeyboardButton("Home", callback_data=f"{PRIVATE_MENU_PREFIX}home"),
            InlineKeyboardButton("Sessions", callback_data=f"{PRIVATE_MENU_PREFIX}sessions"),
        ],
        [
            InlineKeyboardButton("Campaigns", callback_data=f"{PRIVATE_MENU_PREFIX}campaigns"),
            InlineKeyboardButton("Admin Tools", callback_data=f"{PRIVATE_MENU_PREFIX}admin"),
        ],
    ]
    return text, InlineKeyboardMarkup(common_rows)


# Commands that are user-accessible but can be toggled per-chat by admins
USER_COMMANDS = {"mystatus", "leaderboard", "campaignstatus", "mycampaignstats", "stats"}


# ─── ADMIN CHECK ──────────────────────────────────────────────────────────────

async def is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    try:
        member = await context.bot.get_chat_member(
            update.effective_chat.id, update.effective_user.id
        )
        return member.status in ("administrator", "creator")
    except Exception:
        return False


def is_cmd_enabled(conn, chat_id: int, thread_id: int, command: str) -> bool:
    row = conn.execute(
        "SELECT enabled FROM cmd_permissions WHERE chat_id=%s AND thread_id=%s AND command=%s",
        (chat_id, thread_id, command),
    ).fetchone()
    return bool(row and row["enabled"])


async def deny_user_cmd(update: Update, command: str):
    msg = update.message
    await msg.reply_text(
        f"⚠️ `/{command}` is not available in this topic.",
        parse_mode="Markdown",
    )
    try:
        await msg.delete()
    except Exception:
        pass


def parse_mention(update: Update) -> str | None:
    entities = update.message.entities or []
    for e in entities:
        if e.type == "mention":
            return update.message.text[e.offset + 1 : e.offset + e.length]
    return None


# ─── ESCALATION HELPER ────────────────────────────────────────────────────────

MUTE_DURATIONS = {3: 86400, 4: 259200}


async def apply_escalation(context, conn, chat_id: int, user_id: int,
                            handle: str, warnings: int, extra: str = ""):
    from datetime import timezone

    if warnings >= 5:
        try:
            await context.bot.ban_chat_member(chat_id, user_id)
            conn.execute(
                "UPDATE users SET banned=1 WHERE user_id=%s AND chat_id=%s",
                (user_id, chat_id),
            )
            await context.bot.send_message(
                chat_id,
                f"⛔ {handle} has been banned after 5 warnings.",
            )
        except Exception as e:
            logger.warning(f"Ban failed for {handle}: {e}")
            await context.bot.send_message(
                chat_id,
                f"⛔ {handle} reached 5 warnings. Manual ban required — bot lacks permission.",
            )

    elif warnings in MUTE_DURATIONS:
        seconds = MUTE_DURATIONS[warnings]
        hours = seconds // 3600
        until = datetime.now(tz=timezone.utc).timestamp() + seconds
        try:
            await context.bot.restrict_chat_member(
                chat_id,
                user_id,
                permissions=ChatPermissions(can_send_messages=False),
                until_date=int(until),
            )
            prefix = f"{extra}\n" if extra else ""
            await context.bot.send_message(
                chat_id,
                f"{prefix}⚠️ {handle} — warning {warnings}/5. "
                f"Muted for {hours} hours.",
            )
        except Exception as e:
            logger.warning(f"Mute failed for {handle}: {e}")
            await context.bot.send_message(
                chat_id,
                f"{extra}\n⚠️ {handle} — warning {warnings}/5. (Mute failed: {e})",
            )

    else:
        prefix = f"{extra}\n" if extra else ""
        await context.bot.send_message(
            chat_id,
            f"{prefix}⚠️ {handle} — warning {warnings}/5.",
        )


# ─── MESSAGE HANDLER ──────────────────────────────────────────────────────────

async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return
    if msg.chat.type not in ("group", "supergroup"):
        return

    text = (msg.text or msg.caption or "").strip()
    chat_id = msg.chat_id
    thread_id = msg.message_thread_id or 0
    tg_user = msg.from_user
    thread_kwargs = {"message_thread_id": msg.message_thread_id} if msg.message_thread_id else {}

    with db() as conn:
        settings = fetch_settings(conn, chat_id, thread_id)

        if not settings["session_active"]:
            return

        upsert_user(conn, tg_user.id, chat_id, tg_user.username or "", tg_user.full_name)
        user = fetch_user(conn, tg_user.id, chat_id)
        handle = f"@{tg_user.username}" if tg_user.username else tg_user.full_name

        twitter_matches = list(TWITTER_RE.finditer(text))
        valid_drop_message = (
            len(twitter_matches) == 1
            and twitter_matches[0].span() == (0, len(text))
        )

        if not valid_drop_message:
            try:
                await msg.delete()
            except Exception:
                pass
            await context.bot.send_message(
                chat_id,
                f"❌ {handle} — only a single Twitter/X link is allowed during link-drop sessions. "
                "Your message was removed.",
                **thread_kwargs,
            )
            return

        candidate_link = normalize_twitter_link(twitter_matches[0].group(0))
        existing = find_existing_link(conn, chat_id, thread_id, candidate_link)

        if existing:
            try:
                await msg.delete()
            except Exception:
                pass
            original_poster = f"@{existing['username']}" if existing["username"] else "someone"
            await context.bot.send_message(
                chat_id,
                f"❌ {handle} — that link was already submitted by {original_poster}. "
                "Post a different link.",
                **thread_kwargs,
            )
            return

        if user and user["whitelisted"]:
            conn.execute(
                "INSERT INTO link_queue (chat_id, thread_id, user_id, username, link) VALUES (%s,%s,%s,%s,%s)",
                (chat_id, thread_id, tg_user.id, tg_user.username or "", candidate_link),
            )
            conn.execute(
                "UPDATE users SET total_links=total_links+1, points=points+%s WHERE user_id=%s AND chat_id=%s",
                (settings["points_per_link"], tg_user.id, chat_id),
            )
            _maybe_record_campaign(conn, chat_id, thread_id, tg_user, candidate_link)
            await context.bot.send_message(
                chat_id,
                format_drop_announcement(tg_user, candidate_link),
                parse_mode="HTML",
                disable_web_page_preview=True,
                **thread_kwargs,
            )
            try:
                await msg.delete()
            except Exception:
                pass
            return

        queue_size = settings["queue_size"]
        count_after, can_post = queue_progress(conn, chat_id, thread_id, tg_user.id, queue_size)

        if not can_post:
            remaining = queue_size - count_after
            try:
                await msg.delete()
            except Exception:
                pass
            await context.bot.send_message(
                chat_id,
                f"⏳ {handle} posted too early.\n"
                f"Progress: {count_after}/{queue_size}\n"
                f"Still waiting for {remaining} more people before your next link.",
                **thread_kwargs,
            )
            return

        conn.execute(
            "INSERT INTO link_queue (chat_id, thread_id, user_id, username, link) VALUES (%s,%s,%s,%s,%s)",
            (chat_id, thread_id, tg_user.id, tg_user.username or "", candidate_link),
        )
        conn.execute(
            "UPDATE users SET total_links=total_links+1, points=points+%s WHERE user_id=%s AND chat_id=%s",
            (settings["points_per_link"], tg_user.id, chat_id),
        )
        _maybe_record_campaign(conn, chat_id, thread_id, tg_user, candidate_link)

        await context.bot.send_message(
            chat_id,
            format_drop_announcement(tg_user, candidate_link, queue_size),
            parse_mode="HTML",
            disable_web_page_preview=True,
            **thread_kwargs,
        )
        try:
            await msg.delete()
        except Exception:
            pass


def _maybe_record_campaign(conn, chat_id: int, thread_id: int, tg_user, link: str):
    camp = active_campaign(conn, chat_id, thread_id)
    if not camp:
        return
    try:
        conn.execute(
            """INSERT INTO campaign_submissions (campaign_id, user_id, username, link)
               VALUES (%s,%s,%s,%s)
               ON CONFLICT (campaign_id, link) DO NOTHING""",
            (camp["id"], tg_user.id, tg_user.username or "", link),
        )
    except Exception:
        pass


# ─── USER COMMANDS ────────────────────────────────────────────────────────────

async def cmd_mystatus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    thread_id = update.message.message_thread_id or 0
    with db() as conn:
        if not is_cmd_enabled(conn, update.effective_chat.id, thread_id, "mystatus"):
            await deny_user_cmd(update, "mystatus")
            return

    msg = update.message
    chat_id = msg.chat_id
    tg_user = msg.from_user

    with db() as conn:
        settings = fetch_settings(conn, chat_id, thread_id)
        upsert_user(conn, tg_user.id, chat_id, tg_user.username or "", tg_user.full_name)
        user = fetch_user(conn, tg_user.id, chat_id)
        queue_size = settings["queue_size"]
        count_after, can_post = queue_progress(conn, chat_id, thread_id, tg_user.id, queue_size)

    if can_post:
        status_line = " You can drop your link right now!"
    else:
        remaining = queue_size - count_after
        status_line = f"⏳ Waiting for {remaining} more people.\n📊 Progress: {count_after}/{queue_size}"

    warnings = user["warnings"] if user else 0
    total = user["total_links"] if user else 0
    points = user["points"] if user else 0

    await msg.reply_text(
        f" *Your Status*\n\n"
        f"{status_line}\n\n"
        f"🔗 Total links posted: {total}\n"
        f" Points: {points}\n"
        f"⚠️ Warnings: {warnings}/5",
        parse_mode="Markdown",
    )


async def cmd_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0
    with db() as conn:
        if not is_cmd_enabled(conn, chat_id, thread_id, "leaderboard"):
            await deny_user_cmd(update, "leaderboard")
            return
        rows = conn.execute(
            """SELECT username, full_name, total_links, points
               FROM users WHERE chat_id=%s AND total_links > 0
               ORDER BY total_links DESC LIMIT 10""",
            (chat_id,),
        ).fetchall()

    if not rows:
        await update.message.reply_text("No links dropped yet. Be the first!")
        return

    medals = ["🥇", "🥈", "🥉"] + [f"{i + 1}." for i in range(3, 10)]
    lines = ["🏆 *All-Time Leaderboard*\n"]
    for i, row in enumerate(rows):
        name = f"@{row['username']}" if row["username"] else row["full_name"]
        lines.append(f"{medals[i]} {name} — {row['total_links']} links · {row['points']} pts")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_campaignstatus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0
    with db() as conn:
        if not is_cmd_enabled(conn, chat_id, thread_id, "campaignstatus"):
            await deny_user_cmd(update, "campaignstatus")
            return

        camp = active_campaign(conn, chat_id, thread_id)
        if not camp:
            await update.message.reply_text("No active campaign right now.")
            return

        total = conn.execute(
            "SELECT COUNT(*) AS n FROM campaign_submissions WHERE campaign_id=%s",
            (camp["id"],),
        ).fetchone()["n"]

        top = conn.execute(
            """SELECT username, COUNT(*) as cnt
               FROM campaign_submissions WHERE campaign_id=%s
               GROUP BY user_id, username ORDER BY cnt DESC LIMIT 5""",
            (camp["id"],),
        ).fetchall()

    filled = min(int((total / max(camp["target"], 1)) * 10), 10)
    bar = "█" * filled + "░" * (10 - filled)
    pct = int((total / max(camp["target"], 1)) * 100)

    medals = ["🥇", "🥈", "🥉", "4.", "5."]
    top_lines = []
    for i, row in enumerate(top):
        name = f"@{row['username']}" if row["username"] else "Unknown"
        top_lines.append(f"  {medals[i]} {name} — {row['cnt']} links")

    top_block = "\n".join(top_lines) if top_lines else "  No submissions yet"

    await update.message.reply_text(
        f" *{camp['name']}*\n"
        f"_{camp['description']}_\n\n"
        f"[{bar}] {total}/{camp['target']} ({pct}%)\n\n"
        f" Reward: {camp['reward']}\n"
        f"📅 Deadline: {camp['deadline']}\n\n"
        f"🏆 Top Contributors:\n{top_block}",
        parse_mode="Markdown",
    )


async def cmd_mycampaignstats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    tg_user = update.effective_user
    thread_id = update.message.message_thread_id or 0

    with db() as conn:
        if not is_cmd_enabled(conn, chat_id, thread_id, "mycampaignstats"):
            await deny_user_cmd(update, "mycampaignstats")
            return

        camp = active_campaign(conn, chat_id, thread_id)
        if not camp:
            await update.message.reply_text("No active campaign.")
            return

        count = conn.execute(
            "SELECT COUNT(*) AS n FROM campaign_submissions WHERE campaign_id=%s AND user_id=%s",
            (camp["id"], tg_user.id),
        ).fetchone()["n"]

        rank_row = conn.execute(
            """SELECT COUNT(DISTINCT user_id) + 1 as rank FROM (
               SELECT user_id, COUNT(*) as cnt FROM campaign_submissions
               WHERE campaign_id=%s GROUP BY user_id
               HAVING COUNT(*) > (
                   SELECT COUNT(*) FROM campaign_submissions
                   WHERE campaign_id=%s AND user_id=%s
               )
            ) sub""",
            (camp["id"], camp["id"], tg_user.id),
        ).fetchone()

        rank = rank_row["rank"] if rank_row else "N/A"

    handle = f"@{tg_user.username}" if tg_user.username else tg_user.full_name
    await update.message.reply_text(
        f"📊 *{handle}'s Campaign Stats*\n\n"
        f"Campaign: {camp['name']}\n"
        f"🔗 Your submissions: {count}\n"
        f"🏅 Your rank: #{rank}",
        parse_mode="Markdown",
    )


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0
    with db() as conn:
        if not is_cmd_enabled(conn, chat_id, thread_id, "stats"):
            await deny_user_cmd(update, "stats")
            return

        settings = fetch_settings(conn, chat_id, thread_id)
        total_users = conn.execute(
            "SELECT COUNT(*) AS n FROM users WHERE chat_id=%s", (chat_id,)
        ).fetchone()["n"]

        total_links = conn.execute(
            "SELECT COALESCE(SUM(total_links),0) AS n FROM users WHERE chat_id=%s", (chat_id,)
        ).fetchone()["n"]

        total_campaigns = conn.execute(
            "SELECT COUNT(*) AS n FROM campaigns WHERE chat_id=%s AND thread_id=%s",
            (chat_id, thread_id),
        ).fetchone()["n"]

        banned = conn.execute(
            "SELECT COUNT(*) AS n FROM users WHERE chat_id=%s AND banned=1", (chat_id,)
        ).fetchone()["n"]

        warned = conn.execute(
            "SELECT COUNT(*) AS n FROM users WHERE chat_id=%s AND warnings > 0", (chat_id,)
        ).fetchone()["n"]

    session_status = "🟢 Active" if settings["session_active"] else "🔴 Inactive"

    await update.message.reply_text(
        f"📊 *Group Stats*\n\n"
        f"👥 Users tracked: {total_users}\n"
        f"🔗 Total links posted: {total_links}\n"
        f" Campaigns run: {total_campaigns}\n"
        f"⚠️ Users with warnings: {warned}\n"
        f"⛔ Banned users: {banned}\n\n"
        f"⚙️ Queue size: {settings['queue_size']}\n"
        f" Points per link: {settings['points_per_link']}\n"
        f"Session: {session_status}",
        parse_mode="Markdown",
    )


# ─── ADMIN — SESSION CONTROL ──────────────────────────────────────────────────

async def start_session(update: Update, queue_size: int):
    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0
    with db() as conn:
        update_chat_settings(conn, chat_id, thread_id, queue_size=queue_size, session_active=1)
        conn.execute(
            "DELETE FROM link_queue WHERE chat_id=%s AND thread_id=%s", (chat_id, thread_id)
        )

    await update.message.reply_text(
        f"🚀 *{queue_size}-link session started!*\n"
        "Queue has been cleared. Only Twitter/X links accepted.\n"
        f"Users must wait for {queue_size} unique posts before posting again.\n"
        "Use /stopsession to end it.",
        parse_mode="Markdown",
    )


async def cmd_startsession(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return
    await start_session(update, 15)


async def cmd_startsession15(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return
    await start_session(update, 15)


async def cmd_startsession28(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return
    await start_session(update, 28)


async def cmd_stopsession(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0
    with db() as conn:
        update_chat_settings(conn, chat_id, thread_id, session_active=0)

    await update.message.reply_text("🛑 Link-drop session stopped.")


async def cmd_setqueue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    if not context.args:
        await update.message.reply_text("Usage: /setqueue [number]")
        return

    try:
        size = int(context.args[0])
        if size < 1:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Provide a valid positive integer.")
        return

    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0
    with db() as conn:
        update_chat_settings(conn, chat_id, thread_id, queue_size=size)

    await update.message.reply_text(f" Queue size updated to {size}.")


async def cmd_setpoints(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        return

    if not context.args:
        await update.message.reply_text("Usage: /setpoints [number]")
        return

    try:
        pts = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Provide a valid integer.")
        return

    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0
    with db() as conn:
        update_chat_settings(conn, chat_id, thread_id, points_per_link=pts)

    await update.message.reply_text(f" Points per link set to {pts}.")


# ─── ADMIN — USER ACTIONS ─────────────────────────────────────────────────────

async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    username = parse_mention(update)
    if not username:
        await update.message.reply_text("Usage: /reset @username")
        return

    chat_id = update.effective_chat.id
    with db() as conn:
        target = username_to_user(conn, username, chat_id)
        if not target:
            await update.message.reply_text(f"@{username} not in records.")
            return
        conn.execute(
            "UPDATE users SET warnings=0, total_links=0, points=0 WHERE user_id=%s AND chat_id=%s",
            (target["user_id"], chat_id),
        )
        conn.execute(
            "DELETE FROM link_queue WHERE user_id=%s AND chat_id=%s",
            (target["user_id"], chat_id),
        )

    await update.message.reply_text(
        f" @{username}'s stats (warnings, links, points, queue position) have been reset."
    )


async def cmd_whitelist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    username = parse_mention(update)
    if not username:
        await update.message.reply_text("Usage: /whitelist @username")
        return

    chat_id = update.effective_chat.id
    with db() as conn:
        target = username_to_user(conn, username, chat_id)
        if not target:
            await update.message.reply_text(
                f"@{username} has no record yet. They need to post at least once."
            )
            return
        is_wl = target["whitelisted"]
        new_val = 0 if is_wl else 1
        conn.execute(
            "UPDATE users SET whitelisted=%s WHERE user_id=%s AND chat_id=%s",
            (new_val, target["user_id"], chat_id),
        )

    state = "removed from whitelist" if is_wl else "whitelisted (queue-exempt)"
    await update.message.reply_text(f" @{username} has been {state}.")


async def cmd_warn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    username = parse_mention(update)
    if not username:
        await update.message.reply_text("Usage: /warn @username")
        return

    chat_id = update.effective_chat.id
    with db() as conn:
        target = username_to_user(conn, username, chat_id)
        if not target:
            await update.message.reply_text(f"@{username} not found in records.")
            return

        new_warnings = target["warnings"] + 1
        conn.execute(
            "UPDATE users SET warnings=%s WHERE user_id=%s AND chat_id=%s",
            (new_warnings, target["user_id"], chat_id),
        )

        await apply_escalation(
            context, conn, chat_id, target["user_id"],
            f"@{username}", new_warnings,
        )


async def cmd_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    username = parse_mention(update)
    if not username:
        await update.message.reply_text("Usage: /ban @username")
        return

    chat_id = update.effective_chat.id
    with db() as conn:
        target = username_to_user(conn, username, chat_id)
        if not target:
            await update.message.reply_text(f"@{username} not in records.")
            return
        try:
            await context.bot.ban_chat_member(chat_id, target["user_id"])
            conn.execute(
                "UPDATE users SET banned=1 WHERE user_id=%s AND chat_id=%s",
                (target["user_id"], chat_id),
            )
            await update.message.reply_text(f"⛔ @{username} has been banned.")
        except Exception as e:
            await update.message.reply_text(f"❌ Ban failed: {e}")


async def cmd_unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    username = parse_mention(update)
    if not username:
        await update.message.reply_text("Usage: /unban @username")
        return

    chat_id = update.effective_chat.id
    with db() as conn:
        target = username_to_user(conn, username, chat_id)
        if not target:
            await update.message.reply_text(f"@{username} not in records.")
            return
        try:
            await context.bot.unban_chat_member(chat_id, target["user_id"])
            conn.execute(
                "UPDATE users SET banned=0, warnings=0 WHERE user_id=%s AND chat_id=%s",
                (target["user_id"], chat_id),
            )
            await update.message.reply_text(
                f" @{username} has been unbanned and warnings cleared."
            )
        except Exception as e:
            await update.message.reply_text(f"❌ Unban failed: {e}")


async def cmd_unmute(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    username = parse_mention(update)
    if not username:
        await update.message.reply_text("Usage: /unmute @username")
        return

    chat_id = update.effective_chat.id
    with db() as conn:
        target = username_to_user(conn, username, chat_id)
        if not target:
            await update.message.reply_text(f"@{username} not in records.")
            return
        try:
            await context.bot.restrict_chat_member(
                chat_id,
                target["user_id"],
                permissions=ChatPermissions(
                    can_send_messages=True,
                    can_send_other_messages=True,
                    can_add_web_page_previews=True,
                ),
            )
            await update.message.reply_text(f"@{username} has been unmuted.")
        except Exception as e:
            await update.message.reply_text(f"❌ Unmute failed: {e}")


# ─── ADMIN — CAMPAIGN MANAGEMENT ─────────────────────────────────────────────

async def cmd_newcampaign(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    raw = update.message.text.partition(" ")[2].strip()
    parts = [p.strip() for p in raw.split("|")]

    if not raw or not parts[0]:
        await update.message.reply_text(
            "Usage:\n/newcampaign Name | Description | Target | Reward | Deadline\n\n"
            "Example:\n/newcampaign Alpha Drop | Drop your X link for the alpha | 50 | 0.1 SOL each | 2025-05-01"
        )
        return

    name = parts[0]
    description = parts[1] if len(parts) > 1 else ""
    target = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 100
    reward = parts[3] if len(parts) > 3 else "TBA"
    deadline = parts[4] if len(parts) > 4 else "Open-ended"

    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0
    user_id = update.effective_user.id

    with db() as conn:
        conn.execute(
            "UPDATE campaigns SET active=0 WHERE chat_id=%s AND thread_id=%s AND active=1",
            (chat_id, thread_id),
        )
        conn.execute(
            """INSERT INTO campaigns (
                   chat_id, thread_id, name, description, target, reward, deadline, created_by
               ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (chat_id, thread_id, name, description, target, reward, deadline, user_id),
        )
        update_chat_settings(conn, chat_id, thread_id, session_active=1)
        conn.execute(
            "DELETE FROM link_queue WHERE chat_id=%s AND thread_id=%s", (chat_id, thread_id)
        )

    await update.message.reply_text(
        f" *Campaign Launched: {name}*\n\n"
        f"_{description}_\n\n"
        f"📌 Target: {target} submissions\n"
        f" Reward: {reward}\n"
        f"📅 Deadline: {deadline}\n\n"
        f"Link-drop session is now live. Drop your Twitter/X links below!\n"
        f"Use /campaignstatus to track progress.",
        parse_mode="Markdown",
    )


async def cmd_endcampaign(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        return

    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0

    with db() as conn:
        camp = active_campaign(conn, chat_id, thread_id)
        if not camp:
            await update.message.reply_text("No active campaign to end.")
            return

        total = conn.execute(
            "SELECT COUNT(*) AS n FROM campaign_submissions WHERE campaign_id=%s",
            (camp["id"],),
        ).fetchone()["n"]

        top = conn.execute(
            """SELECT username, COUNT(*) as cnt FROM campaign_submissions
               WHERE campaign_id=%s GROUP BY user_id, username ORDER BY cnt DESC LIMIT 10""",
            (camp["id"],),
        ).fetchall()

        conn.execute("UPDATE campaigns SET active=0 WHERE id=%s", (camp["id"],))
        update_chat_settings(conn, chat_id, thread_id, session_active=0)

    medals = ["🥇", "🥈", "🥉"] + [f"{i+1}." for i in range(3, 10)]
    top_lines = [
        f"  {medals[i]} @{row['username'] or 'Unknown'} — {row['cnt']} links"
        for i, row in enumerate(top)
    ]

    await update.message.reply_text(
        f"🏁 *Campaign Ended: {camp['name']}*\n\n"
        f"Total submissions: {total}\n"
        f"Target was: {camp['target']}\n\n"
        f"🏆 Final Leaderboard:\n" + "\n".join(top_lines),
        parse_mode="Markdown",
    )


async def cmd_exportlinks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        return

    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0

    with db() as conn:
        camp = latest_campaign(conn, chat_id, thread_id)
        if not camp:
            await update.message.reply_text("No campaigns found.")
            return

        rows = conn.execute(
            """SELECT username, link, submitted_at, verified FROM campaign_submissions
               WHERE campaign_id=%s ORDER BY submitted_at""",
            (camp["id"],),
        ).fetchall()

    if not rows:
        await update.message.reply_text("No submissions to export.")
        return

    lines = [
        f"Campaign: {camp['name']}",
        f"Status: {'Active' if camp['active'] else 'Ended'}",
        f"Exported: {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}",
        f"Total: {len(rows)} submissions",
        "",
        "Username | Link | Submitted At | Verified",
        "-" * 70,
    ]
    for row in rows:
        verified = "YES" if row["verified"] else "NO"
        lines.append(
            f"@{row['username'] or 'unknown'} | {row['link']} | {row['submitted_at']} | {verified}"
        )

    content = "\n".join(lines).encode("utf-8")
    filename = f"campaign_{camp['id']}_{camp['name'].replace(' ', '_')}.txt"

    await update.message.reply_document(
        document=BytesIO(content),
        filename=filename,
        caption=f"📁 *{camp['name']}* — {len(rows)} submissions exported.",
        parse_mode="Markdown",
    )


async def cmd_verifysub(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        return

    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0
    username = parse_mention(update)
    if not username:
        await update.message.reply_text("Usage: /verifysub @username")
        return

    with db() as conn:
        camp = active_campaign(conn, chat_id, thread_id)
        if not camp:
            camp = latest_campaign(conn, chat_id, thread_id)
        if not camp:
            await update.message.reply_text("No campaign found.")
            return

        target = username_to_user(conn, username, chat_id)
        if not target:
            await update.message.reply_text(f"@{username} not in records.")
            return

        result = conn.execute(
            """UPDATE campaign_submissions SET verified=1
               WHERE campaign_id=%s AND user_id=%s AND verified=0""",
            (camp["id"], target["user_id"]),
        )
        count = result.rowcount

    if count:
        await update.message.reply_text(
            f" {count} submission(s) from @{username} marked as verified."
        )
    else:
        await update.message.reply_text(
            f"No unverified submissions found for @{username}."
        )


async def cmd_removesub(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    username = parse_mention(update)
    if not username:
        await update.message.reply_text("Usage: /removesub @username [optional partial link]")
        return

    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0
    raw = update.message.text.partition(" ")[2].strip()
    parts = raw.split(None, 1)
    link_filter = parts[1].strip() if len(parts) > 1 else None

    with db() as conn:
        camp = active_campaign(conn, chat_id, thread_id)
        if not camp:
            camp = latest_campaign(conn, chat_id, thread_id)
        if not camp:
            await update.message.reply_text("No campaign found.")
            return

        target = username_to_user(conn, username, chat_id)
        if not target:
            await update.message.reply_text(f"@{username} not in records.")
            return

        if link_filter:
            row = conn.execute(
                """SELECT id, link FROM campaign_submissions
                   WHERE campaign_id=%s AND user_id=%s AND link LIKE %s
                   ORDER BY id DESC LIMIT 1""",
                (camp["id"], target["user_id"], f"%{link_filter}%"),
            ).fetchone()
        else:
            row = conn.execute(
                """SELECT id, link FROM campaign_submissions
                   WHERE campaign_id=%s AND user_id=%s
                   ORDER BY id DESC LIMIT 1""",
                (camp["id"], target["user_id"]),
            ).fetchone()

        if not row:
            await update.message.reply_text(
                f"No matching submission found for @{username}."
            )
            return

        conn.execute("DELETE FROM campaign_submissions WHERE id=%s", (row["id"],))
        conn.execute(
            "DELETE FROM link_queue WHERE chat_id=%s AND user_id=%s AND link=%s",
            (chat_id, target["user_id"], row["link"]),
        )
        conn.execute(
            "UPDATE users SET total_links=GREATEST(0, total_links-1) WHERE user_id=%s AND chat_id=%s",
            (target["user_id"], chat_id),
        )

    await update.message.reply_text(
        f"Submission removed for @{username}:\n{row['link']}\n\n"
        "Their queue position has been cleared — they can repost."
    )


async def cmd_logpayout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        return

    chat_id = update.effective_chat.id
    username = parse_mention(update)
    if not username or not context.args:
        await update.message.reply_text(
            "Usage: /logpayout @username [amount] [reason]\n"
            "Example: /logpayout @john 0.1 SOL Alpha Drop reward"
        )
        return

    all_text = update.message.text.partition(" ")[2]
    parts = all_text.split(None, 2)
    amount = parts[1] if len(parts) > 1 else "?"
    reason = parts[2] if len(parts) > 2 else ""

    with db() as conn:
        target = username_to_user(conn, username, chat_id)
        if not target:
            await update.message.reply_text(f"@{username} not in records.")
            return

        conn.execute(
            """INSERT INTO rewards (chat_id, user_id, username, amount, reason)
               VALUES (%s,%s,%s,%s,%s)""",
            (chat_id, target["user_id"], username, amount, reason),
        )

    await update.message.reply_text(
        f" Payout logged: @{username} — {amount}\nReason: {reason or 'N/A'}"
    )


async def cmd_payouts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        return

    chat_id = update.effective_chat.id
    with db() as conn:
        rows = conn.execute(
            """SELECT username, amount, reason, paid_at FROM rewards
               WHERE chat_id=%s ORDER BY id DESC LIMIT 20""",
            (chat_id,),
        ).fetchall()

    if not rows:
        await update.message.reply_text("No payouts logged yet.")
        return

    lines = [" *Recent Payouts*\n"]
    for row in rows:
        paid_str = str(row["paid_at"])[:10]
        lines.append(
            f"@{row['username']} — {row['amount']} ({row['reason'] or 'N/A'}) | {paid_str}"
        )

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_tagall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    chat_id = update.effective_chat.id
    intro = update.message.text.partition(" ")[2].strip()
    thread_kwargs = (
        {"message_thread_id": update.message.message_thread_id}
        if update.message.message_thread_id
        else {}
    )

    with db() as conn:
        rows = conn.execute(
            """
            SELECT user_id, username, full_name
            FROM users
            WHERE chat_id=%s AND banned=0
            ORDER BY COALESCE(NULLIF(username, ''), full_name, CAST(user_id AS TEXT))
            """,
            (chat_id,),
        ).fetchall()

    if not rows:
        await update.message.reply_text("No tracked members found for this chat.")
        return

    messages = chunk_tagall_messages(build_tagall_mentions(rows), intro=intro)

    for message in messages:
        await context.bot.send_message(
            chat_id,
            message,
            parse_mode="HTML",
            disable_web_page_preview=True,
            **thread_kwargs,
        )


# ─── ADMIN — COMMAND PERMISSIONS ─────────────────────────────────────────────

async def cmd_enablecmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    if not context.args:
        await update.message.reply_text(
            "Usage: /enablecmd [command]\n"
            f"Valid commands: {', '.join(sorted(USER_COMMANDS))}"
        )
        return

    cmd = context.args[0].lower().lstrip("/")
    if cmd not in USER_COMMANDS:
        await update.message.reply_text(
            f"❌ `{cmd}` is not a toggleable user command.\n"
            f"Valid: {', '.join(sorted(USER_COMMANDS))}",
            parse_mode="Markdown",
        )
        return

    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0
    with db() as conn:
        conn.execute(
            """INSERT INTO cmd_permissions (chat_id, thread_id, command, enabled)
               VALUES (%s,%s,%s,1)
               ON CONFLICT (chat_id, thread_id, command) DO UPDATE SET enabled=1""",
            (chat_id, thread_id, cmd),
        )

    topic_label = f"topic {thread_id}" if thread_id else "this group (no topic)"
    await update.message.reply_text(
        f"`/{cmd}` enabled for users in {topic_label}.",
        parse_mode="Markdown",
    )


async def cmd_disablecmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    if not context.args:
        await update.message.reply_text(
            "Usage: /disablecmd [command]\n"
            f"Valid commands: {', '.join(sorted(USER_COMMANDS))}"
        )
        return

    cmd = context.args[0].lower().lstrip("/")
    if cmd not in USER_COMMANDS:
        await update.message.reply_text(
            f"❌ `{cmd}` is not a toggleable user command.\n"
            f"Valid: {', '.join(sorted(USER_COMMANDS))}",
            parse_mode="Markdown",
        )
        return

    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0
    with db() as conn:
        conn.execute(
            """INSERT INTO cmd_permissions (chat_id, thread_id, command, enabled)
               VALUES (%s,%s,%s,0)
               ON CONFLICT (chat_id, thread_id, command) DO UPDATE SET enabled=0""",
            (chat_id, thread_id, cmd),
        )

    topic_label = f"topic {thread_id}" if thread_id else "this group (no topic)"
    await update.message.reply_text(
        f"🔒 `/{cmd}` disabled for users in {topic_label}.",
        parse_mode="Markdown",
    )


async def cmd_cmdstatus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("❌ Admins only.")
        return

    chat_id = update.effective_chat.id
    thread_id = update.message.message_thread_id or 0

    with db() as conn:
        rows = {
            row["command"]: bool(row["enabled"])
            for row in conn.execute(
                "SELECT command, enabled FROM cmd_permissions WHERE chat_id=%s AND thread_id=%s",
                (chat_id, thread_id),
            ).fetchall()
        }

    topic_label = f"Topic {thread_id}" if thread_id else "Group (no topic)"
    lines = [f"*Command Permissions — {topic_label}*\n"]
    for cmd in sorted(USER_COMMANDS):
        enabled = rows.get(cmd, False)
        icon = "🟢" if enabled else "🔴"
        lines.append(f"{icon} `/{cmd}`")

    lines.append("\nUse /enablecmd or /disablecmd to toggle.")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ─── HELP ─────────────────────────────────────────────────────────────────────

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        text, markup = build_private_menu("home")
        await update.message.reply_text(text, reply_markup=markup)
        return

    await update.message.reply_text(
        " *KOL Campaign Manager*\n\n"
        "*── User Commands ──*\n"
        "/mystatus — Your queue position & stats\n"
        "/leaderboard — All-time top posters\n"
        "/campaignstatus — Current campaign progress in this topic\n"
        "/mycampaignstats — Your campaign submissions in this topic\n"
        "/stats — Group-wide stats\n\n"
        "*── Admin: Sessions ──*\n"
        "/startsession — Start 15-link session\n"
        "/startsession15 — Start 15-link session\n"
        "/startsession28 — Start 28-link session\n"
        "/stopsession — Stop the session\n"
        "/setqueue [n] — Change queue size manually\n"
        "/setpoints [n] — Points awarded per valid link\n\n"
        "*── Admin: User Control ──*\n"
        "/reset @user — Wipe user stats & queue\n"
        "/whitelist @user — Toggle queue exemption\n"
        "/warn @user — Issue warning (3=mute 24h, 4=mute 72h, 5=ban)\n"
        "/ban @user — Ban from group\n"
        "/unban @user — Unban user\n"
        "/unmute @user — Lift a mute early\n"
        "/tagall [message] — Mention all tracked members\n\n"
        "*── Admin: Command Permissions ──*\n"
        "/enablecmd [command] — Allow users to run a command\n"
        "/disablecmd [command] — Block users from a command\n"
        "/cmdstatus — See which user commands are on/off\n\n"
        "*── Admin: Campaigns ──*\n"
        "/newcampaign Name | Desc | Target | Reward | Deadline\n"
        "/endcampaign — Close active campaign\n"
        "/exportlinks — Download all submitted links from this topic campaign as a text file\n"
        "/verifysub @user — Mark submissions verified\n"
        "/removesub @user [partial link] — Remove a submission\n"
        "/logpayout @user [amount] [reason] — Save a payment record\n"
        "/payouts — Show recent payment records\n",
        parse_mode="Markdown",
    )


async def on_private_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    await query.answer()
    if query.message.chat.type != "private":
        return

    section = query.data.removeprefix(PRIVATE_MENU_PREFIX) if query.data else "home"
    text, markup = build_private_menu(section)
    await query.edit_message_text(text, reply_markup=markup, parse_mode="Markdown")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Unhandled exception while processing update", exc_info=context.error)


def main():
    init_db()

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_error_handler(error_handler)
    app.add_handler(CallbackQueryHandler(on_private_menu, pattern=f"^{PRIVATE_MENU_PREFIX}"))

    for cmd in ["start", "help"]:
        app.add_handler(CommandHandler(cmd, cmd_help))

    app.add_handler(CommandHandler("mystatus", cmd_mystatus))
    app.add_handler(CommandHandler("leaderboard", cmd_leaderboard))
    app.add_handler(CommandHandler("campaignstatus", cmd_campaignstatus))
    app.add_handler(CommandHandler("mycampaignstats", cmd_mycampaignstats))
    app.add_handler(CommandHandler("stats", cmd_stats))

    app.add_handler(CommandHandler("startsession", cmd_startsession))
    app.add_handler(CommandHandler("startsession15", cmd_startsession15))
    app.add_handler(CommandHandler("startsession28", cmd_startsession28))
    app.add_handler(CommandHandler("stopsession", cmd_stopsession))
    app.add_handler(CommandHandler("setqueue", cmd_setqueue))
    app.add_handler(CommandHandler("setpoints", cmd_setpoints))

    app.add_handler(CommandHandler("reset", cmd_reset))
    app.add_handler(CommandHandler("whitelist", cmd_whitelist))
    app.add_handler(CommandHandler("warn", cmd_warn))
    app.add_handler(CommandHandler("ban", cmd_ban))
    app.add_handler(CommandHandler("unban", cmd_unban))
    app.add_handler(CommandHandler("unmute", cmd_unmute))
    app.add_handler(CommandHandler("tagall", cmd_tagall))

    app.add_handler(CommandHandler("enablecmd", cmd_enablecmd))
    app.add_handler(CommandHandler("disablecmd", cmd_disablecmd))
    app.add_handler(CommandHandler("cmdstatus", cmd_cmdstatus))

    app.add_handler(CommandHandler("newcampaign", cmd_newcampaign))
    app.add_handler(CommandHandler("endcampaign", cmd_endcampaign))
    app.add_handler(CommandHandler("exportlinks", cmd_exportlinks))
    app.add_handler(CommandHandler("verifysub", cmd_verifysub))
    app.add_handler(CommandHandler("removesub", cmd_removesub))
    app.add_handler(CommandHandler("logpayout", cmd_logpayout))
    app.add_handler(CommandHandler("payouts", cmd_payouts))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))

    logger.info("KOL Campaign Manager Bot is running (PostgreSQL)...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
