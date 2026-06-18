#!/usr/bin/env python3
"""
Telegram Forward Bot
Copies messages from a source channel to destination channels
without the forward tag, using a Telethon userbot.

Supports:
- Telethon string sessions        (native)
- Pyrogram string sessions v1/v2  (auto-converted to Telethon)
- Phone number login              (interactive fallback)
- Database persistence for sessions and destination channels
- /save: fetch a single restricted message and repost to destinations

Rate Limiting:
- 1 message per second
- 30 messages per minute
- 30 second break after every 30 messages
"""

import os
import asyncio
import logging
import re
import struct
import base64
import ipaddress
import shutil
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from typing import List, Optional, Dict, Any

# Load .env file for local development (no-op if file doesn't exist)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, ConversationHandler, ContextTypes, filters,
)
from telethon import TelegramClient, events as telethon_events
from telethon.sessions import StringSession
from telethon.tl.types import MessageEmpty
from telethon.errors import (
    SessionPasswordNeededError, PhoneNumberInvalidError,
    PhoneCodeInvalidError, FloodWaitError,
)

from database import Database

# ─────────────────────────────────────────────
#  Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  Downloads Folder Setup
# ─────────────────────────────────────────────
DOWNLOADS_DIR = Path(__file__).parent / "downloads"
DOWNLOADS_DIR.mkdir(exist_ok=True)
logger.info(f"Downloads folder: {DOWNLOADS_DIR.absolute()}")

# Configure Telethon to use this folder for media downloads
os.environ["TL_DOWNLOAD_PATH"] = str(DOWNLOADS_DIR)

# ─────────────────────────────────────────────
#  Config
# ─────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
API_ID    = int(os.getenv("API_ID", "0"))
API_HASH  = os.getenv("API_HASH", "")

if not BOT_TOKEN or API_ID == 0 or not API_HASH:
    raise ValueError("BOT_TOKEN, API_ID, and API_HASH are required.")

db = Database("bot_data.db")

# ─────────────────────────────────────────────
#  Conversation states
# ─────────────────────────────────────────────
(
    WAITING_SESSION,        # 0
    WAITING_SOURCE,         # 1  (reserved)
    WAITING_DEST_ADD,       # 2
    WAITING_PHONE,          # 3
    WAITING_CODE,           # 4
    WAITING_PASSWORD,       # 5
    WAITING_FORWARD_FIRST,  # 6
    WAITING_FORWARD_LAST,   # 7
    WAITING_SAVE_LINK,      # 8
    WAITING_BULK_SAVE_FIRST,# 9
    WAITING_BULK_SAVE_LAST, # 10
    WAITING_WATCH_SOURCE,   # 11
    WAITING_INCOMING,       # 12
    WAITING_OUTGOING,       # 13
    WAITING_ADD_ROUTE_SRC,  # 14
    WAITING_ADD_ROUTE_DST,  # 15
    WAITING_FILTER,         # 16
    WAITING_BLACKLIST,      # 17
    WAITING_WHITELIST,      # 18
    WAITING_DELAY,          # 19
    WAITING_BEGIN_TEXT,     # 20
    WAITING_END_TEXT,       # 21
    WAITING_FILTER_USERS,   # 22
    WAITING_REMOVE_ROUTE,   # 23
    WAITING_REMOVE_FILTER,  # 24
    WAITING_REM_BLACKLIST,  # 25
    WAITING_REM_WHITELIST,  # 26
) = range(27)

# ─────────────────────────────────────────────
#  Global userbot reference
# ─────────────────────────────────────────────
userbot_client: Optional[TelegramClient] = None
forward_task:   Optional[asyncio.Task]   = None

# ── Auto-Forward globals ──────────────────────────────────────────
auto_forward_handler = None          # registered Telethon event handler fn (legacy)
auto_forward_task_user: Optional[int] = None  # user who owns the active watch
_pending_albums: Dict[int, List]    = {}      # grouped_id → [Message, ...]
_album_tasks:   Dict[int, asyncio.Task] = {}  # grouped_id → flush Task
_route_handlers: Dict[str, Any]     = {}      # source_channel → handler fn (route mode)

# ─────────────────────────────────────────────
#  DC address table  (for Pyrogram → Telethon conversion)
# ─────────────────────────────────────────────
_DC_IPS: Dict[int, str] = {
    1: "149.154.175.53",
    2: "149.154.167.51",
    3: "149.154.175.100",
    4: "149.154.167.91",
    5: "91.108.56.130",
}
_DC_PORT = 443


# ═══════════════════════════════════════════════════════
#  SESSION HELPERS
# ═══════════════════════════════════════════════════════

def _b64d(s: str) -> bytes:
    s = s.strip()
    pad = len(s) % 4
    if pad:
        s += "=" * (4 - pad)
    return base64.urlsafe_b64decode(s)


def _pyrogram_to_telethon(session_string: str) -> Optional[str]:
    try:
        data = _b64d(session_string)
    except Exception:
        return None

    _FORMATS = [
        (262, ">BI?256s",   lambda d: (d[0], d[3])),
        (266, ">B256sQ?",   lambda d: (d[0], d[1])),
        (269, ">i256sQ?",   lambda d: (d[0], d[1])),
        (271, ">BI?256sQ?", lambda d: (d[0], d[3])),
    ]

    dc_id: Optional[int] = None
    auth_key: Optional[bytes] = None

    for size, fmt, extractor in _FORMATS:
        if len(data) == size:
            try:
                parts = struct.unpack(fmt, data)
                dc_id, auth_key = extractor(parts)
                break
            except struct.error:
                continue

    if dc_id is None or auth_key is None:
        return None
    if dc_id not in _DC_IPS:
        return None

    try:
        ip_bytes = ipaddress.ip_address(_DC_IPS[dc_id]).packed
        telethon_data = struct.pack(">B4sH256s", dc_id, ip_bytes, _DC_PORT, auth_key)
        return base64.urlsafe_b64encode(telethon_data).decode().rstrip("=")
    except Exception:
        return None


def _make_client(session_str: str = "") -> TelegramClient:
    """
    Create a TelegramClient with sensible defaults.
    connection_retries=2 prevents infinite hangs on bad sessions.
    """
    return TelegramClient(
        StringSession(session_str),
        API_ID,
        API_HASH,
        connection_retries=2,
        retry_delay=1,
    )


async def _try_telethon_session(session_str: str) -> Optional[TelegramClient]:
    """
    Try to connect a Telethon client.
    Uses get_me() (real server round-trip) — more reliable than is_user_authorized().
    Returns an authorised connected client, or None.
    """
    client = None
    try:
        client = _make_client(session_str)
        await client.connect()
        me = await client.get_me()          # real server call, catches revoked sessions
        if me is not None:
            logger.info(f"Session verified: {me.first_name} ({me.id})")
            return client
        logger.debug("_try_telethon_session: get_me() returned None — not authorised")
    except Exception as exc:
        logger.debug(f"_try_telethon_session failed: {type(exc).__name__}: {exc}")
    if client:
        try:
            await client.disconnect()
        except Exception:
            pass
    return None


async def _resolve_any_session(raw: str) -> Optional[tuple]:
    """
    Accept Telethon or Pyrogram (v1/v2) string sessions.
    Returns (connected_client, telethon_session_string) or None.
    """
    raw = raw.strip().strip("`\"'")

    # 1. Try as-is (Telethon native)
    client = await _try_telethon_session(raw)
    if client:
        logger.info("Session accepted as native Telethon StringSession.")
        return client, raw

    # 2. Try converting from Pyrogram
    converted = _pyrogram_to_telethon(raw)
    if converted:
        client = await _try_telethon_session(converted)
        if client:
            logger.info("Session converted Pyrogram → Telethon.")
            return client, converted

    logger.warning("Could not authenticate with any known session format.")
    return None


# ═══════════════════════════════════════════════════════
#  USERBOT GETTER  — FIX: use get_me() not is_user_authorized()
# ═══════════════════════════════════════════════════════

async def get_userbot(user_id: int) -> Optional[TelegramClient]:
    """
    Return a live, authorised TelegramClient.
    Always uses get_me() for server-side verification so stale/revoked
    sessions are detected immediately rather than failing silently.
    """
    global userbot_client

    # ── Re-use existing live client ──────────────────────────────
    if userbot_client:
        try:
            me = await userbot_client.get_me()   # FIX: was is_user_authorized()
            if me is not None:
                return userbot_client
        except Exception:
            pass
        try:
            await userbot_client.disconnect()
        except Exception:
            pass
        userbot_client = None

    # ── Reconnect from stored session ────────────────────────────
    session_str = db.get_session(user_id)
    if not session_str:
        return None

    try:
        client = _make_client(session_str)
        await client.connect()
        me = await client.get_me()           # FIX: was is_user_authorized()
        if me is not None:
            logger.info(f"Userbot reconnected: {me.first_name} ({me.id})")
            userbot_client = client
            return client
        await client.disconnect()
    except Exception as exc:
        logger.error(f"get_userbot reconnect failed: {type(exc).__name__}: {exc}")

    return None


# ═══════════════════════════════════════════════════════
#  MISC HELPERS
# ═══════════════════════════════════════════════════════

def _parse_channel(channel: str):
    """Return int for numeric IDs, str for @usernames."""
    channel = channel.strip()
    try:
        return int(channel)
    except ValueError:
        return channel


def validate_channel_format(channel: str) -> bool:
    """
    Accept:
      @username
      -1001234567890  (full negative ID)
      1001234567890   (positive ID without minus)
      https://t.me/channelname  (extract @username)
    """
    if not channel or not isinstance(channel, str):
        return False
    c = channel.strip()
    # Extract from t.me URL
    m = re.match(r"https?://t\.me/([A-Za-z][A-Za-z0-9_]{2,})", c)
    if m:
        return True
    if c.startswith("@"):
        return len(c) > 1 and re.match(r"^@[A-Za-z][A-Za-z0-9_]{2,}$", c) is not None
    if c.startswith("-"):
        return c[1:].isdigit()
    return c.isdigit()


def normalise_channel(channel: str) -> str:
    """
    Convert t.me URLs to @username format.
    Leaves @username and numeric IDs unchanged.
    """
    c = channel.strip()
    m = re.match(r"https?://t\.me/([A-Za-z][A-Za-z0-9_]{2,})", c)
    if m:
        return f"@{m.group(1)}"
    return c


def extract_session_string(raw: str) -> str:
    raw = raw.strip()
    m = re.search(r"```(?:\w+)?\s*\n?(.*?)\n?```", raw, re.DOTALL)
    if m:
        return m.group(1).strip()
    m = re.search(r"`([^`]+)`", raw)
    if m:
        return m.group(1).strip()
    return raw.strip("`\"'")


# ═══════════════════════════════════════════════════════
#  KEYBOARDS
# ═══════════════════════════════════════════════════════

def main_menu_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📖 Help",      callback_data="help"),
            InlineKeyboardButton("⚙️ Settings",  callback_data="settings"),
        ],
        [InlineKeyboardButton("🔐 Login Userbot", callback_data="login")],
    ])


def settings_keyboard(user_id: int):
    dest_channels = db.get_destination_channels(user_id)
    dest_count = len(dest_channels)
    rows = [
        [InlineKeyboardButton("➕ Add Destination Channel", callback_data="add_dest")],
    ]
    if dest_count > 0:
        rows.append([InlineKeyboardButton("🗑 Manage Destinations", callback_data="manage_dests")])
    rows.append([InlineKeyboardButton("◀️ Back", callback_data="back_main")])
    return InlineKeyboardMarkup(rows)


def manage_dests_keyboard(user_id: int):
    channels = db.get_destination_channels(user_id)
    rows = []
    for i, ch in enumerate(channels):
        display = ch.get("channel_name") or ch["channel_id"]
        rows.append([
            InlineKeyboardButton(f"❌ Remove: {display[:35]}", callback_data=f"remove_dest_{i}")
        ])
    rows.append([InlineKeyboardButton("◀️ Back to Settings", callback_data="settings")])
    return InlineKeyboardMarkup(rows)


# ═══════════════════════════════════════════════════════
#  /start  /help  /settings  /status  /stop
# ═══════════════════════════════════════════════════════

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return
    db.add_user(user.id, user.first_name, user.last_name or "")
    has_session = db.get_session(user.id) is not None
    status = "✅ Userbot connected" if has_session else "⚠️ Userbot not logged in"
    text = (
        f"👋 *Welcome, {user.first_name}!*\n\n"
        "I copy messages from a source channel to your destination channels "
        "*without* the forward tag.\n\n"
        f"*Status:* {status}\n\n"
        "Use the buttons below to get started:"
    )
    await update.message.reply_text(text, parse_mode="Markdown",
                                    reply_markup=main_menu_keyboard())


HELP_TEXT = (
    "📖 *Bot Commands & Usage*\n\n"
    "▸ `/start` — Welcome message\n"
    "▸ `/help` — This help page\n"
    "▸ `/login` — Connect userbot (string session or phone)\n"
    "▸ `/forward` — Copy a message range from any source channel\n"
    "▸ `/save` — Fetch & repost a single restricted message link\n"
    "▸ `/watch` — 🔴 *Auto-forward* new messages from a source channel\n"
    "▸ `/unwatch` — Stop auto-forwarding\n"
    "▸ `/status` — Show bot status & downloads folder info\n"
    "▸ `/cleanup` — Manually cleanup old downloaded files\n"
    "▸ `/stop` — Stop an active copy job\n"
    "▸ `/settings` — Add / remove destination channels\n\n"
    "🔴 *Auto-Forward (/watch):*\n"
    "The userbot listens for *new messages* in a source channel and\n"
    "instantly forwards them to all your destination channels.\n"
    "Albums (media groups) are batched and sent together.\n"
    "Auto-forward survives bot restarts — runs until you `/unwatch`.\n\n"
    "🔐 *Supported Session Formats:*\n"
    "• Telethon StringSession\n"
    "• Pyrogram StringSession v1 & v2 *(auto-converted)*\n"
    "• Phone number login *(interactive)*\n\n"
    "📌 *Channel Format:* `@username`, `-1001234567890`, or `t.me/name`\n\n"
    "📥 */save — Restricted Message Saver:*\n"
    "Paste any `t.me/c/...` or `t.me/channel/...` link and the bot\n"
    "will download the message via your userbot and repost it to all\n"
    "your destination channels — preserving all media, captions,\n"
    "formatting, and albums.\n\n"
    "💾 *Downloads Folder:*\n"
    "Media files are automatically downloaded to `./downloads/` folder\n"
    "and cleaned up after sending. Use `/cleanup` to manually remove old files.\n\n"
    "⏱️ *Rate Limiting (for /forward & /save):*\n"
    "• 1 message per second\n"
    "• 30 messages per minute\n"
    "• 30 second break after every 30 messages"
)


async def help_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="back_main")]])
    if update.message:
        await update.message.reply_text(HELP_TEXT, parse_mode="Markdown", reply_markup=kb)
    else:
        await update.callback_query.edit_message_text(HELP_TEXT, parse_mode="Markdown", reply_markup=kb)


async def settings_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    dest_count = len(db.get_destination_channels(user_id))
    text = (
        "⚙️ *Settings*\n\n"
        f"📤 *Destinations:* {dest_count} channel(s)\n\n"
        "_Source channel is set each time you run /forward._"
    )
    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown",
                                        reply_markup=settings_keyboard(user_id))
    else:
        await update.callback_query.edit_message_text(text, parse_mode="Markdown",
                                                       reply_markup=settings_keyboard(user_id))


async def status_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    session  = db.get_session(user_id)
    dests    = db.get_destination_channels(user_id)
    active   = forward_task and not forward_task.done()

    # Auto-forward watch status
    watch_cfg    = db.get_watch_config(user_id)
    watching     = bool(watch_cfg and watch_cfg.get("is_active") and auto_forward_task_user == user_id)
    watch_source = (watch_cfg or {}).get("source_channel", "—")

    # Downloads folder info
    downloads_count   = len(list(DOWNLOADS_DIR.glob("*"))) if DOWNLOADS_DIR.exists() else 0
    downloads_size    = sum(f.stat().st_size for f in DOWNLOADS_DIR.glob("*") if f.is_file())
    downloads_size_mb = downloads_size / (1024 * 1024)

    watch_line = f"\n   └─ Source: `{watch_source}`" if watching else ""
    text = (
        "📊 *Bot Status*\n\n"
        f"🔐 Userbot:       {'✅ Connected' if session else '❌ Not connected'}\n"
        f"📤 Destinations:  {len(dests)} channel(s)\n"
        f"🔄 Copy job:      {'▶️ Running' if active else '⏹ Idle'}\n"
        f"🔴 Auto-forward:  {'▶️ Watching' if watching else '⏹ Off'}{watch_line}\n\n"
        f"💾 *Downloads Folder:*\n"
        f"📁 Files: `{downloads_count}`\n"
        f"💽 Size: `{downloads_size_mb:.2f} MB`\n"
        f"📍 Path: `./downloads/`"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def stop_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global forward_task
    if forward_task and not forward_task.done():
        forward_task.cancel()
        await update.message.reply_text("🛑 Copy job cancelled.")
    else:
        await update.message.reply_text("No active copy job to stop.")


async def cleanup_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    Manually cleanup old downloaded files (older than 3 days).
    """
    try:
        deleted_count = _cleanup_downloads_folder(max_age_days=3)
        downloads_count = len(list(DOWNLOADS_DIR.glob("*"))) if DOWNLOADS_DIR.exists() else 0
        downloads_size = sum(f.stat().st_size for f in DOWNLOADS_DIR.glob("*") if f.is_file())
        downloads_size_mb = downloads_size / (1024 * 1024)
        
        text = (
            f"🧹 *Downloads Cleanup*\n\n"
            f"✅ Removed: `{deleted_count}` old file(s)\n\n"
            f"📊 *Remaining:*\n"
            f"📁 Files: `{downloads_count}`\n"
            f"💽 Size: `{downloads_size_mb:.2f} MB`"
        )
        await update.message.reply_text(text, parse_mode="Markdown")
    except Exception as exc:
        logger.error(f"Cleanup command error: {exc}")
        await update.message.reply_text(f"❌ Cleanup failed: {exc}")


# ═══════════════════════════════════════════════════════
#  LOGIN CONVERSATION
# ═══════════════════════════════════════════════════════

async def login_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
    text = (
        "🔐 *Login Userbot*\n\n"
        "Send one of the following:\n\n"
        "1️⃣ *String session* — Telethon or Pyrogram (v1/v2). "
        "The bot auto-detects the format.\n"
        "2️⃣ *Phone number* — e.g. `+1234567890` to login interactively.\n\n"
        "_Your session is stored encrypted locally._"
    )
    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)
    else:
        await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
    return WAITING_SESSION


async def receive_session(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id  = update.effective_user.id
    raw_text = (update.message.text or "").strip()

    db.add_user(user_id, update.effective_user.first_name,
                update.effective_user.last_name or "")

    # Delete the message so the session string isn't visible in chat
    try:
        await update.message.delete()
    except Exception:
        pass

    # ── Phone number branch ──────────────────────────────────────
    if re.match(r"^\+\d{5,15}$", raw_text):
        ctx.user_data["phone_number"] = raw_text
        await update.effective_chat.send_message("📱 Sending verification code…")
        return await start_phone_login(update, ctx)

    session_str = extract_session_string(raw_text)

    if not session_str or len(session_str) < 10:
        await update.effective_chat.send_message(
            "❓ That doesn't look like a valid session string.\n"
            "Send your phone number (e.g. `+1234567890`) to login interactively.",
            parse_mode="Markdown",
        )
        return WAITING_SESSION

    msg = await update.effective_chat.send_message("⏳ Verifying session…")

    result = await _resolve_any_session(session_str)

    if result is None:
        await msg.edit_text(
            "❌ Could not authenticate with this session.\n\n"
            "Supported formats: Telethon, Pyrogram v1, Pyrogram v2.\n\n"
            "Or send your phone number (e.g. `+1234567890`) to login interactively.",
            parse_mode="Markdown",
        )
        return WAITING_SESSION

    client, telethon_str = result
    me = await client.get_me()

    global userbot_client
    userbot_client = client
    db.save_session(user_id, telethon_str)

    await msg.edit_text(
        f"✅ *Logged in as:* {me.first_name} (`{me.id}`)\n\n"
        "Userbot is ready. Use /forward or /save.",
        parse_mode="Markdown",
        reply_markup=main_menu_keyboard(),
    )
    return ConversationHandler.END


# ── Phone login ─────────────────────────────────────────────────

async def start_phone_login(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    phone = ctx.user_data.get("phone_number")
    if not phone and update.message and update.message.text:
        phone = update.message.text.strip()
        ctx.user_data["phone_number"] = phone

    if not phone:
        await update.effective_chat.send_message(
            "📞 Send your phone number with country code (e.g. `+1234567890`).",
            parse_mode="Markdown",
        )
        return WAITING_PHONE

    try:
        client = _make_client()
        await client.connect()
        await client.send_code_request(phone)
        ctx.user_data["temp_client"] = client
        await update.effective_chat.send_message(
            "📨 Verification code sent to your Telegram account.\n\n"
            "Send the code with a *space between each digit*\n"
            "Example: `2 3 5 6 3`",
            parse_mode="Markdown",
        )
        return WAITING_CODE
    except PhoneNumberInvalidError:
        await update.effective_chat.send_message("❌ Invalid phone number. Try again:")
        return WAITING_PHONE
    except Exception as exc:
        logger.error(f"start_phone_login: {type(exc).__name__}: {exc}")
        await update.effective_chat.send_message("❌ Error sending code. Please try again.")
        return WAITING_PHONE


async def receive_code(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    code    = (update.message.text or "").strip().replace(" ", "")
    try:
        await update.message.delete()
    except Exception:
        pass

    client = ctx.user_data.get("temp_client")
    phone  = ctx.user_data.get("phone_number")
    if not client:
        await update.effective_chat.send_message("Session expired. Use /login again.")
        return ConversationHandler.END

    try:
        await client.sign_in(phone, code)
        me          = await client.get_me()
        session_str = client.session.save()
        global userbot_client
        userbot_client = client
        db.save_session(user_id, session_str)
        await update.effective_chat.send_message(
            f"✅ *Logged in as:* {me.first_name} (`{me.id}`)\n\n"
            "Userbot is ready. Use /forward or /save.",
            parse_mode="Markdown",
            reply_markup=main_menu_keyboard(),
        )
        return ConversationHandler.END
    except SessionPasswordNeededError:
        await update.effective_chat.send_message(
            "🔐 2-step verification enabled. Send your cloud password:"
        )
        return WAITING_PASSWORD
    except PhoneCodeInvalidError:
        await update.effective_chat.send_message("❌ Invalid code. Try again:")
        return WAITING_CODE
    except Exception as exc:
        logger.error(f"receive_code: {type(exc).__name__}: {exc}")
        await update.effective_chat.send_message("❌ Error. Start over with /login.")
        return ConversationHandler.END


async def receive_password(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id  = update.effective_user.id
    password = (update.message.text or "").strip()
    try:
        await update.message.delete()
    except Exception:
        pass

    client = ctx.user_data.get("temp_client")
    if not client:
        await update.effective_chat.send_message("Session expired. Use /login again.")
        return ConversationHandler.END

    try:
        await client.sign_in(password=password)
        me          = await client.get_me()
        session_str = client.session.save()
        global userbot_client
        userbot_client = client
        db.save_session(user_id, session_str)
        await update.effective_chat.send_message(
            f"✅ *Logged in as:* {me.first_name} (`{me.id}`)\n\n"
            "Userbot is ready.",
            parse_mode="Markdown",
            reply_markup=main_menu_keyboard(),
        )
        return ConversationHandler.END
    except Exception as exc:
        logger.error(f"receive_password: {type(exc).__name__}: {exc}")
        await update.effective_chat.send_message("❌ Wrong password or error. Try again:")
        return WAITING_PASSWORD


# ═══════════════════════════════════════════════════════
#  ADD DESTINATION CONVERSATION  — FIX: query.answer() + add_user + normalise
# ═══════════════════════════════════════════════════════

async def ask_dest(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()                    # FIX: was missing — stops loading spinner
    await query.edit_message_text(
        "➕ *Add Destination Channel*\n\n"
        "Send the channel username, numeric ID, or t.me link.\n\n"
        "Examples:\n"
        "• `@mychannel`\n"
        "• `-1001234567890`\n"
        "• `https://t.me/mychannel`\n\n"
        "_The userbot must be a member/admin of that channel._",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel", callback_data="settings")]
        ]),
    )
    return WAITING_DEST_ADD


async def receive_dest(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    raw     = (update.message.text or "").strip()

    # FIX: ensure user row exists before inserting destination (FK safety)
    db.add_user(user_id, update.effective_user.first_name,
                update.effective_user.last_name or "")

    if not validate_channel_format(raw):
        await update.message.reply_text(
            "❌ Invalid format.\n\n"
            "Accepted formats:\n"
            "• `@username`\n"
            "• `-1001234567890`\n"
            "• `https://t.me/channelname`",
            parse_mode="Markdown",
        )
        return WAITING_DEST_ADD

    # Normalise t.me URLs → @username
    channel = normalise_channel(raw)

    # FIX: check if already active before inserting
    existing = db.get_destination_channel_ids(user_id)
    if channel in existing:
        await update.message.reply_text(
            f"⚠️ `{channel}` is already in your destinations.",
            parse_mode="Markdown",
            reply_markup=settings_keyboard(user_id),
        )
        return ConversationHandler.END

    db.add_destination_channel(user_id, channel)
    await update.message.reply_text(
        f"✅ Added `{channel}` to destinations.",
        parse_mode="Markdown",
        reply_markup=settings_keyboard(user_id),
    )
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════
#  LINK PARSING HELPERS
# ═══════════════════════════════════════════════════════

def _parse_message_link(text: str) -> Optional[tuple]:
    """
    Parse a t.me message link.
      Public:  https://t.me/channame/123   → ("@channame", 123)
      Private: https://t.me/c/12345678/99  → ("-10012345678", 99)
    """
    text = text.strip()
    m = re.search(r"t\.me/c/(\d+)/(\d+)", text)
    if m:
        return f"-100{m.group(1)}", int(m.group(2))
    m = re.search(r"t\.me/([A-Za-z][A-Za-z0-9_]{2,})/(\d+)", text)
    if m:
        return f"@{m.group(1)}", int(m.group(2))
    return None


def _extract_forwarded_origin(message) -> Optional[tuple]:
    origin = getattr(message, "forward_origin", None)
    if origin is not None:
        chat   = getattr(origin, "chat", None)
        msg_id = getattr(origin, "message_id", None)
        if chat and msg_id:
            channel = f"@{chat.username}" if chat.username else str(chat.id)
            return channel, msg_id
    fwd_chat   = getattr(message, "forward_from_chat", None)
    fwd_msg_id = getattr(message, "forward_from_message_id", None)
    if fwd_chat and fwd_msg_id:
        channel = f"@{fwd_chat.username}" if fwd_chat.username else str(fwd_chat.id)
        return channel, fwd_msg_id
    return None


def _resolve_msg_ref(message) -> Optional[tuple]:
    result = _extract_forwarded_origin(message)
    if result:
        return result
    text = message.text or message.caption or ""
    return _parse_message_link(text)


# ═══════════════════════════════════════════════════════
#  /forward CONVERSATION
# ═══════════════════════════════════════════════════════

async def forward_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    message = update.message or (update.callback_query and update.callback_query.message)
    if not message:
        return

    if not db.get_session(user_id):
        await message.reply_text("❌ Userbot not logged in. Use /login first.")
        return ConversationHandler.END

    if not db.get_destination_channel_ids(user_id):
        await message.reply_text("❌ No destination channels set. Add one via /settings.")
        return ConversationHandler.END

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
    await message.reply_text(
        "📨 *Step 1 of 2 — First Message*\n\n"
        "Forward the *first* message you want to copy from the source channel\n"
        "— or paste its message link.\n\n"
        "Example: `https://t.me/sourcechannel/100`",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    return WAITING_FORWARD_FIRST


async def receive_forward_first(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    result = _resolve_msg_ref(update.message)
    if result is None:
        await update.message.reply_text(
            "❌ Couldn't read that.\n\n"
            "Please *forward* the first message from the source channel, "
            "or paste a link like:\n`https://t.me/channelname/100`",
            parse_mode="Markdown",
        )
        return WAITING_FORWARD_FIRST

    source, first_id = result
    ctx.user_data["fwd_source"]   = source
    ctx.user_data["fwd_first_id"] = first_id

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
    await update.message.reply_text(
        f"✅ First message: ID `{first_id}` from `{source}`\n\n"
        "📨 *Step 2 of 2 — Last Message*\n\n"
        "Now forward the *last* message you want to copy\n"
        "— or paste its message link.",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    return WAITING_FORWARD_LAST


async def receive_forward_last(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    result  = _resolve_msg_ref(update.message)

    if result is None:
        await update.message.reply_text(
            "❌ Couldn't read that.\n\n"
            "Please *forward* the last message from the source channel, "
            "or paste a link like:\n`https://t.me/channelname/200`",
            parse_mode="Markdown",
        )
        return WAITING_FORWARD_LAST

    _chan, last_id = result
    source   = ctx.user_data.get("fwd_source")
    first_id = ctx.user_data.get("fwd_first_id")

    if last_id < first_id:
        await update.message.reply_text(
            f"❌ Last ID (`{last_id}`) is before first (`{first_id}`).\n"
            "Please link a *later* message:",
            parse_mode="Markdown",
        )
        return WAITING_FORWARD_LAST

    client = await get_userbot(user_id)
    if not client:
        await update.message.reply_text(
            "❌ Could not connect userbot. Check your session or run /login again."
        )
        return ConversationHandler.END

    destinations = db.get_destination_channel_ids(user_id)
    session_id   = db.start_forwarding_session(user_id)
    if session_id == -1:
        await update.message.reply_text("❌ Could not start copy session (DB error).")
        return ConversationHandler.END

    progress_msg = await update.message.reply_text(
        f"🚀 *Starting copy job…*\n\n"
        f"📡 Source: `{source}`\n"
        f"🔢 Messages: `{first_id}` → `{last_id}`\n"
        f"📤 Destinations: {len(destinations)}\n\n"
        f"⏳ Collecting messages…",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🛑 Stop", callback_data=f"stop_forwarding_{session_id}")]
        ]),
    )

    global forward_task
    forward_task = asyncio.create_task(
        _copy_range_loop(
            app=ctx.application,
            client=client,
            source_str=source,
            destinations=destinations,
            first_id=first_id,
            last_id=last_id,
            progress_msg=progress_msg,
            session_id=session_id,
            user_id=user_id,
        )
    )
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════
#  /save CONVERSATION
# ═══════════════════════════════════════════════════════

async def save_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not db.get_session(user_id):
        await update.message.reply_text("❌ Userbot not logged in. Use /login first.")
        return ConversationHandler.END

    if not db.get_destination_channel_ids(user_id):
        await update.message.reply_text("❌ No destination channels set. Add one via /settings.")
        return ConversationHandler.END

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📦 Bulk Save (Range)", callback_data="bulk_save")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")],
    ])
    await update.message.reply_text(
        "📥 */save — Restricted Message Saver*\n\n"
        "Send me the message link you want to save and repost.\n\n"
        "Supported formats:\n"
        "• `https://t.me/c/1234567890/99` *(private channel)*\n"
        "• `https://t.me/channelname/99` *(public channel)*\n\n"
        "The userbot will fetch the message via MTProto and send it to\n"
        "all your destination channels — preserving media, captions,\n"
        "formatting, and albums.\n\n"
        "💡 *Or tap* 📦 *Bulk Save* *to save a full range of messages.*",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    return WAITING_SAVE_LINK


async def receive_save_link(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text    = (update.message.text or update.message.caption or "").strip()

    parsed = _parse_message_link(text)
    if not parsed:
        await update.message.reply_text(
            "❌ That doesn't look like a valid message link.\n\n"
            "Please send a link like:\n"
            "`https://t.me/c/1234567890/99`\n"
            "`https://t.me/channelname/99`",
            parse_mode="Markdown",
        )
        return WAITING_SAVE_LINK

    channel_str, msg_id = parsed

    status_msg = await update.message.reply_text(
        f"⏳ *Fetching message…*\n\n"
        f"📡 Source: `{channel_str}`\n"
        f"🔢 Message ID: `{msg_id}`",
        parse_mode="Markdown",
    )

    client = await get_userbot(user_id)
    if not client:
        await status_msg.edit_text(
            "❌ Could not connect userbot. Check your session or run /login again."
        )
        return ConversationHandler.END

    destinations = db.get_destination_channel_ids(user_id)

    asyncio.create_task(
        _save_and_repost(
            app=ctx.application,
            client=client,
            channel_str=channel_str,
            msg_id=msg_id,
            destinations=destinations,
            status_msg=status_msg,
        )
    )
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════
#  BULK SAVE CONVERSATION  (triggered by 📦 button in /save)
# ═══════════════════════════════════════════════════════

async def bulk_save_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Called when user taps the 📦 Bulk Save button inside the /save flow."""
    query = update.callback_query
    await query.answer()

    user_id = update.effective_user.id
    if not db.get_session(user_id):
        await query.edit_message_text("❌ Userbot not logged in. Use /login first.")
        return ConversationHandler.END

    if not db.get_destination_channel_ids(user_id):
        await query.edit_message_text("❌ No destination channels set. Add one via /settings.")
        return ConversationHandler.END

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
    await query.edit_message_text(
        "📦 *Bulk Save — Step 1 of 2*\n\n"
        "Send the link of the *first* message you want to save.\n\n"
        "Example:\n"
        "`https://t.me/c/1234567890/100`\n"
        "`https://t.me/channelname/100`",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    return WAITING_BULK_SAVE_FIRST


async def receive_bulk_save_first(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Receive the first message link for bulk save."""
    text = (update.message.text or "").strip()
    parsed = _parse_message_link(text)

    if not parsed:
        await update.message.reply_text(
            "❌ Couldn't parse that link.\n\n"
            "Please send a valid message link:\n"
            "`https://t.me/c/1234567890/100`\n"
            "`https://t.me/channelname/100`",
            parse_mode="Markdown",
        )
        return WAITING_BULK_SAVE_FIRST

    channel_str, first_id = parsed
    ctx.user_data["bulk_channel"] = channel_str
    ctx.user_data["bulk_first_id"] = first_id

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
    await update.message.reply_text(
        f"✅ First message: ID `{first_id}` from `{channel_str}`\n\n"
        "📦 *Bulk Save — Step 2 of 2*\n\n"
        "Now send the link of the *last* message you want to save.",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    return WAITING_BULK_SAVE_LAST


async def receive_bulk_save_last(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Receive the last message link and kick off the bulk save loop."""
    user_id = update.effective_user.id
    text = (update.message.text or "").strip()
    parsed = _parse_message_link(text)

    if not parsed:
        await update.message.reply_text(
            "❌ Couldn't parse that link.\n\n"
            "Please send a valid message link:\n"
            "`https://t.me/c/1234567890/200`",
            parse_mode="Markdown",
        )
        return WAITING_BULK_SAVE_LAST

    _chan, last_id = parsed
    channel_str = ctx.user_data.get("bulk_channel")
    first_id    = ctx.user_data.get("bulk_first_id")

    if not channel_str or first_id is None:
        await update.message.reply_text("❌ Session data lost. Please run /save again.")
        return ConversationHandler.END

    if last_id < first_id:
        await update.message.reply_text(
            f"❌ Last ID (`{last_id}`) is before first (`{first_id}`).\n"
            "Please send a *later* message link:",
            parse_mode="Markdown",
        )
        return WAITING_BULK_SAVE_LAST

    client = await get_userbot(user_id)
    if not client:
        await update.message.reply_text(
            "❌ Could not connect userbot. Check your session or run /login again."
        )
        return ConversationHandler.END

    destinations = db.get_destination_channel_ids(user_id)
    if not destinations:
        await update.message.reply_text("❌ No destination channels configured.")
        return ConversationHandler.END

    session_id = db.start_forwarding_session(user_id)
    if session_id == -1:
        await update.message.reply_text("❌ Could not start bulk save session (DB error).")
        return ConversationHandler.END

    progress_msg = await update.message.reply_text(
        f"🚀 *Starting Bulk Save…*\n\n"
        f"📡 Source: `{channel_str}`\n"
        f"🔢 Range: `{first_id}` → `{last_id}`\n"
        f"📤 Destinations: {len(destinations)}\n\n"
        f"⏳ Fetching messages… (skips empty/deleted ones)",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🛑 Stop", callback_data=f"stop_forwarding_{session_id}")]
        ]),
    )

    global forward_task
    forward_task = asyncio.create_task(
        _bulk_save_loop(
            app=ctx.application,
            client=client,
            channel_str=channel_str,
            destinations=destinations,
            first_id=first_id,
            last_id=last_id,
            progress_msg=progress_msg,
            session_id=session_id,
            user_id=user_id,
        )
    )
    return ConversationHandler.END


async def _bulk_save_loop(
    app,
    client: TelegramClient,
    channel_str: str,
    destinations: List[str],
    first_id: int,
    last_id: int,
    progress_msg,
    session_id: int,
    user_id: int,
):
    """
    Bulk restricted-message saver.
    Fetches each message in [first_id, last_id] via MTProto (get_messages),
    skips deleted/empty ones, and re-posts to all destinations using _safe_send
    (downloads locally, re-uploads — same engine as single /save).
    Applies rate limiting: 1 msg/sec, 30/min, 30-sec break.
    """

    async def _edit_progress(text: str, done: bool = False):
        try:
            kb = None if done else InlineKeyboardMarkup([
                [InlineKeyboardButton("🛑 Stop", callback_data=f"stop_forwarding_{session_id}")]
            ])
            await app.bot.edit_message_text(
                chat_id=progress_msg.chat_id,
                message_id=progress_msg.message_id,
                text=text,
                parse_mode="Markdown",
                reply_markup=kb,
            )
        except Exception:
            pass

    saved     = 0
    failed    = 0
    skipped   = 0
    processed = 0
    total_range = last_id - first_id + 1

    messages_in_minute = 0
    minute_start_time  = asyncio.get_event_loop().time()
    last_send_time     = 0.0
    last_edit_time     = 0.0

    try:
        # Resolve source entity once
        try:
            source_entity = await client.get_entity(_parse_channel(channel_str))
        except Exception as exc:
            await _edit_progress(
                f"❌ *Cannot access channel* `{channel_str}`\n\n"
                f"Error: `{type(exc).__name__}: {exc}`\n\n"
                "Make sure the userbot has *joined* the source channel.",
                done=True,
            )
            db.end_forwarding_session(session_id)
            return

        # Resolve destination entities
        dest_entities: Dict[str, Any] = {}
        for dest in destinations:
            try:
                dest_entities[dest] = await client.get_entity(_parse_channel(dest))
            except Exception as exc:
                logger.warning(f"Bulk save: cannot resolve dest {dest}: {exc}")

        if not dest_entities:
            await _edit_progress(
                "❌ No destination channels could be resolved.\n\n"
                "Make sure the userbot is a member/admin in each destination.",
                done=True,
            )
            db.end_forwarding_session(session_id)
            return

        # Iterate through IDs one by one (handles gaps/deleted messages gracefully)
        seen_grouped_ids: set = set()   # avoid re-sending the same album twice

        for msg_id in range(first_id, last_id + 1):
            processed += 1

            # ── Rate limiting ────────────────────────────────────────
            current_time = asyncio.get_event_loop().time()
            if current_time - minute_start_time >= 60:
                messages_in_minute = 0
                minute_start_time  = current_time

            if messages_in_minute >= 30:
                await _edit_progress(
                    f"📦 *Bulk Save — Rate Limiting…*\n\n"
                    f"✅ Saved:    `{saved}`\n"
                    f"⏭️ Skipped:  `{skipped}`\n"
                    f"❌ Failed:   `{failed}`\n"
                    f"📊 Progress: `{processed} / {total_range}`\n\n"
                    f"⏸️ Taking 30-second break…"
                )
                await asyncio.sleep(30)
                messages_in_minute = 0
                minute_start_time  = asyncio.get_event_loop().time()

            time_since_last = asyncio.get_event_loop().time() - last_send_time
            if time_since_last < 1.0:
                await asyncio.sleep(1.0 - time_since_last)

            # ── Fetch message ─────────────────────────────────────────
            try:
                fetched = await client.get_messages(source_entity, ids=[msg_id])
            except FloodWaitError as fwe:
                wait = fwe.seconds + 2
                await _edit_progress(
                    f"📦 *Bulk Save — FloodWait*\n\n"
                    f"⏳ Telegram asked to wait `{wait}s`…\n\n"
                    f"✅ Saved: `{saved}` | ❌ Failed: `{failed}`"
                )
                await asyncio.sleep(wait)
                try:
                    fetched = await client.get_messages(source_entity, ids=[msg_id])
                except Exception:
                    skipped += 1
                    continue
            except Exception as exc:
                logger.warning(f"Bulk save: get_messages({msg_id}) failed: {exc}")
                skipped += 1
                continue

            msg = fetched[0] if fetched else None
            if msg is None or isinstance(msg, MessageEmpty):
                skipped += 1
                continue

            # ── Handle grouped/album messages ─────────────────────────
            grouped_id = getattr(msg, "grouped_id", None)
            album_msgs: Optional[List] = None

            if grouped_id:
                # Skip if we already sent this album from a previous message ID
                if grouped_id in seen_grouped_ids:
                    skipped += 1
                    continue
                seen_grouped_ids.add(grouped_id)
                try:
                    window = []
                    async for m in client.iter_messages(
                        source_entity,
                        min_id=max(1, msg_id - 10),
                        max_id=msg_id + 11,
                        limit=25,
                    ):
                        window.append(m)
                    album_msgs = sorted(
                        [m for m in window
                         if m and not isinstance(m, MessageEmpty)
                         and getattr(m, "grouped_id", None) == grouped_id],
                        key=lambda m: m.id,
                    )
                    if not album_msgs:
                        album_msgs = [msg]
                except Exception:
                    album_msgs = [msg]

            any_ok = False
            for dest_key, dest_entity in dest_entities.items():
                ok = await _safe_send(
                    client=client,
                    dest=dest_entity,
                    msg=msg if not album_msgs else None,
                    album=album_msgs if album_msgs else None,
                )
                if ok:
                    any_ok = True

            if any_ok:
                saved += 1
            else:
                failed += 1

            messages_in_minute += 1
            last_send_time = asyncio.get_event_loop().time()
            db.update_forwarding_stats(session_id, saved, failed)

            now = asyncio.get_event_loop().time()
            if now - last_edit_time >= 3 or processed % 5 == 0:
                last_edit_time = now
                await _edit_progress(
                    f"📦 *Bulk Save in Progress…*\n\n"
                    f"✅ Saved:    `{saved}`\n"
                    f"⏭️ Skipped:  `{skipped}` *(empty/deleted/dup)*\n"
                    f"❌ Failed:   `{failed}`\n"
                    f"📊 Progress: `{processed} / {total_range}`\n\n"
                    f"⏱️ This minute: `{messages_in_minute} / 30`"
                )

        db.end_forwarding_session(session_id)
        await _edit_progress(
            f"✅ *Bulk Save Complete!*\n\n"
            f"📡 Source: `{channel_str}`\n"
            f"✅ Saved:    `{saved}`\n"
            f"⏭️ Skipped:  `{skipped}` *(deleted/empty)*\n"
            f"❌ Failed:   `{failed}`\n"
            f"📤 Destinations: {len(dest_entities)}",
            done=True,
        )

    except asyncio.CancelledError:
        db.end_forwarding_session(session_id)
        await _edit_progress(
            f"🛑 *Bulk Save Stopped.*\n\n"
            f"✅ Saved so far: `{saved}`\n"
            f"⏭️ Skipped:      `{skipped}`\n"
            f"❌ Failed:       `{failed}`",
            done=True,
        )
    except Exception as exc:
        logger.exception(f"_bulk_save_loop crashed: {exc}")
        db.end_forwarding_session(session_id)
        await _edit_progress(
            f"💥 *Unexpected error:* `{type(exc).__name__}`\n\n"
            f"✅ Saved so far: `{saved}`\n"
            f"❌ Failed:       `{failed}`",
            done=True,
        )


async def _save_and_repost(
    app,
    client: TelegramClient,
    channel_str: str,
    msg_id: int,
    destinations: List[str],
    status_msg,
):
    """
    Fetch a single message (or album) from a restricted channel via MTProto
    and repost it to all destinations without a Forwarded-from tag.

    Key fixes vs previous version:
    - Uses get_messages(ids=[list]) not ids=int (more reliable)
    - Checks for MessageEmpty (Telegram returns these for deleted/inaccessible messages)
    - Album fetching uses iter_messages (correct API for range queries)
    - send_file passes caption_entities separately for proper formatting
    """

    async def _edit(text: str):
        try:
            await app.bot.edit_message_text(
                chat_id=status_msg.chat_id,
                message_id=status_msg.message_id,
                text=text,
                parse_mode="Markdown",
            )
        except Exception:
            pass

    # ── 1. Resolve source entity ─────────────────────────────────
    try:
        source_entity = await client.get_entity(_parse_channel(channel_str))
    except Exception as exc:
        await _edit(
            f"❌ *Cannot access channel* `{channel_str}`\n\n"
            f"Error: `{type(exc).__name__}: {exc}`\n\n"
            "Make sure the userbot has *joined* the source channel."
        )
        return

    # ── 2. Fetch the target message ───────────────────────────────
    # Pass ids as a LIST so Telethon returns a list — more consistent
    # than passing a bare int (which returns a single object).
    try:
        fetched = await client.get_messages(source_entity, ids=[msg_id])
    except Exception as exc:
        await _edit(
            f"❌ *Failed to fetch message* `{msg_id}`\n\n"
            f"Error: `{type(exc).__name__}: {exc}`"
        )
        return

    # Telethon returns a list when ids= is a list
    if not fetched:
        await _edit(f"❌ Message `{msg_id}` not found in `{channel_str}`.")
        return

    target_msg = fetched[0] if isinstance(fetched, list) else fetched

    # FIX: check for MessageEmpty — Telegram sends these for deleted/restricted messages
    if target_msg is None or isinstance(target_msg, MessageEmpty):
        await _edit(
            f"❌ Message `{msg_id}` is empty or inaccessible.\n\n"
            "The message may have been deleted, or the userbot cannot read it\n"
            "(make sure it has joined the channel, not just the link preview)."
        )
        return

    # ── 3. Expand album (grouped media) ──────────────────────────
    grouped_id = getattr(target_msg, "grouped_id", None)
    album_msgs: Optional[List] = None

    if grouped_id:
        await _edit(
            f"⏳ *Fetching album…*\n\n"
            f"📡 Source: `{channel_str}`\n"
            f"🔢 Message ID: `{msg_id}`\n"
            f"📸 Collecting album parts…"
        )
        try:
            # FIX: use iter_messages for range — get_messages doesn't support min_id/max_id
            window = []
            async for m in client.iter_messages(
                source_entity,
                min_id=max(1, msg_id - 10),
                max_id=msg_id + 11,
                limit=25,
            ):
                window.append(m)

            album_msgs = sorted(
                [m for m in window
                 if m and not isinstance(m, MessageEmpty)
                 and getattr(m, "grouped_id", None) == grouped_id],
                key=lambda m: m.id,
            )
            if not album_msgs:
                album_msgs = [target_msg]   # fallback to single message
        except Exception as exc:
            logger.warning(f"Album fetch failed, falling back to single: {exc}")
            album_msgs = [target_msg]

    # ── 4. Resolve destination entities ──────────────────────────
    dest_entities: Dict[str, Any] = {}
    for dest in destinations:
        try:
            dest_entities[dest] = await client.get_entity(_parse_channel(dest))
        except Exception as exc:
            logger.warning(f"Cannot resolve dest {dest}: {type(exc).__name__}: {exc}")

    if not dest_entities:
        await _edit(
            "❌ No destination channels could be resolved.\n\n"
            "Make sure the userbot is a member/admin in each destination."
        )
        return

    n_files = len(album_msgs) if album_msgs else 1
    await _edit(
        f"📤 *Sending to {len(dest_entities)} destination(s)…*\n\n"
        f"📡 Source: `{channel_str}`\n"
        f"🔢 Message ID: `{msg_id}`"
        + (f"\n📸 Album: `{n_files}` file(s)" if album_msgs and len(album_msgs) > 1 else "")
    )

    # ── 5. Send to each destination ───────────────────────────────
    sent_ok   = 0
    failed_ds = []

    for dest_key, dest_entity in dest_entities.items():
        ok = await _safe_send(
            client=client,
            dest=dest_entity,
            msg=target_msg if not album_msgs else None,
            album=album_msgs if album_msgs else None,
        )
        if ok:
            sent_ok += 1
        else:
            failed_ds.append(dest_key)

    # ── 6. Final status ───────────────────────────────────────────
    if sent_ok == len(dest_entities):
        icon, note = "✅", f"Sent to all *{sent_ok}* destination(s)."
    elif sent_ok > 0:
        icon, note = "⚠️", (
            f"Sent to *{sent_ok}* destination(s).\n"
            f"Failed: `{'`, `'.join(failed_ds)}`"
        )
    else:
        icon, note = "❌", "Failed to send to any destination."

    await _edit(
        f"{icon} *Save complete!*\n\n"
        f"📡 Source: `{channel_str}`\n"
        f"🔢 Message ID: `{msg_id}`\n"
        f"📤 {note}"
    )


# ═══════════════════════════════════════════════════════
#  FILE CLEANUP HELPERS
# ═══════════════════════════════════════════════════════

def _cleanup_file(file_path: Optional[str]) -> bool:
    """
    Safely delete a downloaded file.
    Returns True if deleted, False if file doesn't exist or error occurs.
    """
    if not file_path:
        return False
    
    try:
        path = Path(file_path)
        if path.exists() and path.is_file():
            path.unlink()
            logger.debug(f"Cleaned up: {file_path}")
            return True
    except Exception as exc:
        logger.warning(f"Failed to cleanup {file_path}: {exc}")
    return False


def _cleanup_files(file_paths: List[str]) -> int:
    """
    Clean up multiple files.
    Returns count of successfully deleted files.
    """
    if not file_paths:
        return 0
    
    deleted_count = 0
    for fp in file_paths:
        if _cleanup_file(fp):
            deleted_count += 1
    return deleted_count


def _cleanup_downloads_folder(max_age_days: int = 3) -> int:
    """
    Cleanup old files from downloads folder (auto-maintenance).
    Deletes files older than max_age_days.
    Returns count of deleted files.
    """
    if not DOWNLOADS_DIR.exists():
        return 0
    
    import time
    current_time = time.time()
    max_age_seconds = max_age_days * 24 * 60 * 60
    deleted_count = 0
    
    try:
        for file_path in DOWNLOADS_DIR.glob("*"):
            if file_path.is_file():
                file_age = current_time - file_path.stat().st_mtime
                if file_age > max_age_seconds:
                    try:
                        file_path.unlink()
                        logger.debug(f"Auto-cleanup old file: {file_path.name}")
                        deleted_count += 1
                    except Exception as exc:
                        logger.warning(f"Failed to auto-cleanup {file_path}: {exc}")
    except Exception as exc:
        logger.warning(f"Auto-cleanup folder scan failed: {exc}")
    
    return deleted_count


async def _cleanup_old_files_async():
    """
    Async wrapper for cleanup task (runs in background).
    Cleans up files older than 3 days.
    """
    try:
        deleted = _cleanup_downloads_folder(max_age_days=3)
        if deleted > 0:
            logger.info(f"Auto-cleanup: removed {deleted} old file(s)")
    except Exception as exc:
        logger.warning(f"Async cleanup failed: {exc}")


# ═══════════════════════════════════════════════════════
#  CORE COPY ENGINE  — _safe_send with full format support
# ═══════════════════════════════════════════════════════

async def _safe_send(
    client: TelegramClient,
    dest,
    msg=None,
    album: Optional[List] = None,
    caption: Optional[str] = None,
    retries: int = 3,
) -> Any:
    """
    Copy a single message or album to dest without a Forwarded-from tag.
    Preserves: media, captions, text formatting entities, stickers, documents.
    
    FIX: For restricted channel media, downloads locally then re-uploads.
    Downloaded files are stored in ./downloads/ and cleaned up after sending.
    """
    downloaded_files = []  # Track files for cleanup
    
    try:
        for attempt in range(retries):
            try:
                # ── Album / media group ──────────────────────────────
                if album:
                    # Filter out any MessageEmpty entries
                    valid = [m for m in album if m and not isinstance(m, MessageEmpty) and m.media]
                    if not valid:
                        logger.warning("Album has no valid media")
                        return True     # nothing to send
                    
                    # Use the first message's text as caption if not overridden
                    if caption is None:
                        send_caption = next((m.message for m in valid if m.message), "")
                        caption_entities = next(
                            (m.entities for m in valid if m.message and m.entities), None
                        )
                        parse_mode = None
                    else:
                        send_caption = caption
                        caption_entities = None
                        parse_mode = 'md'
                    
                    # ── FIX: Download and re-upload media for restricted channels ──
                    file_paths = []
                    try:
                        for idx, m in enumerate(valid):
                            try:
                                # Create unique filename
                                file_ext = ""
                                if m.document:
                                    file_ext = m.document.mime_type.split("/")[-1] if m.document.mime_type else "file"
                                elif m.photo:
                                    file_ext = "jpg"
                                elif m.video:
                                    file_ext = "mp4"
                                elif m.audio:
                                    file_ext = "mp3"
                                else:
                                    file_ext = "media"
                                
                                download_filename = f"{m.id}_{idx}.{file_ext}"
                                download_path = DOWNLOADS_DIR / download_filename
                                
                                logger.info(f"Downloading album media {idx+1}/{len(valid)}: {download_filename}")
                                
                                # Download file locally with proper error handling
                                file_path = await client.download_media(m.media, file=str(download_path))
                                
                                if file_path and Path(file_path).exists():
                                    file_paths.append(file_path)
                                    downloaded_files.append(file_path)
                                    logger.info(f"✓ Downloaded: {file_path} ({Path(file_path).stat().st_size} bytes)")
                                else:
                                    logger.warning(f"Download failed or file doesn't exist: {file_path}")
                                    # Try direct media reference as fallback
                                    file_paths.append(m.media)
                                    
                            except Exception as dl_exc:
                                logger.error(f"Album media download error (idx={idx}): {type(dl_exc).__name__}: {dl_exc}")
                                # Fall back to direct media reference
                                file_paths.append(m.media)
                        
                        if file_paths:
                            logger.info(f"Sending album with {len(file_paths)} media files...")
                            sent_msgs = await client.send_file(
                                dest,
                                file_paths,
                                caption=send_caption,
                                formatting_entities=caption_entities,
                                parse_mode=parse_mode,
                            )
                            logger.info("✓ Album sent successfully")
                            return sent_msgs
                        return None
                    except Exception as send_exc:
                        logger.error(f"Album send failed: {type(send_exc).__name__}: {send_exc}")
                        raise

                if msg is None:
                    return None

                # ── Media (photo, video, doc, audio, sticker, …) ────
                if msg.media:
                    logger.info(f"Processing media message: {type(msg.media).__name__}")
                    file_to_send = msg.media
                    
                    # ── FIX: For restricted channels, download then re-upload ──
                    try:
                        # Determine file extension based on media type
                        file_ext = "media"
                        if msg.document:
                            # Get extension from document
                            if msg.document.mime_type:
                                file_ext = msg.document.mime_type.split("/")[-1]
                            elif msg.document.attributes:
                                for attr in msg.document.attributes:
                                    if hasattr(attr, 'file_name'):
                                        file_ext = attr.file_name.split('.')[-1] if '.' in attr.file_name else 'file'
                                        break
                        elif msg.photo:
                            file_ext = "jpg"
                        elif msg.video:
                            file_ext = "mp4"
                        elif msg.audio:
                            file_ext = "mp3"
                        elif msg.voice:
                            file_ext = "ogg"
                        
                        # Create download path
                        download_filename = f"{msg.id}.{file_ext}"
                        download_path = DOWNLOADS_DIR / download_filename
                        
                        logger.info(f"Attempting to download: {download_filename}")
                        
                        # Download to organized downloads folder
                        downloaded_path = await client.download_media(msg.media, file=str(download_path))
                        
                        if downloaded_path and Path(downloaded_path).exists():
                            file_to_send = downloaded_path
                            downloaded_files.append(downloaded_path)
                            file_size = Path(downloaded_path).stat().st_size
                            file_size_mb = file_size / (1024 * 1024)
                            logger.info(f"✓ Downloaded: {downloaded_path} ({file_size_mb:.2f} MB)")
                        else:
                            logger.warning(f"Download returned no file or file doesn't exist")
                            
                    except Exception as dl_exc:
                        logger.error(f"Media download error: {type(dl_exc).__name__}: {dl_exc}")
                        logger.warning("Falling back to direct media send")
                        # Proceed with direct media reference as fallback
                    
                    try:
                        logger.info(f"Sending file: {file_to_send}")
                        send_caption = caption if caption is not None else (msg.message or "")
                        caption_entities = None if caption is not None else (msg.entities if msg.entities else None)
                        parse_mode = 'md' if caption is not None else None
                        
                        sent = await client.send_file(
                            dest,
                            file_to_send,
                            caption=send_caption,
                            formatting_entities=caption_entities,
                            parse_mode=parse_mode,
                        )
                        logger.info("✓ Media sent successfully")
                        return sent
                    except Exception as send_exc:
                        logger.error(f"send_file failed: {type(send_exc).__name__}: {send_exc}")
                        raise

                # ── Plain text ────────────────────────────────────────
                if msg.message or caption is not None:
                    logger.info("Sending text message")
                    send_text = caption if caption is not None else msg.message
                    caption_entities = None if caption is not None else (msg.entities if msg.entities else None)
                    parse_mode = 'md' if caption is not None else None
                    
                    sent = await client.send_message(
                        dest,
                        send_text,
                        formatting_entities=caption_entities,
                        parse_mode=parse_mode,
                    )
                    logger.info("✓ Text sent successfully")
                    return sent

                logger.warning("Message has no media or text")
                return None  # empty — skip silently

            except FloodWaitError as fwe:
                wait = fwe.seconds + 3
                logger.warning(f"FloodWait error — sleeping {wait}s")
                await asyncio.sleep(wait)

            except Exception as exc:
                logger.error(f"_safe_send attempt {attempt + 1}/{retries}: {type(exc).__name__}: {exc}")
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt)

        logger.error("All retry attempts failed")
        return None
    
    finally:
        # ── CLEANUP: Delete downloaded files after sending ──
        if downloaded_files:
            try:
                cleanup_count = _cleanup_files(downloaded_files)
                if cleanup_count > 0:
                    logger.info(f"✓ Cleaned up {cleanup_count} file(s) from downloads folder")
                
                # Run periodic cleanup of old files (optional)
                if asyncio.get_event_loop():
                    asyncio.create_task(_cleanup_old_files_async())
            except Exception as cleanup_exc:
                logger.error(f"Cleanup error: {cleanup_exc}")


# ═══════════════════════════════════════════════════════
#  FILTER PIPELINE HELPERS
# ═══════════════════════════════════════════════════════

def _passes_filters(user_id: int, text: str, sender_id: Optional[int]) -> bool:
    """
    Returns True if the message should be forwarded.
    Priority: user_filter → blacklist → whitelist.
    """
    # User-filter: only allow specific senders (if list is non-empty)
    user_filters = db.get_user_filters(user_id)
    if user_filters and sender_id not in user_filters:
        logger.debug(f"Message from {sender_id} dropped by user_filter.")
        return False

    content = (text or "").lower()

    # Blacklist: drop if any keyword found
    for kw in db.get_blacklist(user_id):
        if kw in content:
            logger.debug(f"Message dropped by blacklist keyword '{kw}'.")
            return False

    # Whitelist: drop if no keyword found (only enforced when list is non-empty)
    whitelist = db.get_whitelist(user_id)
    if whitelist and not any(kw in content for kw in whitelist):
        logger.debug("Message dropped: no whitelist keyword found.")
        return False

    return True


def _apply_text_filters(user_id: int, text: str) -> str:
    """Apply text-replacement filters and [mono] formatting."""
    if not text:
        return text
    result = text
    for f in db.get_filters(user_id):
        pattern     = f["pattern"]
        replacement = f["replacement"]
        if f.get("is_regex"):
            try:
                result = re.sub(pattern, replacement, result)
            except re.error as exc:
                logger.warning(f"Regex filter error '{pattern}': {exc}")
        else:
            result = result.replace(pattern, replacement)
    # [mono]text[/mono] → `text`
    result = re.sub(r"\[mono\](.*?)\[/mono\]", r"`\1`", result, flags=re.DOTALL)
    return result


def _apply_placeholders(text: str, sender) -> str:
    """Replace [user.X] placeholders with live user info."""
    if not text or not sender:
        return text
    username   = getattr(sender, "username", None) or ""
    uid        = str(getattr(sender, "id", ""))
    first_name = getattr(sender, "first_name", "") or ""
    last_name  = getattr(sender, "last_name", "")  or ""
    text = text.replace("[user.username]",          f"@{username}" if username else uid)
    text = text.replace("[user.id]",                uid)
    text = text.replace("[user.first_name]",        first_name)
    text = text.replace("[user.last_name]",         last_name)
    text = text.replace("[user.username | user.id]", f"@{username}" if username else uid)
    return text


async def _build_caption(user_id: int, original_text: str, sender,
                         cfg: Dict) -> str:
    """Assemble the final caption: filters → placeholders → begin/end text."""
    text = _apply_text_filters(user_id, original_text)
    text = _apply_placeholders(text, sender)
    if cfg.get("begin_text"):
        text = cfg["begin_text"] + ("\n" if text else "") + text
    if cfg.get("end_text"):
        text = text + ("\n" if text else "") + cfg["end_text"]
    return text


# ═══════════════════════════════════════════════════════
#  AUTO-FORWARD ENGINE  (real-time /watch feature)
# ═══════════════════════════════════════════════════════

async def _flush_album(grouped_id: int, client: TelegramClient, user_id: int,
                       destinations: List[str], source_str: str):
    """
    Wait 1.5 s, then send all batched album parts to destinations.
    Applies the filter pipeline to the album caption.
    """
    await asyncio.sleep(1.5)
    album = _pending_albums.pop(grouped_id, [])
    _album_tasks.pop(grouped_id, None)
    if not album:
        return
    album.sort(key=lambda m: m.id)

    cfg = db.get_config(user_id)
    if not cfg.get("media_enabled", True):
        return

    # Build caption from the first message that has text
    first_text = next((m.message for m in album if m.message), "") or ""
    sender = None
    try:
        sender = await client.get_entity(album[0].sender_id)
    except Exception:
        pass
    caption = await _build_caption(user_id, first_text, sender, cfg)

    for dest_str in destinations:
        try:
            dest_entity = await client.get_entity(_parse_channel(dest_str))
            await _safe_send(client, dest_entity, album=album, caption=caption)
        except Exception as exc:
            logger.error(f"Auto-forward album → {dest_str} failed: {exc}")


def _stop_auto_forward(client: Optional[TelegramClient] = None):
    """Remove all registered NewMessage/edit/delete event handlers."""
    global auto_forward_handler, auto_forward_task_user, _route_handlers

    # Remove legacy single-handler
    if client and auto_forward_handler:
        try:
            client.remove_event_handler(auto_forward_handler)
            logger.info("Auto-forward legacy handler removed.")
        except Exception as exc:
            logger.warning(f"Could not remove legacy handler: {exc}")
    auto_forward_handler = None

    # Remove all route-based handlers
    if client:
        for src, handler in list(_route_handlers.items()):
            try:
                client.remove_event_handler(handler)
                logger.info(f"Route handler removed for source: {src}")
            except Exception as exc:
                logger.warning(f"Could not remove route handler ({src}): {exc}")
    _route_handlers.clear()

    auto_forward_task_user = None

    # Cancel pending album timers
    for task in list(_album_tasks.values()):
        task.cancel()
    _album_tasks.clear()
    _pending_albums.clear()


def _make_message_handler(client: TelegramClient, user_id: int,
                          source_str: str, get_dests_fn):
    """
    Factory that returns a NewMessage handler coroutine.
    get_dests_fn() is called at runtime to get fresh destination list.
    """
    async def _on_new_message(event):
        try:
            msg = event.message
            if not msg or isinstance(msg, MessageEmpty):
                return

            # Get sender for filtering/placeholders
            sender = None
            sender_id = None
            try:
                sender = await event.get_sender()
                sender_id = sender.id if sender else None
            except Exception:
                pass

            text = msg.message or ""

            # ── Filter pipeline ─────────────────────────────────────
            if not _passes_filters(user_id, text, sender_id):
                return

            cfg = db.get_config(user_id)

            # ── Media / text toggle ─────────────────────────────────
            has_media = msg.media is not None
            if has_media and not cfg.get("media_enabled", True):
                return
            if not has_media and not cfg.get("text_enabled", True):
                return

            # ── Delay ───────────────────────────────────────────────
            delay = cfg.get("delay", 0)
            if delay > 0:
                await asyncio.sleep(delay)

            # ── Get destinations ────────────────────────────────────
            destinations = get_dests_fn()
            if not destinations:
                return

            grouped_id = getattr(msg, "grouped_id", None)

            if grouped_id:
                # Album: batch parts, flush after 1.5 s
                if grouped_id not in _pending_albums:
                    _pending_albums[grouped_id] = []
                _pending_albums[grouped_id].append(msg)
                existing = _album_tasks.get(grouped_id)
                if existing and not existing.done():
                    existing.cancel()
                _album_tasks[grouped_id] = asyncio.create_task(
                    _flush_album(grouped_id, client, user_id, destinations, source_str)
                )
            else:
                # Single message
                final_caption = await _build_caption(user_id, text, sender, cfg)

                for dest_str in destinations:
                    try:
                        dest_entity = await client.get_entity(_parse_channel(dest_str))

                        if cfg.get("copy_mode", True):
                            # Copy mode: download & re-upload (no forward tag)
                            if has_media:
                                # Override caption via dedicated send
                                sent = await _safe_send(client, dest_entity, msg=msg, caption=final_caption)
                            else:
                                # Text-only — use modified caption
                                no_preview = not cfg.get("url_preview", False)
                                sent = await client.send_message(
                                    dest_entity,
                                    final_caption or msg.message,
                                    link_preview=not no_preview,
                                )
                        else:
                            # Native forward mode (shows "Forwarded from" tag)
                            sent = await client.forward_messages(dest_entity, msg)

                        # ── Edit/Delete sync: store mapping ─────────────
                        cfg_sync = cfg.get("edit_sync") or cfg.get("delete_sync")
                        if cfg_sync:
                            try:
                                sent_id = sent.id if hasattr(sent, "id") else None
                                if sent_id:
                                    db.save_message_mapping(
                                        user_id, source_str, msg.id, dest_str, sent_id
                                    )
                            except Exception:
                                pass

                    except Exception as exc:
                        logger.error(f"Auto-forward → {dest_str} failed: {exc}")

        except Exception as exc:
            logger.exception(f"_on_new_message error: {exc}")

    return _on_new_message


def _make_edit_handler(client: TelegramClient, user_id: int, source_str: str):
    """Handler for edited messages — updates forwarded copies."""
    async def _on_edit(event):
        try:
            cfg = db.get_config(user_id)
            if not cfg.get("edit_sync"):
                return
            msg = event.message
            if not msg:
                return
            mappings = db.get_message_mappings(user_id, source_str, msg.id)
            if not mappings:
                return
            new_text = msg.message or ""
            sender = None
            try:
                sender = await event.get_sender()
            except Exception:
                pass
            final = await _build_caption(user_id, new_text, sender, cfg)
            for m in mappings:
                try:
                    dest_entity = await client.get_entity(_parse_channel(m["dest_chat"]))
                    await client.edit_message(dest_entity, m["dest_msg_id"], final)
                except Exception as exc:
                    logger.warning(f"Edit sync failed for {m['dest_chat']}: {exc}")
        except Exception as exc:
            logger.exception(f"_on_edit error: {exc}")
    return _on_edit


def _make_delete_handler(client: TelegramClient, user_id: int, source_str: str):
    """Handler for deleted messages — deletes forwarded copies."""
    async def _on_delete(event):
        try:
            cfg = db.get_config(user_id)
            if not cfg.get("delete_sync"):
                return
            for msg_id in event.deleted_ids:
                mappings = db.get_message_mappings(user_id, source_str, msg_id)
                for m in mappings:
                    try:
                        dest_entity = await client.get_entity(_parse_channel(m["dest_chat"]))
                        await client.delete_messages(dest_entity, [m["dest_msg_id"]])
                    except Exception as exc:
                        logger.warning(f"Delete sync failed for {m['dest_chat']}: {exc}")
                db.delete_message_mappings(user_id, source_str, msg_id)
        except Exception as exc:
            logger.exception(f"_on_delete error: {exc}")
    return _on_delete


async def _start_auto_forward(user_id: int, source_channel: str = None) -> bool:
    """
    Route-aware auto-forward launcher.

    • If user has active routes  → register one Telethon handler per unique source.
    • Otherwise (legacy mode)   → register one handler for source_channel and
                                   forward to all destination_channels (old behavior).

    Always falls back gracefully so existing users are unaffected.
    Returns True if at least one handler was registered.
    """
    global auto_forward_handler, auto_forward_task_user, _route_handlers

    client = await get_userbot(user_id)
    if not client:
        logger.error("_start_auto_forward: no live userbot client.")
        return False

    _stop_auto_forward(client)   # clean slate

    routes = db.get_routes(user_id)

    if routes:
        # ── Route-based mode ─────────────────────────────────────
        unique_sources = db.get_unique_active_sources(user_id)
        registered = 0

        for src in unique_sources:
            try:
                src_entity = await client.get_entity(_parse_channel(src))

                # Capture src in closure
                def make_get_dests(s):
                    return lambda: db.get_destinations_for_source(user_id, s)

                msg_handler = _make_message_handler(
                    client, user_id, src, make_get_dests(src)
                )
                edit_handler   = _make_edit_handler(client, user_id, src)
                delete_handler = _make_delete_handler(client, user_id, src)

                client.add_event_handler(
                    msg_handler,
                    telethon_events.NewMessage(chats=src_entity),
                )
                client.add_event_handler(
                    edit_handler,
                    telethon_events.MessageEdited(chats=src_entity),
                )
                client.add_event_handler(
                    delete_handler,
                    telethon_events.MessageDeleted(chats=src_entity),
                )
                _route_handlers[src] = msg_handler
                registered += 1
                logger.info(f"Route handler registered: {src}")
            except Exception as exc:
                logger.error(f"Cannot register route handler for '{src}': {exc}")

        if registered > 0:
            auto_forward_task_user = user_id
            db.set_watch_active(user_id, True)
            logger.info(
                f"Route-based auto-forward STARTED for user {user_id} "
                f"({registered}/{len(unique_sources)} sources active)"
            )
            return True
        return False

    else:
        # ── Legacy mode (no routes) ───────────────────────────────
        legacy_source = source_channel
        if not legacy_source:
            cfg = db.get_watch_config(user_id)
            if cfg:
                legacy_source = cfg.get("source_channel")
        if not legacy_source:
            return False

        try:
            src_entity = await client.get_entity(_parse_channel(legacy_source))
        except Exception as exc:
            logger.error(f"Legacy mode: cannot resolve '{legacy_source}': {exc}")
            return False

        def legacy_get_dests():
            return db.get_destination_channel_ids(user_id)

        handler = _make_message_handler(
            client, user_id, legacy_source, legacy_get_dests
        )
        edit_handler   = _make_edit_handler(client, user_id, legacy_source)
        delete_handler = _make_delete_handler(client, user_id, legacy_source)

        client.add_event_handler(
            handler,
            telethon_events.NewMessage(chats=src_entity),
        )
        client.add_event_handler(
            edit_handler,
            telethon_events.MessageEdited(chats=src_entity),
        )
        client.add_event_handler(
            delete_handler,
            telethon_events.MessageDeleted(chats=src_entity),
        )

        auto_forward_handler   = handler
        auto_forward_task_user = user_id
        db.set_watch_active(user_id, True)

        logger.info(f"Legacy auto-forward STARTED: '{legacy_source}' → user {user_id}'s destinations")
        return True


# ═══════════════════════════════════════════════════════
#  /watch CONVERSATION
# ═══════════════════════════════════════════════════════

async def watch_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Start auto-forwarding. Uses routes if configured, else asks for source."""
    user_id = update.effective_user.id

    if not db.get_session(user_id):
        await update.message.reply_text("❌ Userbot not logged in. Use /login first.")
        return ConversationHandler.END

    routes = db.get_routes(user_id)

    # ── Route mode: start immediately ────────────────────────────
    if routes:
        if not db.get_destination_channel_ids(user_id) and \
                not db.get_unique_active_sources(user_id):
            await update.message.reply_text(
                "❌ No routes configured. Use /add_route to create one first."
            )
            return ConversationHandler.END

        msg = await update.message.reply_text("⏳ Starting route-based forwarding…")
        ok = await _start_auto_forward(user_id)
        if ok:
            sources = db.get_unique_active_sources(user_id)
            await msg.edit_text(
                "✅ *Forwarding started.*\n\n"
                "🔴 Monitoring all configured routes.\n\n"
                f"📡 Sources: `{len(sources)}`\n"
                f"🗺 Routes: `{len(routes)}`\n\n"
                "Use /routes to see all routes.\n"
                "Use /unwatch to stop.",
                parse_mode="Markdown",
            )
        else:
            await msg.edit_text(
                "❌ Failed to start forwarding.\n"
                "Verify that the userbot has access to all source channels."
            )
        return ConversationHandler.END

    # ── Legacy mode: no routes → ask for source channel ──────────
    if not db.get_destination_channel_ids(user_id):
        await update.message.reply_text(
            "❌ No destination channels set.\n"
            "Add one via /settings, or set up routes with /add_route."
        )
        return ConversationHandler.END

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
    await update.message.reply_text(
        "🔴 *Auto-Forward Setup*\n\n"
        "You have no routes configured yet.\n\n"
        "Send the *source channel* to watch for new messages:\n\n"
        "• `@username`\n"
        "• `-1001234567890`\n"
        "• `https://t.me/channelname`\n\n"
        "💡 _Tip: Use /add_route to set up named route mappings._",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    return WAITING_WATCH_SOURCE


async def receive_watch_source(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Legacy mode: validate source channel and start watching."""
    user_id = update.effective_user.id
    raw     = (update.message.text or "").strip()

    if not validate_channel_format(raw):
        await update.message.reply_text(
            "❌ Invalid channel format.\n\n"
            "Accepted formats:\n"
            "• `@username`\n"
            "• `-1001234567890`\n"
            "• `https://t.me/channelname`",
            parse_mode="Markdown",
        )
        return WAITING_WATCH_SOURCE

    source = normalise_channel(raw)
    msg    = await update.message.reply_text(f"⏳ Connecting to `{source}`…", parse_mode="Markdown")

    db.save_watch_config(user_id, source)

    ok = await _start_auto_forward(user_id, source)
    if not ok:
        db.set_watch_active(user_id, False)
        await msg.edit_text(
            f"❌ *Cannot access* `{source}`\n\n"
            "Make sure the userbot has joined the channel, then try again.",
            parse_mode="Markdown",
        )
        return ConversationHandler.END

    await msg.edit_text(
        "✅ *Forwarding started.*\n\n"
        f"🔴 Watching: `{source}`\n"
        f"📤 Destinations: {len(db.get_destination_channel_ids(user_id))} channel(s)\n\n"
        "Every new message will be instantly forwarded.\n"
        "Use /unwatch to stop.",
        parse_mode="Markdown",
    )
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════
#  /unwatch COMMAND
# ═══════════════════════════════════════════════════════

async def unwatch_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Stop auto-forwarding. Routes and settings remain saved."""
    user_id = update.effective_user.id
    global userbot_client

    routes = db.get_routes(user_id)
    watch_cfg = db.get_watch_config(user_id)

    is_active = (
        auto_forward_task_user == user_id
        and (bool(_route_handlers) or auto_forward_handler is not None)
    )

    if not is_active:
        await update.message.reply_text(
            "⏹ Forwarding is not currently active.\n\n"
            "Use /watch to start."
        )
        return

    _stop_auto_forward(userbot_client)
    db.set_watch_active(user_id, False)

    if routes:
        source_info = f"{len(routes)} route(s)"
    else:
        source_info = (watch_cfg or {}).get("source_channel", "unknown")

    await update.message.reply_text(
        "⏹ *Forwarding stopped.*\n\n"
        f"Was watching: `{source_info}`\n\n"
        "Your routes and settings remain saved.\n"
        "Use /watch to start again.",
        parse_mode="Markdown",
    )


# ═══════════════════════════════════════════════════════
#  CORE COPY LOOP (for /forward)
# ═══════════════════════════════════════════════════════

async def _copy_range_loop(
    app,
    client: TelegramClient,
    source_str: str,
    destinations: List[str],
    first_id: int,
    last_id: Optional[int],
    progress_msg,
    session_id: int,
    user_id: int,
):
    forwarded = 0
    failed    = 0
    batch_size = 50
    messages_in_minute = 0
    minute_start_time = asyncio.get_event_loop().time()
    last_send_time = 0

    async def _edit_progress(text: str, done: bool = False):
        kb = None if done else InlineKeyboardMarkup([
            [InlineKeyboardButton("🛑 Stop", callback_data=f"stop_forwarding_{session_id}")]
        ])
        try:
            await app.bot.edit_message_text(
                chat_id=progress_msg.chat_id,
                message_id=progress_msg.message_id,
                text=text,
                parse_mode="Markdown",
                reply_markup=kb,
            )
        except Exception:
            pass

    try:
        try:
            source_entity = await client.get_entity(_parse_channel(source_str))
        except Exception as exc:
            await _edit_progress(
                f"❌ *Cannot resolve source channel* `{source_str}`.\n\n"
                f"Error: `{type(exc).__name__}`\n\n"
                "Make sure the userbot has joined the source channel.",
                done=True,
            )
            return

        if last_id is None:
            async for m in client.iter_messages(source_entity, limit=1):
                last_id = m.id
                break
            if last_id is None:
                await _edit_progress("❌ Source channel appears empty.", done=True)
                return

        if last_id < first_id:
            await _edit_progress(
                f"❌ Last ID ({last_id}) is less than first ({first_id}).", done=True
            )
            return

        dest_entities: Dict[str, Any] = {}
        for dest in destinations:
            try:
                dest_entities[dest] = await client.get_entity(_parse_channel(dest))
            except Exception as exc:
                logger.warning(f"Cannot resolve dest {dest}: {type(exc).__name__}")

        if not dest_entities:
            await _edit_progress(
                "❌ No destination channels could be resolved.\n\n"
                "Make sure the userbot is a member/admin in each destination.",
                done=True,
            )
            return

        total_range = last_id - first_id + 1
        await _edit_progress(
            f"📥 *Collecting messages…*\n\n"
            f"📡 Source: `{source_str}`\n"
            f"🔢 IDs: `{first_id}` → `{last_id}` (~{total_range} slots)\n"
            f"📤 Destinations: {len(dest_entities)}\n\n"
            "⏱️ Rate: 1 msg/sec, 30/min then 30sec break"
        )

        current_id = first_id
        processed  = 0
        last_edit_time = 0
        total_sent = 0

        while current_id <= last_id:
            stats = db.get_forwarding_stats(session_id)
            if stats and not stats.get("is_active"):
                break

            batch = []
            async for msg in client.iter_messages(
                source_entity,
                min_id=current_id - 1,
                max_id=last_id,
                reverse=True,
                limit=batch_size,
            ):
                batch.append(msg)
                current_id = msg.id + 1

            if not batch:
                break

            # Group albums
            groups: List[List] = []
            i = 0
            while i < len(batch):
                msg = batch[i]
                if msg.grouped_id:
                    album = [msg]
                    j = i + 1
                    while j < len(batch) and batch[j].grouped_id == msg.grouped_id:
                        album.append(batch[j])
                        j += 1
                    groups.append(album)
                    i = j
                else:
                    groups.append([msg])
                    i += 1

            for group in groups:
                stats = db.get_forwarding_stats(session_id)
                if stats and not stats.get("is_active"):
                    break

                current_time = asyncio.get_event_loop().time()

                if current_time - minute_start_time >= 60:
                    messages_in_minute = 0
                    minute_start_time = current_time

                if messages_in_minute >= 30:
                    await _edit_progress(
                        f"📤 *Rate limiting…*\n\n"
                        f"✅ Copied:   `{forwarded}`\n"
                        f"❌ Failed:   `{failed}`\n"
                        f"📊 Progress: `{processed} / ~{total_range}`\n\n"
                        f"⏸️ Taking 30-second break…"
                    )
                    await asyncio.sleep(30)
                    messages_in_minute = 0
                    minute_start_time = asyncio.get_event_loop().time()
                    continue

                time_since_last = current_time - last_send_time
                if time_since_last < 1.0:
                    await asyncio.sleep(1.0 - time_since_last)

                is_album = len(group) > 1
                for dest_key, dest_entity in dest_entities.items():
                    ok = await _safe_send(
                        client, dest_entity,
                        msg=group[0] if not is_album else None,
                        album=group if is_album else None,
                    )
                    if ok:
                        forwarded += 1
                    else:
                        failed += 1

                total_sent += len(group)
                messages_in_minute += 1
                processed += len(group)
                last_send_time = asyncio.get_event_loop().time()
                db.update_forwarding_stats(session_id, forwarded, failed)

                now = asyncio.get_event_loop().time()
                if now - last_edit_time >= 3 or processed % 5 == 0:
                    last_edit_time = now
                    await _edit_progress(
                        f"📤 *Copying…*\n\n"
                        f"✅ Copied:   `{forwarded}`\n"
                        f"❌ Failed:   `{failed}`\n"
                        f"📊 Progress: `{processed} / ~{total_range}`\n\n"
                        f"⏱️ This minute: `{messages_in_minute} / 30`"
                    )

        db.end_forwarding_session(session_id)
        await _edit_progress(
            f"✅ *Copy job complete!*\n\n"
            f"📡 Source: `{source_str}`\n"
            f"📊 Copied:  `{forwarded}`\n"
            f"❌ Failed:  `{failed}`\n"
            f"📤 Destinations: {len(dest_entities)}",
            done=True,
        )

    except asyncio.CancelledError:
        db.end_forwarding_session(session_id)
        await _edit_progress(
            f"🛑 *Copy job stopped.*\n\n"
            f"✅ Copied so far: `{forwarded}`\n"
            f"❌ Failed:        `{failed}`",
            done=True,
        )
    except Exception as exc:
        logger.exception(f"_copy_range_loop crashed: {exc}")
        db.end_forwarding_session(session_id)
        await _edit_progress(
            f"💥 *Unexpected error:* `{type(exc).__name__}`\n\n"
            f"✅ Copied so far: `{forwarded}`\n"
            f"❌ Failed:        `{failed}`",
            done=True,
        )


# ═══════════════════════════════════════════════════════
#  CALLBACK QUERY ROUTER
# ═══════════════════════════════════════════════════════

async def button_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    user_id = update.effective_user.id
    await query.answer()
    d = query.data

    if d == "back_main":
        has_session = db.get_session(user_id) is not None
        status = "✅ Userbot connected" if has_session else "⚠️ Userbot not logged in"
        await query.edit_message_text(
            f"👋 *Welcome back!*\n\n*Status:* {status}\n\nChoose an option:",
            parse_mode="Markdown",
            reply_markup=main_menu_keyboard(),
        )

    elif d == "help":
        await help_command(update, ctx)

    elif d == "settings":
        await settings_command(update, ctx)

    elif d == "login":
        return await login_command(update, ctx)

    elif d == "add_dest":
        return await ask_dest(update, ctx)

    elif d == "manage_dests":
        dests = db.get_destination_channels(user_id)
        if not dests:
            await query.edit_message_text(
                "No destination channels set.",
                reply_markup=settings_keyboard(user_id),
            )
            return
        await query.edit_message_text(
            "🗑 *Manage Destinations*\n\nTap a channel to remove it:",
            parse_mode="Markdown",
            reply_markup=manage_dests_keyboard(user_id),
        )

    elif d.startswith("remove_dest_"):
        idx = int(d.split("_")[-1])
        db.remove_destination_channel_by_index(user_id, idx)
        await query.edit_message_text(
            "✅ Channel removed.",
            reply_markup=settings_keyboard(user_id),
        )

    elif d.startswith("stop_forwarding_"):
        session_id = int(d.split("_")[-1])
        db.end_forwarding_session(session_id)
        global forward_task
        if forward_task and not forward_task.done():
            forward_task.cancel()
        await query.edit_message_text("🛑 *Copy job cancelled.*", parse_mode="Markdown")

    elif d == "cancel_conv":
        await query.edit_message_text("❌ Cancelled.", reply_markup=main_menu_keyboard())
        return ConversationHandler.END

    elif d == "bulk_save":
        # Handled inside save_conv — this fallback fires if somehow triggered outside
        return await bulk_save_start(update, ctx)

    elif d == "confirm_reset_config":
        user_id = query.from_user.id
        db.reset_config(user_id)
        await query.edit_message_text(
            "✅ *Configuration reset to defaults.*\n\n"
            "Routes, sources, destinations, filters, blacklist, and whitelist are unchanged.\n"
            "Use /config to review your current settings.",
            parse_mode="Markdown",
        )

    elif d == "confirm_delete_all":
        user_id = query.from_user.id
        global userbot_client
        _stop_auto_forward(userbot_client)
        if userbot_client:
            try:
                await userbot_client.disconnect()
            except Exception:
                pass
            userbot_client = None
        db.delete_all_user_data(user_id)
        await query.edit_message_text(
            "✅ *All your data has been deleted.*\n\n"
            "Session, routes, settings — everything has been removed.\n"
            "Use /start to begin fresh.",
            parse_mode="Markdown",
        )




# ═══════════════════════════════════════════════════════
#  /incoming — Add Source Channel
# ═══════════════════════════════════════════════════════

async def incoming_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Start flow to add a source channel."""
    user_id = update.effective_user.id
    db.add_user(user_id, update.effective_user.first_name,
                update.effective_user.last_name or "")

    sources = db.get_source_channels(user_id)
    lines   = "\n".join(f"• `{s['channel_id']}`" for s in sources) if sources else "_None added yet_"

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
    await update.message.reply_text(
        "📥 *Incoming (Source Channels)*\n\n"
        f"*Current sources:*\n{lines}\n\n"
        "Send the source channel you want to monitor:\n\n"
        "• `@username`\n"
        "• `-1001234567890`\n"
        "• `https://t.me/channelname`",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    return WAITING_INCOMING


async def receive_incoming(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    raw     = (update.message.text or "").strip()

    if not validate_channel_format(raw):
        await update.message.reply_text(
            "❌ Invalid format. Send `@username`, `-100ID`, or a `t.me/` link.",
            parse_mode="Markdown",
        )
        return WAITING_INCOMING

    channel = normalise_channel(raw)
    sources = db.get_source_channel_ids(user_id)

    if channel in sources:
        await update.message.reply_text(
            f"⚠️ `{channel}` is already in your source list.",
            parse_mode="Markdown",
        )
        return ConversationHandler.END

    db.add_source_channel(user_id, channel)
    total = len(db.get_source_channels(user_id))
    await update.message.reply_text(
        f"✅ *Source added:* `{channel}`\n\n"
        f"Total sources: {total}\n\n"
        "Now use /add_route to map this source to a destination.",
        parse_mode="Markdown",
    )
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════
#  /outgoing — Add Destination Channel (route system)
# ═══════════════════════════════════════════════════════

async def outgoing_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Add a destination channel for the route system."""
    user_id = update.effective_user.id
    db.add_user(user_id, update.effective_user.first_name,
                update.effective_user.last_name or "")

    dests = db.get_destination_channels(user_id)
    lines = "\n".join(f"• `{d['channel_id']}`" for d in dests) if dests else "_None added yet_"

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
    await update.message.reply_text(
        "📤 *Outgoing (Destination Channels)*\n\n"
        f"*Current destinations:*\n{lines}\n\n"
        "Send the destination channel where messages should be forwarded:\n\n"
        "• `@username`\n"
        "• `-1001234567890`\n"
        "• `https://t.me/channelname`",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    return WAITING_OUTGOING


async def receive_outgoing(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    raw     = (update.message.text or "").strip()

    if not validate_channel_format(raw):
        await update.message.reply_text(
            "❌ Invalid format. Send `@username`, `-100ID`, or a `t.me/` link.",
            parse_mode="Markdown",
        )
        return WAITING_OUTGOING

    channel = normalise_channel(raw)
    existing = db.get_destination_channel_ids(user_id)

    if channel in existing:
        await update.message.reply_text(
            f"⚠️ `{channel}` is already in your destinations.",
            parse_mode="Markdown",
        )
        return ConversationHandler.END

    db.add_user(user_id, update.effective_user.first_name,
                update.effective_user.last_name or "")
    db.add_destination_channel(user_id, channel)
    total = len(db.get_destination_channels(user_id))
    await update.message.reply_text(
        f"✅ *Destination added:* `{channel}`\n\n"
        f"Total destinations: {total}\n\n"
        "Use /add_route to map a source to this destination.",
        parse_mode="Markdown",
    )
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════
#  /routes — List All Routes
# ═══════════════════════════════════════════════════════

async def routes_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    routes  = db.get_routes(user_id)

    if not routes:
        await update.message.reply_text(
            "🗺 *Configured Routes*\n\n"
            "_No routes configured yet._\n\n"
            "Use /add_route to create one.",
            parse_mode="Markdown",
        )
        return

    lines = "\n".join(
        f"`{i + 1}.` `{r['source']}` → `{r['destination']}`"
        for i, r in enumerate(routes)
    )
    await update.message.reply_text(
        f"🗺 *Configured Routes* ({len(routes)})\n\n"
        f"{lines}\n\n"
        "Use /add_route to add • /remove_route to delete • /watch to start",
        parse_mode="Markdown",
    )


# ═══════════════════════════════════════════════════════
#  /add_route — Create a Route (2-step conversation)
# ═══════════════════════════════════════════════════════

async def add_route_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    db.add_user(user_id, update.effective_user.first_name,
                update.effective_user.last_name or "")

    sources = db.get_source_channels(user_id)
    dests   = db.get_destination_channels(user_id)

    if not sources and not dests:
        await update.message.reply_text(
            "⚠️ *No sources or destinations found.*\n\n"
            "Run /incoming to add a source channel first,\n"
            "then /outgoing to add a destination.",
            parse_mode="Markdown",
        )

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
    await update.message.reply_text(
        "➕ *Add Route — Step 1/2*\n\n"
        "Send the *source channel* (messages will be read from here):\n\n"
        "• `@username`\n"
        "• `-1001234567890`\n"
        "• `https://t.me/channelname`",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    return WAITING_ADD_ROUTE_SRC


async def receive_add_route_src(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    raw = (update.message.text or "").strip()
    if not validate_channel_format(raw):
        await update.message.reply_text(
            "❌ Invalid format. Send `@username`, `-100ID`, or `t.me/` link.",
            parse_mode="Markdown",
        )
        return WAITING_ADD_ROUTE_SRC

    ctx.user_data["route_src"] = normalise_channel(raw)
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
    await update.message.reply_text(
        "➕ *Add Route — Step 2/2*\n\n"
        f"Source: `{ctx.user_data['route_src']}`\n\n"
        "Now send the *destination channel* (messages will be sent here):\n\n"
        "• `@username`\n"
        "• `-1001234567890`\n"
        "• `https://t.me/channelname`",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    return WAITING_ADD_ROUTE_DST


async def receive_add_route_dst(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    raw     = (update.message.text or "").strip()

    if not validate_channel_format(raw):
        await update.message.reply_text(
            "❌ Invalid format. Send `@username`, `-100ID`, or `t.me/` link.",
            parse_mode="Markdown",
        )
        return WAITING_ADD_ROUTE_DST

    src = ctx.user_data.get("route_src", "")
    dst = normalise_channel(raw)

    if src == dst:
        await update.message.reply_text("❌ Source and destination cannot be the same channel.")
        return WAITING_ADD_ROUTE_DST

    ok = db.add_route(user_id, src, dst)
    if not ok:
        await update.message.reply_text(
            f"⚠️ Route `{src}` → `{dst}` already exists.",
            parse_mode="Markdown",
        )
        return ConversationHandler.END

    # Auto-register source and destination
    db.add_source_channel(user_id, src)
    db.add_user(user_id, update.effective_user.first_name,
                update.effective_user.last_name or "")
    db.add_destination_channel(user_id, dst)

    routes = db.get_routes(user_id)
    await update.message.reply_text(
        f"✅ *Route created!*\n\n"
        f"`{src}` → `{dst}`\n\n"
        f"Total routes: {len(routes)}\n\n"
        "Run /watch to start forwarding.",
        parse_mode="Markdown",
    )
    ctx.user_data.pop("route_src", None)
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════
#  /remove_route — Delete a Route
# ═══════════════════════════════════════════════════════

async def remove_route_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    routes  = db.get_routes(user_id)

    if not routes:
        await update.message.reply_text("❌ No routes to remove. Use /add_route first.")
        return ConversationHandler.END

    lines = "\n".join(
        f"`{i + 1}.` `{r['source']}` → `{r['destination']}`"
        for i, r in enumerate(routes)
    )
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
    await update.message.reply_text(
        f"🗑 *Remove Route*\n\n{lines}\n\n"
        "Send the *number* of the route to remove:",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    return WAITING_REMOVE_ROUTE


async def receive_remove_route(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    raw     = (update.message.text or "").strip()
    try:
        idx = int(raw) - 1
    except ValueError:
        await update.message.reply_text("❌ Please send a valid number.")
        return WAITING_REMOVE_ROUTE

    ok = db.remove_route_by_index(user_id, idx)
    if ok:
        remaining = db.get_routes(user_id)
        await update.message.reply_text(
            f"✅ Route removed. Remaining routes: {len(remaining)}",
        )
    else:
        await update.message.reply_text("❌ Invalid number. Use /remove_route to try again.")
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════
#  /filter — Add Text Replacement Filter
# ═══════════════════════════════════════════════════════

async def filter_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    filters = db.get_filters(user_id)
    lines   = "\n".join(
        f"`{i+1}.` `{f['pattern']}` → `{f['replacement']}`"
        + (" _(regex)_" if f.get("is_regex") else "")
        for i, f in enumerate(filters)
    ) if filters else "_No filters yet_"

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
    await update.message.reply_text(
        "🔤 *Text Replacement Filters*\n\n"
        f"*Current filters:*\n{lines}\n\n"
        "Send a replacement rule in one of these formats:\n\n"
        "Plain: `old text ==> new text`\n"
        "Regex: `re:pattern ==> replacement`\n\n"
        "_Supports [mono]text[/mono] formatting._",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    return WAITING_FILTER


async def receive_filter(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    raw     = (update.message.text or "").strip()

    if "==>" not in raw:
        await update.message.reply_text(
            "❌ Invalid format. Use: `old ==> new` or `re:pattern ==> replacement`",
            parse_mode="Markdown",
        )
        return WAITING_FILTER

    parts       = raw.split("==>", 1)
    pattern_raw = parts[0].strip()
    replacement = parts[1].strip()
    is_regex    = False

    if pattern_raw.startswith("re:"):
        pattern_raw = pattern_raw[3:].strip()
        is_regex    = True
        try:
            re.compile(pattern_raw)
        except re.error as exc:
            await update.message.reply_text(f"❌ Invalid regex: `{exc}`", parse_mode="Markdown")
            return WAITING_FILTER

    db.add_filter(user_id, pattern_raw, replacement, is_regex)
    await update.message.reply_text(
        f"✅ Filter added:\n`{pattern_raw}` → `{replacement}`"
        + (" _(regex)_" if is_regex else ""),
        parse_mode="Markdown",
    )
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════
#  /remove_filter — Remove a Filter
# ═══════════════════════════════════════════════════════

async def remove_filter_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    filters = db.get_filters(user_id)

    if not filters:
        await update.message.reply_text("❌ No filters to remove.")
        return ConversationHandler.END

    lines = "\n".join(
        f"`{i+1}.` `{f['pattern']}` → `{f['replacement']}`"
        for i, f in enumerate(filters)
    )
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
    await update.message.reply_text(
        f"🗑 *Remove Filter*\n\n{lines}\n\nSend the *number* to remove:",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    return WAITING_REMOVE_FILTER


async def receive_remove_filter(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
        idx = int((update.message.text or "").strip()) - 1
    except ValueError:
        await update.message.reply_text("❌ Please send a valid number.")
        return WAITING_REMOVE_FILTER

    ok = db.remove_filter_by_index(user_id, idx)
    await update.message.reply_text(
        "✅ Filter removed." if ok else "❌ Invalid number."
    )
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════
#  /blacklist — Manage Blacklist
# ═══════════════════════════════════════════════════════

async def blacklist_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    items   = db.get_blacklist(user_id)
    lines   = "\n".join(f"• `{kw}`" for kw in items) if items else "_Empty_"

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
    await update.message.reply_text(
        "🚫 *Blacklist*\n\n"
        f"*Current keywords:*\n{lines}\n\n"
        "Send keyword(s) to block (one per line or comma-separated).\n"
        "Messages containing any blacklist keyword will be *dropped*.",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    return WAITING_BLACKLIST


async def receive_blacklist(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    raw     = (update.message.text or "").strip()
    keywords = [k.strip() for k in re.split(r"[,\n]+", raw) if k.strip()]

    added = 0
    for kw in keywords:
        if db.add_blacklist(user_id, kw):
            added += 1

    total = len(db.get_blacklist(user_id))
    await update.message.reply_text(
        f"✅ Added {added} keyword(s) to blacklist.\n"
        f"Total blacklist entries: {total}",
    )
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════
#  /rem_blacklist — Remove Blacklist Entry
# ═══════════════════════════════════════════════════════

async def rem_blacklist_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    items   = db.get_blacklist(user_id)

    if not items:
        await update.message.reply_text("❌ Blacklist is empty.")
        return ConversationHandler.END

    lines = "\n".join(f"`{i+1}.` `{kw}`" for i, kw in enumerate(items))
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
    await update.message.reply_text(
        f"🗑 *Remove Blacklist Entry*\n\n{lines}\n\n"
        "Send the *number* to remove, or send the keyword directly:",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    return WAITING_REM_BLACKLIST


async def receive_rem_blacklist(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    raw     = (update.message.text or "").strip()

    # Try as number first
    try:
        idx = int(raw) - 1
        ok  = db.remove_blacklist_by_index(user_id, idx)
    except ValueError:
        ok = db.remove_blacklist(user_id, raw)

    await update.message.reply_text(
        f"✅ Removed `{raw}` from blacklist." if ok else "❌ Entry not found.",
        parse_mode="Markdown",
    )
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════
#  /whitelist — Manage Whitelist
# ═══════════════════════════════════════════════════════

async def whitelist_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    items   = db.get_whitelist(user_id)
    lines   = "\n".join(f"• `{kw}`" for kw in items) if items else "_Empty_"

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
    await update.message.reply_text(
        "✅ *Whitelist*\n\n"
        f"*Current keywords:*\n{lines}\n\n"
        "Send keyword(s) to whitelist (one per line or comma-separated).\n"
        "When non-empty, *only* messages containing a whitelist keyword are forwarded.",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    return WAITING_WHITELIST


async def receive_whitelist(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id  = update.effective_user.id
    raw      = (update.message.text or "").strip()
    keywords = [k.strip() for k in re.split(r"[,\n]+", raw) if k.strip()]

    added = 0
    for kw in keywords:
        if db.add_whitelist(user_id, kw):
            added += 1

    total = len(db.get_whitelist(user_id))
    await update.message.reply_text(
        f"✅ Added {added} keyword(s) to whitelist.\n"
        f"Total whitelist entries: {total}",
    )
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════
#  /rem_whitelist — Remove Whitelist Entry
# ═══════════════════════════════════════════════════════

async def rem_whitelist_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    items   = db.get_whitelist(user_id)

    if not items:
        await update.message.reply_text("❌ Whitelist is empty.")
        return ConversationHandler.END

    lines = "\n".join(f"`{i+1}.` `{kw}`" for i, kw in enumerate(items))
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
    await update.message.reply_text(
        f"🗑 *Remove Whitelist Entry*\n\n{lines}\n\n"
        "Send the *number* to remove, or send the keyword directly:",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    return WAITING_REM_WHITELIST


async def receive_rem_whitelist(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    raw     = (update.message.text or "").strip()
    try:
        idx = int(raw) - 1
        ok  = db.remove_whitelist_by_index(user_id, idx)
    except ValueError:
        ok = db.remove_whitelist(user_id, raw)

    await update.message.reply_text(
        f"✅ Removed `{raw}` from whitelist." if ok else "❌ Entry not found.",
        parse_mode="Markdown",
    )
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════
#  /delay — Set Forwarding Delay
# ═══════════════════════════════════════════════════════

async def delay_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    cfg     = db.get_config(user_id)
    current = cfg.get("delay", 0)

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
    await update.message.reply_text(
        f"⏱️ *Forwarding Delay*\n\n"
        f"Current delay: `{current}` seconds\n\n"
        "Send the delay in seconds (0 = no delay, max 3600):",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    return WAITING_DELAY


async def receive_delay(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    raw     = (update.message.text or "").strip()
    try:
        delay = int(raw)
        if delay < 0 or delay > 3600:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Please send a number between 0 and 3600.")
        return WAITING_DELAY

    db.set_config(user_id, delay=delay)
    await update.message.reply_text(
        f"✅ Delay set to `{delay}` second(s).",
        parse_mode="Markdown",
    )
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════
#  /begin_text — Set Prefix Text
# ═══════════════════════════════════════════════════════

async def begin_text_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    cfg     = db.get_config(user_id)
    current = cfg.get("begin_text", "") or "_None_"

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
    await update.message.reply_text(
        f"📝 *Begin Text (Prefix)*\n\n"
        f"Current: {current}\n\n"
        "Send the text to prepend before every forwarded message.\n"
        "Supports [user.first_name], [user.username], [user.id] placeholders.\n\n"
        "Send `-` to clear the current begin text.",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    return WAITING_BEGIN_TEXT


async def receive_begin_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    raw     = (update.message.text or "").strip()

    if raw == "-":
        db.set_config(user_id, begin_text="")
        await update.message.reply_text("✅ Begin text cleared.")
    else:
        db.set_config(user_id, begin_text=raw)
        await update.message.reply_text(
            f"✅ Begin text set to:\n`{raw}`",
            parse_mode="Markdown",
        )
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════
#  /end_text — Set Suffix Text
# ═══════════════════════════════════════════════════════

async def end_text_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    cfg     = db.get_config(user_id)
    current = cfg.get("end_text", "") or "_None_"

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
    await update.message.reply_text(
        f"📝 *End Text (Suffix)*\n\n"
        f"Current: {current}\n\n"
        "Send the text to append after every forwarded message.\n"
        "Supports [user.first_name], [user.username], [user.id] placeholders.\n\n"
        "Send `-` to clear the current end text.",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    return WAITING_END_TEXT


async def receive_end_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    raw     = (update.message.text or "").strip()

    if raw == "-":
        db.set_config(user_id, end_text="")
        await update.message.reply_text("✅ End text cleared.")
    else:
        db.set_config(user_id, end_text=raw)
        await update.message.reply_text(
            f"✅ End text set to:\n`{raw}`",
            parse_mode="Markdown",
        )
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════
#  /filter_users — Filter by Sender User IDs
# ═══════════════════════════════════════════════════════

async def filter_users_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ufilters = db.get_user_filters(user_id)
    lines    = "\n".join(f"• `{uid}`" for uid in ufilters) if ufilters else "_Not set (all users)_"

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
    await update.message.reply_text(
        "👤 *User Filters*\n\n"
        f"*Allowed sender IDs:*\n{lines}\n\n"
        "Send user ID(s) to whitelist (space or comma-separated).\n"
        "When set, only messages from these users are forwarded.\n\n"
        "Send `-` to clear all user filters (allow everyone).",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    return WAITING_FILTER_USERS


async def receive_filter_users(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    raw     = (update.message.text or "").strip()

    if raw == "-":
        db.clear_user_filters(user_id)
        await update.message.reply_text("✅ User filters cleared. All senders are now allowed.")
        return ConversationHandler.END

    parts = re.split(r"[\s,]+", raw)
    added = 0
    bad   = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        try:
            uid = int(p)
            db.add_user_filter(user_id, uid)
            added += 1
        except ValueError:
            bad.append(p)

    msg = f"✅ Added {added} user ID(s) to filter."
    if bad:
        msg += f"\n⚠️ Could not parse: `{'`, `'.join(bad)}`"
    await update.message.reply_text(msg, parse_mode="Markdown")
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════
#  /config — View Full Configuration
# ═══════════════════════════════════════════════════════

async def config_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # Sources
    sources = db.get_source_channels(user_id)
    src_lines = "\n".join(f"  • `{s['channel_id']}`" for s in sources) if sources else "  _None_"

    # Destinations
    dests = db.get_destination_channels(user_id)
    dst_lines = "\n".join(f"  • `{d['channel_id']}`" for d in dests) if dests else "  _None_"

    # Routes
    routes = db.get_routes(user_id)
    rte_lines = "\n".join(
        f"  `{i+1}.` `{r['source']}` → `{r['destination']}`"
        for i, r in enumerate(routes)
    ) if routes else "  _None_"

    # Config
    cfg = db.get_config(user_id)

    # Filters
    filters_list = db.get_filters(user_id)
    flt_lines = ", ".join(f"`{f['pattern']} → {f['replacement']}`" for f in filters_list) or "_[]_"

    # Blacklist / Whitelist / User filters
    bl  = db.get_blacklist(user_id)
    wl  = db.get_whitelist(user_id)
    uf  = db.get_user_filters(user_id)

    bl_str = ", ".join(f"`{k}`" for k in bl) if bl else "_[]_"
    wl_str = ", ".join(f"`{k}`" for k in wl) if wl else "_[]_"
    uf_str = ", ".join(f"`{u}`" for u in uf) if uf else "_[]_"

    # Watch status
    is_watching = (
        auto_forward_task_user == user_id
        and (bool(_route_handlers) or auto_forward_handler is not None)
    )

    def yn(val):
        return "Enabled" if val else "Disabled"

    text = (
        "⚙️ *Current Configuration*\n\n"
        f"📥 *Sources:*\n{src_lines}\n\n"
        f"📤 *Destinations:*\n{dst_lines}\n\n"
        f"🗺 *Routes:*\n{rte_lines}\n\n"
        f"⏱️ *Delay:* `{cfg['delay']}` seconds\n"
        f"📝 *Begin Text:* `{cfg['begin_text'] or 'None'}`\n"
        f"📝 *End Text:* `{cfg['end_text'] or 'None'}`\n"
        f"📋 *Copy Mode:* `{'Copy (no tag)' if cfg['copy_mode'] else 'Forward (with tag)'}`\n"
        f"🖼 *Media Forwarding:* {yn(cfg['media_enabled'])}\n"
        f"💬 *Text Forwarding:* {yn(cfg['text_enabled'])}\n"
        f"🔗 *URL Preview:* {yn(cfg['url_preview'])}\n"
        f"✏️ *Edit Sync:* {yn(cfg['edit_sync'])}\n"
        f"🗑 *Delete Sync:* {yn(cfg['delete_sync'])}\n\n"
        f"🔤 *Text Filters:* {flt_lines}\n"
        f"🚫 *Blacklist:* {bl_str}\n"
        f"✅ *Whitelist:* {wl_str}\n"
        f"👤 *User Filters:* {uf_str}\n\n"
        f"🔴 *Forwarding Status:* `{'RUNNING' if is_watching else 'STOPPED'}`"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


# ═══════════════════════════════════════════════════════
#  /reset_config — Reset All Settings to Defaults
# ═══════════════════════════════════════════════════════

async def reset_config_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Yes, reset", callback_data="confirm_reset_config"),
        InlineKeyboardButton("❌ Cancel",     callback_data="cancel_conv"),
    ]])
    await update.message.reply_text(
        "⚠️ *Reset Configuration*\n\n"
        "This will reset delay, begin/end text, copy mode, media/text toggle, "
        "URL preview, edit/delete sync to their defaults.\n\n"
        "*Routes, sources, destinations, filters, blacklist, and whitelist will NOT be affected.*\n\n"
        "Are you sure?",
        parse_mode="Markdown",
        reply_markup=kb,
    )


# ═══════════════════════════════════════════════════════
#  /remove_session — Remove the Stored Session
# ═══════════════════════════════════════════════════════

async def remove_session_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    global userbot_client

    session = db.get_session(user_id)
    if not session:
        await update.message.reply_text("❌ No session stored.")
        return

    # Disconnect live client
    _stop_auto_forward(userbot_client)
    if userbot_client:
        try:
            await userbot_client.disconnect()
        except Exception:
            pass
        userbot_client = None

    db.delete_session(user_id)
    db.set_watch_active(user_id, False)

    await update.message.reply_text(
        "✅ *Session removed.*\n\n"
        "Userbot disconnected. Use /login to connect a new session.",
        parse_mode="Markdown",
    )


# ═══════════════════════════════════════════════════════
#  /delete — Delete All User Data
# ═══════════════════════════════════════════════════════

async def delete_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🗑 Yes, delete everything", callback_data="confirm_delete_all"),
        InlineKeyboardButton("❌ Cancel",                 callback_data="cancel_conv"),
    ]])
    await update.message.reply_text(
        "☢️ *Delete All My Data*\n\n"
        "This will permanently delete:\n"
        "• Your session\n"
        "• All sources & destinations\n"
        "• All routes\n"
        "• All filters, blacklist, whitelist\n"
        "• All configuration\n\n"
        "⚠️ *This cannot be undone.*",
        parse_mode="Markdown",
        reply_markup=kb,
    )


# ═══════════════════════════════════════════════════════
#  TOGGLE SETTINGS  (copy/forward mode, media, text, url preview, edit/delete sync)
# ═══════════════════════════════════════════════════════

async def toggle_config_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    Generic toggle handler. Usage: /copy_mode, /toggle_media, /toggle_text,
    /toggle_url_preview, /toggle_edit_sync, /toggle_delete_sync
    """
    user_id = update.effective_user.id
    cmd     = (update.message.text or "").lstrip("/").split()[0].lower()

    toggle_map = {
        "copy_mode":         ("copy_mode",      "Copy mode",          "Copy (no tag)",   "Forward (with tag)"),
        "toggle_media":      ("media_enabled",   "Media forwarding",   "Enabled",         "Disabled"),
        "toggle_text":       ("text_enabled",    "Text forwarding",    "Enabled",         "Disabled"),
        "toggle_url_preview":("url_preview",     "URL preview",        "Enabled",         "Disabled"),
        "toggle_edit_sync":  ("edit_sync",       "Edit sync",          "Enabled",         "Disabled"),
        "toggle_delete_sync":("delete_sync",     "Delete sync",        "Enabled",         "Disabled"),
    }

    if cmd not in toggle_map:
        await update.message.reply_text("❌ Unknown toggle command.")
        return

    key, label, on_label, off_label = toggle_map[cmd]
    cfg     = db.get_config(user_id)
    new_val = not cfg.get(key, True)
    db.set_config(user_id, **{key: new_val})
    state = on_label if new_val else off_label
    await update.message.reply_text(
        f"✅ *{label}:* {state}",
        parse_mode="Markdown",
    )


# ═══════════════════════════════════════════════════════
#  HEALTH CHECK SERVER  (required for Koyeb Web Service)
# ═══════════════════════════════════════════════════════

class _HealthHandler(BaseHTTPRequestHandler):
    """
    Minimal HTTP handler that satisfies Koyeb's health-check probe.
    Responds 200 OK to any GET request — keeps the free Web Service alive.
    """
    def do_GET(self):
        body = b"OK"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # Silence HTTP access log lines so they don't clutter bot logs
    def log_message(self, fmt, *args):
        pass


def _start_health_server():
    """
    Run a tiny HTTP server in a background daemon thread.
    Koyeb's free Web Service tier requires an HTTP listener on $PORT.
    Using a daemon thread means it exits automatically when the main
    process exits — no cleanup needed.
    """
    port = int(os.getenv("PORT", "8000"))
    server = HTTPServer(("0.0.0.0", port), _HealthHandler)
    logger.info(f"Health-check server listening on port {port}")
    server.serve_forever()


# ═══════════════════════════════════════════════════════
#  APPLICATION SETUP
# ═══════════════════════════════════════════════════════

def main():
    app = Application.builder().token(BOT_TOKEN).build()

    login_conv = ConversationHandler(
        entry_points=[
            CommandHandler("login", login_command),
            CallbackQueryHandler(login_command, pattern="^login$"),
        ],
        states={
            WAITING_SESSION:  [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_session)],
            WAITING_PHONE:    [MessageHandler(filters.TEXT & ~filters.COMMAND, start_phone_login)],
            WAITING_CODE:     [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_code)],
            WAITING_PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_password)],
        },
        fallbacks=[CallbackQueryHandler(button_handler, pattern="^cancel_conv$")],
        per_message=False,
    )

    dest_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(ask_dest, pattern="^add_dest$")],
        states={
            WAITING_DEST_ADD: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_dest)],
        },
        fallbacks=[CallbackQueryHandler(button_handler, pattern="^settings$")],
        per_message=False,
    )

    forward_conv = ConversationHandler(
        entry_points=[CommandHandler("forward", forward_command)],
        states={
            WAITING_FORWARD_FIRST: [MessageHandler(~filters.COMMAND, receive_forward_first)],
            WAITING_FORWARD_LAST:  [MessageHandler(~filters.COMMAND, receive_forward_last)],
        },
        fallbacks=[CallbackQueryHandler(button_handler, pattern="^cancel_conv$")],
        per_message=False,
    )

    save_conv = ConversationHandler(
        entry_points=[CommandHandler("save", save_command)],
        states={
            WAITING_SAVE_LINK: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_save_link),
                CallbackQueryHandler(bulk_save_start, pattern="^bulk_save$"),
            ],
            WAITING_BULK_SAVE_FIRST: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_bulk_save_first),
            ],
            WAITING_BULK_SAVE_LAST: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_bulk_save_last),
            ],
        },
        fallbacks=[CallbackQueryHandler(button_handler, pattern="^cancel_conv$")],
        per_message=False,
    )

    watch_conv = ConversationHandler(
        entry_points=[CommandHandler("watch", watch_command)],
        states={
            WAITING_WATCH_SOURCE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_watch_source)
            ],
        },
        fallbacks=[CallbackQueryHandler(button_handler, pattern="^cancel_conv$")],
        per_message=False,
    )

    incoming_conv = ConversationHandler(
        entry_points=[CommandHandler("incoming", incoming_command)],
        states={
            WAITING_INCOMING: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_incoming)
            ],
        },
        fallbacks=[CallbackQueryHandler(button_handler, pattern="^cancel_conv$")],
        per_message=False,
    )

    outgoing_conv = ConversationHandler(
        entry_points=[CommandHandler("outgoing", outgoing_command)],
        states={
            WAITING_OUTGOING: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_outgoing)
            ],
        },
        fallbacks=[CallbackQueryHandler(button_handler, pattern="^cancel_conv$")],
        per_message=False,
    )

    add_route_conv = ConversationHandler(
        entry_points=[CommandHandler("add_route", add_route_command)],
        states={
            WAITING_ADD_ROUTE_SRC: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_add_route_src)
            ],
            WAITING_ADD_ROUTE_DST: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_add_route_dst)
            ],
        },
        fallbacks=[CallbackQueryHandler(button_handler, pattern="^cancel_conv$")],
        per_message=False,
    )

    remove_route_conv = ConversationHandler(
        entry_points=[CommandHandler("remove_route", remove_route_command)],
        states={
            WAITING_REMOVE_ROUTE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_remove_route)
            ],
        },
        fallbacks=[CallbackQueryHandler(button_handler, pattern="^cancel_conv$")],
        per_message=False,
    )

    filter_conv = ConversationHandler(
        entry_points=[CommandHandler("filter", filter_command)],
        states={
            WAITING_FILTER: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_filter)
            ],
        },
        fallbacks=[CallbackQueryHandler(button_handler, pattern="^cancel_conv$")],
        per_message=False,
    )

    remove_filter_conv = ConversationHandler(
        entry_points=[CommandHandler("remove_filter", remove_filter_command)],
        states={
            WAITING_REMOVE_FILTER: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_remove_filter)
            ],
        },
        fallbacks=[CallbackQueryHandler(button_handler, pattern="^cancel_conv$")],
        per_message=False,
    )

    blacklist_conv = ConversationHandler(
        entry_points=[CommandHandler("blacklist", blacklist_command)],
        states={
            WAITING_BLACKLIST: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_blacklist)
            ],
        },
        fallbacks=[CallbackQueryHandler(button_handler, pattern="^cancel_conv$")],
        per_message=False,
    )

    rem_blacklist_conv = ConversationHandler(
        entry_points=[CommandHandler("rem_blacklist", rem_blacklist_command)],
        states={
            WAITING_REM_BLACKLIST: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_rem_blacklist)
            ],
        },
        fallbacks=[CallbackQueryHandler(button_handler, pattern="^cancel_conv$")],
        per_message=False,
    )

    whitelist_conv = ConversationHandler(
        entry_points=[CommandHandler("whitelist", whitelist_command)],
        states={
            WAITING_WHITELIST: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_whitelist)
            ],
        },
        fallbacks=[CallbackQueryHandler(button_handler, pattern="^cancel_conv$")],
        per_message=False,
    )

    rem_whitelist_conv = ConversationHandler(
        entry_points=[CommandHandler("rem_whitelist", rem_whitelist_command)],
        states={
            WAITING_REM_WHITELIST: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_rem_whitelist)
            ],
        },
        fallbacks=[CallbackQueryHandler(button_handler, pattern="^cancel_conv$")],
        per_message=False,
    )

    delay_conv = ConversationHandler(
        entry_points=[CommandHandler("delay", delay_command)],
        states={
            WAITING_DELAY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_delay)
            ],
        },
        fallbacks=[CallbackQueryHandler(button_handler, pattern="^cancel_conv$")],
        per_message=False,
    )

    begin_text_conv = ConversationHandler(
        entry_points=[CommandHandler("begin_text", begin_text_command)],
        states={
            WAITING_BEGIN_TEXT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_begin_text)
            ],
        },
        fallbacks=[CallbackQueryHandler(button_handler, pattern="^cancel_conv$")],
        per_message=False,
    )

    end_text_conv = ConversationHandler(
        entry_points=[CommandHandler("end_text", end_text_command)],
        states={
            WAITING_END_TEXT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_end_text)
            ],
        },
        fallbacks=[CallbackQueryHandler(button_handler, pattern="^cancel_conv$")],
        per_message=False,
    )

    filter_users_conv = ConversationHandler(
        entry_points=[CommandHandler("filter_users", filter_users_command)],
        states={
            WAITING_FILTER_USERS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_filter_users)
            ],
        },
        fallbacks=[CallbackQueryHandler(button_handler, pattern="^cancel_conv$")],
        per_message=False,
    )

    # ── Register all handlers (ConversationHandlers first, then Commands) ──
    app.add_handler(login_conv)
    app.add_handler(dest_conv)
    app.add_handler(forward_conv)
    app.add_handler(save_conv)
    app.add_handler(watch_conv)
    # New route & filter conversation handlers
    app.add_handler(incoming_conv)
    app.add_handler(outgoing_conv)
    app.add_handler(add_route_conv)
    app.add_handler(remove_route_conv)
    app.add_handler(filter_conv)
    app.add_handler(remove_filter_conv)
    app.add_handler(blacklist_conv)
    app.add_handler(rem_blacklist_conv)
    app.add_handler(whitelist_conv)
    app.add_handler(rem_whitelist_conv)
    app.add_handler(delay_conv)
    app.add_handler(begin_text_conv)
    app.add_handler(end_text_conv)
    app.add_handler(filter_users_conv)

    # Existing commands (unchanged)
    app.add_handler(CommandHandler("start",    start))
    app.add_handler(CommandHandler("help",     help_command))
    app.add_handler(CommandHandler("settings", settings_command))
    app.add_handler(CommandHandler("stop",     stop_command))
    app.add_handler(CommandHandler("cleanup",  cleanup_command))
    app.add_handler(CommandHandler("status",   status_command))
    app.add_handler(CommandHandler("unwatch",  unwatch_command))
    # New simple commands
    app.add_handler(CommandHandler("routes",         routes_command))
    app.add_handler(CommandHandler("config",         config_command))
    app.add_handler(CommandHandler("reset_config",   reset_config_command))
    app.add_handler(CommandHandler("remove_session", remove_session_command))
    app.add_handler(CommandHandler("delete",         delete_command))
    # Toggle commands
    for _tcmd in ["copy_mode", "toggle_media", "toggle_text",
                  "toggle_url_preview", "toggle_edit_sync", "toggle_delete_sync"]:
        app.add_handler(CommandHandler(_tcmd, toggle_config_command))

    app.add_handler(CallbackQueryHandler(button_handler))

    async def post_init(application):
        await application.bot.set_my_commands([
            # ── Core ─────────────────────────────────────
            BotCommand("start",          "Welcome message"),
            BotCommand("help",           "Show all commands"),
            BotCommand("login",          "Connect userbot (session or phone)"),
            BotCommand("settings",       "Manage destination channels"),
            BotCommand("status",         "Show bot status & downloads folder info"),
            # ── Forwarding ───────────────────────────────
            BotCommand("watch",          "Start auto-forwarding (route-based or legacy)"),
            BotCommand("unwatch",        "Stop auto-forwarding"),
            BotCommand("routes",         "List all forwarding routes"),
            BotCommand("add_route",      "Add a source → destination route"),
            BotCommand("remove_route",   "Remove a route"),
            BotCommand("incoming",       "Add source channel"),
            BotCommand("outgoing",       "Add destination channel"),
            # ── Batch copy ───────────────────────────────
            BotCommand("forward",        "Copy a message range to destinations"),
            BotCommand("save",           "Fetch & repost a single restricted message"),
            BotCommand("stop",           "Stop active copy job"),
            # ── Filters ──────────────────────────────────
            BotCommand("filter",         "Add text replacement filter (old ==> new)"),
            BotCommand("remove_filter",  "Remove a text filter"),
            BotCommand("blacklist",      "Add blacklist keywords"),
            BotCommand("rem_blacklist",  "Remove blacklist keyword"),
            BotCommand("whitelist",      "Add whitelist keywords"),
            BotCommand("rem_whitelist",  "Remove whitelist keyword"),
            BotCommand("filter_users",   "Only forward from specific user IDs"),
            # ── Settings ─────────────────────────────────
            BotCommand("delay",          "Set forwarding delay in seconds"),
            BotCommand("begin_text",     "Set prefix text for forwarded messages"),
            BotCommand("end_text",       "Set suffix text for forwarded messages"),
            BotCommand("copy_mode",      "Toggle copy vs native forward mode"),
            BotCommand("toggle_media",   "Toggle media forwarding on/off"),
            BotCommand("toggle_text",    "Toggle text message forwarding on/off"),
            BotCommand("toggle_edit_sync",   "Toggle edit sync (mirror edits)"),
            BotCommand("toggle_delete_sync", "Toggle delete sync (mirror deletes)"),
            BotCommand("config",         "View full current configuration"),
            BotCommand("reset_config",   "Reset settings to defaults"),
            # ── Account ──────────────────────────────────
            BotCommand("remove_session", "Remove stored userbot session"),
            BotCommand("cleanup",        "Cleanup old downloaded files"),
            BotCommand("delete",         "Permanently delete all your data"),
        ])

        # ── Restart-recovery: resume any active auto-forward sessions ──
        # The upgraded _start_auto_forward auto-detects routes vs legacy mode.
        try:
            active_watches = db.get_all_active_watches()
            for watch in active_watches:
                uid = watch["user_id"]
                src = watch.get("source_channel")
                logger.info(f"Restart-recovery: resuming auto-forward for user {uid}")
                ok = await _start_auto_forward(uid, src)
                if not ok:
                    logger.warning(f"Restart-recovery: failed to resume watch for user {uid}")
                    db.set_watch_active(uid, False)
        except Exception as exc:
            logger.error(f"Restart-recovery error: {exc}")

    app.post_init = post_init

    # Start the HTTP health-check server in a background daemon thread
    # BEFORE run_polling so Koyeb marks the service healthy right away.
    health_thread = threading.Thread(target=_start_health_server, daemon=True)
    health_thread.start()

    logger.info("Bot starting…")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
