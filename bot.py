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
from telethon import TelegramClient
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
) = range(11)

# ─────────────────────────────────────────────
#  Global userbot reference
# ─────────────────────────────────────────────
userbot_client: Optional[TelegramClient] = None
forward_task:   Optional[asyncio.Task]   = None

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
    "▸ `/status` — Show bot status & downloads folder info\n"
    "▸ `/cleanup` — Manually cleanup old downloaded files\n"
    "▸ `/stop` — Stop an active copy job\n"
    "▸ `/settings` — Add / remove destination channels\n\n"
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
    "⏱️ *Rate Limiting:*\n"
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
    
    # Get downloads folder info
    downloads_count = len(list(DOWNLOADS_DIR.glob("*"))) if DOWNLOADS_DIR.exists() else 0
    downloads_size = sum(f.stat().st_size for f in DOWNLOADS_DIR.glob("*") if f.is_file())
    downloads_size_mb = downloads_size / (1024 * 1024)
    
    text = (
        "📊 *Bot Status*\n\n"
        f"🔐 Userbot:      {'✅ Connected' if session else '❌ Not connected'}\n"
        f"📤 Destinations: {len(dests)} channel(s)\n"
        f"🔄 Copy job:     {'▶️ Running' if active else '⏹ Idle'}\n\n"
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
    retries: int = 3,
) -> bool:
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
                    # Use the first message's text as caption
                    caption = next((m.message for m in valid if m.message), "")
                    # FIX: pass caption_entities for proper formatting on albums
                    caption_entities = next(
                        (m.entities for m in valid if m.message and m.entities), None
                    )
                    
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
                            await client.send_file(
                                dest,
                                file_paths,
                                caption=caption,
                                formatting_entities=caption_entities,
                            )
                            logger.info("✓ Album sent successfully")
                        return True
                    except Exception as send_exc:
                        logger.error(f"Album send failed: {type(send_exc).__name__}: {send_exc}")
                        raise

                if msg is None:
                    return True

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
                        await client.send_file(
                            dest,
                            file_to_send,
                            caption=msg.message or "",
                            # FIX: pass entities so bold/italic/links survive on captions
                            formatting_entities=msg.entities if msg.entities else None,
                        )
                        logger.info("✓ Media sent successfully")
                        return True
                    except Exception as send_exc:
                        logger.error(f"send_file failed: {type(send_exc).__name__}: {send_exc}")
                        raise

                # ── Plain text ────────────────────────────────────────
                if msg.message:
                    logger.info("Sending text message")
                    await client.send_message(
                        dest,
                        msg.message,
                        formatting_entities=msg.entities if msg.entities else None,
                    )
                    logger.info("✓ Text sent successfully")
                    return True

                logger.warning("Message has no media or text")
                return True  # empty — skip silently

            except FloodWaitError as fwe:
                wait = fwe.seconds + 3
                logger.warning(f"FloodWait error — sleeping {wait}s")
                await asyncio.sleep(wait)

            except Exception as exc:
                logger.error(f"_safe_send attempt {attempt + 1}/{retries}: {type(exc).__name__}: {exc}")
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt)

        logger.error("All retry attempts failed")
        return False
    
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

    # ConversationHandlers must be registered before the generic button_handler
    app.add_handler(login_conv)
    app.add_handler(dest_conv)
    app.add_handler(forward_conv)
    app.add_handler(save_conv)

    app.add_handler(CommandHandler("start",    start))
    app.add_handler(CommandHandler("help",     help_command))
    app.add_handler(CommandHandler("settings", settings_command))
    app.add_handler(CommandHandler("stop",     stop_command))
    app.add_handler(CommandHandler("cleanup",  cleanup_command))
    app.add_handler(CommandHandler("status",   status_command))
    app.add_handler(CallbackQueryHandler(button_handler))

    async def post_init(application):
        await application.bot.set_my_commands([
            BotCommand("start",    "Welcome message"),
            BotCommand("help",     "Show all commands"),
            BotCommand("login",    "Connect userbot (session or phone)"),
            BotCommand("settings", "Manage destination channels"),
            BotCommand("forward",  "Copy a message range to destinations"),
            BotCommand("save",     "Fetch & repost a single restricted message"),
            BotCommand("status",   "Show bot status & downloads folder info"),
            BotCommand("cleanup",  "Cleanup old downloaded files"),
            BotCommand("stop",     "Stop active copy job"),
        ])

    app.post_init = post_init
    logger.info("Bot starting…")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
