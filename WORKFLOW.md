# Universal Telegram Forward Bot — Workflow & Architecture

> A Telegram userbot-powered message forwarder. Copies messages from any
> source channel to multiple destination channels **without** the "Forwarded
> from" tag, using the MTProto protocol via Telethon.

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Module Breakdown](#module-breakdown)
3. [Conversation Flows](#conversation-flows)
4. [Data Flow — Sessions & Encryption](#data-flow--sessions--encryption)
5. [Rate Limiting](#rate-limiting)
6. [Koyeb Deployment Guide](#koyeb-deployment-guide)
7. [Environment Variables Reference](#environment-variables-reference)

---

## System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Telegram Platform                           │
│                                                                     │
│   User ──(Bot API)──► python-telegram-bot    ◄── Bot Commands      │
│                              │                                      │
│   Source Channel ──(MTProto)──► Telethon Userbot ──► Dest Channels │
└─────────────────────────────────────────────────────────────────────┘
                               │
                     ┌─────────▼─────────┐
                     │    bot.py         │
                     │  (Bot Logic)      │
                     └─────────┬─────────┘
                               │
                     ┌─────────▼─────────┐
                     │   database.py     │
                     │  (MongoDB Layer)  │
                     └─────────┬─────────┘
                               │
                     ┌─────────▼─────────┐
                     │  MongoDB Atlas    │
                     │  (Free M0 Tier)   │
                     └───────────────────┘
```

The bot runs **two simultaneous Telegram connections**:
- **Bot API** (`python-telegram-bot`) — handles commands from the operator user
- **MTProto userbot** (`Telethon`) — reads restricted/private channel messages as a real Telegram user

---

## Module Breakdown

### `bot.py` (2,188 lines)

| Section | Lines | Purpose |
|---|---|---|
| Config & Globals | 1–110 | Load env vars, set up downloads dir, global state |
| Session Helpers | 113–224 | Telethon/Pyrogram session conversion & verification |
| Misc Helpers | 273–329 | Channel ID parsing, validation, normalisation |
| Keyboards | 332–367 | Inline keyboard builders |
| Command Handlers | 370–500 | `/start`, `/help`, `/settings`, `/status`, `/stop`, `/cleanup` |
| Login Conversation | 502–690 | Multi-step login flow (session string or phone number) |
| Destination Conv. | 692–753 | Add/manage destination channels |
| Link Parsing | 756–798 | Parse `t.me/...` links and forwarded message origins |
| /forward Conv. | 800–921 | Two-step forward range setup |
| /save Conv. | 924–1002 | Single restricted message saver |
| Bulk Save Conv. | 1005–1365 | Range-based restricted message saver |
| File Cleanup | 1528–1608 | Downloads folder management |
| Copy Engine | 1610–1809 | `_safe_send` — core send with retry, download, re-upload |
| Copy Loop | 1811–2023 | `_copy_range_loop` — batch forwarder with rate limiting |
| Callback Router | 2025–2093 | Routes all inline button presses |
| App Setup | 2095–2188 | Registers handlers and starts polling |

### `database.py`

MongoDB Atlas driver layer. **All method signatures are identical to the original sqlite3 version** — `bot.py` is completely unaware of the underlying database technology.

| Collection | Purpose | Key Fields |
|---|---|---|
| `users` | Operator Telegram user records | `user_id`, `first_name`, `last_name` |
| `sessions` | Encrypted Telethon string sessions | `user_id`, `string_session` (encrypted), `is_active` |
| `destination_channels` | Per-user destination list | `user_id`, `channel_id`, `is_active` |
| `source_channels` | Last-used source channel per user | `user_id`, `channel_id` |
| `forwarding_stats` | Copy/save job tracking | `user_id`, `session_id`, `forwarded_count`, `is_active` |
| `admins` | Admin user IDs | `user_id` |

---

## Conversation Flows

### Login (`/login`)

```
/login or 🔐 Login button
        │
        ▼
  WAITING_SESSION
        │
        ├─── Looks like a phone number (+1234567890)?
        │              │
        │              ▼
        │         WAITING_CODE ──── send_code_request()
        │              │
        │              ├── 2FA enabled? ──► WAITING_PASSWORD ──► ✅ Logged in
        │              │
        │              └── OK ──────────────────────────────────► ✅ Logged in
        │
        └─── Looks like a session string?
                       │
                       ├── Telethon format? ──► verify ──► ✅ Logged in
                       │
                       └── Pyrogram format? ──► convert ──► verify ──► ✅ Logged in

  On success: session stored encrypted in MongoDB → userbot_client set globally
```

### /forward — Batch Copy

```
/forward
    │
    ├── No session? → error
    ├── No destinations? → error
    │
    ▼
WAITING_FORWARD_FIRST
    │  (user forwards a message or pastes t.me link)
    ▼
WAITING_FORWARD_LAST
    │  (user forwards last message or pastes link)
    ▼
_copy_range_loop() ──► asyncio.Task (background)
    │
    ├── Resolve source entity via Telethon
    ├── Resolve all destination entities
    ├── iter_messages() in batches of 50
    │       ├── Group albums (shared grouped_id)
    │       └── For each group → _safe_send()
    ├── Rate limit: 1 msg/sec, 30/min, 30s break
    └── Progress updates via edit_message_text()
```

### /save — Single Restricted Message

```
/save
    │
    ▼
WAITING_SAVE_LINK
    │  (user pastes t.me/c/... or t.me/channel/... link)
    ▼
_save_and_repost() ──► asyncio.Task (background)
    │
    ├── Resolve source entity
    ├── get_messages(ids=[msg_id])
    ├── If grouped_id → iter_messages() to collect full album
    ├── Resolve destination entities
    └── _safe_send() to each destination
```

### /save → 📦 Bulk Save — Range of Restricted Messages

```
📦 Bulk Save button (inside /save flow)
    │
    ▼
WAITING_BULK_SAVE_FIRST ──► store channel + first_id
    │
    ▼
WAITING_BULK_SAVE_LAST
    │
    ▼
_bulk_save_loop() ──► asyncio.Task (background)
    │
    ├── Resolve source entity
    ├── Resolve all destination entities
    ├── For msg_id in range(first_id, last_id+1):
    │       ├── get_messages(ids=[msg_id])
    │       ├── Skip MessageEmpty (deleted/inaccessible)
    │       ├── If album → iter_messages() window to collect parts
    │       └── _safe_send() to each destination
    ├── Rate limit: same as /forward
    └── Progress updates every 3s or every 5 messages
```

---

## Data Flow — Sessions & Encryption

```
User sends session string
        │
        ▼
receive_session() in bot.py
        │
        ├── _resolve_any_session(raw)
        │       ├── Try as Telethon StringSession → get_me() ping
        │       └── Try convert Pyrogram → Telethon → get_me() ping
        │
        ▼
db.save_session(user_id, telethon_session_str)
        │
        ├── Database._encrypt(session_str)
        │       └── Fernet.encrypt() with key from ENCRYPTION_KEY env var
        │
        └── MongoDB: sessions.update_one(upsert=True)
                      {string_session: "<encrypted_blob>", is_active: true}


Later, on /forward or /save:
        │
get_userbot(user_id)
        │
        ├── db.get_session(user_id)
        │       └── MongoDB: sessions.find_one() → Fernet.decrypt() → plain session
        │
        ├── _make_client(session_str) → TelegramClient
        ├── client.connect()
        └── client.get_me()  ← real server round-trip (catches revoked sessions)
```

**Key**: The `ENCRYPTION_KEY` env var must stay constant. If it changes, all stored sessions become unreadable (Fernet decryption fails). Users would need to re-login.

---

## Rate Limiting

Both `/forward` and Bulk Save apply the same three-layer rate limiting:

| Layer | Limit | Action when hit |
|---|---|---|
| Per-second | 1 message/sec | `asyncio.sleep()` for remainder of second |
| Per-minute | 30 messages/min | `asyncio.sleep(30)` — 30-second break |
| Telegram FloodWait | Dynamic | Sleep for `fwe.seconds + 2/3` then retry once |

`_safe_send()` has its own retry loop (3 attempts, exponential back-off: 1s, 2s, 4s).

---

## Koyeb Deployment Guide

### 1. Create MongoDB Atlas Cluster

1. Go to [cloud.mongodb.com](https://cloud.mongodb.com) and sign up (free)
2. Create a **free M0 cluster** (any cloud/region)
3. Under **Database Access**, create a DB user with Read/Write access
4. Under **Network Access**, add `0.0.0.0/0` (allow all IPs — required for Koyeb)
5. Go to **Connect → Drivers → Python** and copy the connection string
6. Replace `<password>` with your DB user's password
7. Append `/universalbot` before `?retryWrites` to set the database name

### 2. Generate Encryption Key

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Save this value — you'll need it as `ENCRYPTION_KEY` on Koyeb. **Do not lose it.**

### 3. Push to GitHub

```bash
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/yourname/your-repo.git
git push -u origin main
```

> [!CAUTION]
> Confirm `.gitignore` is present and `.env` / `bot_data.db` / `.encryption_key` are NOT tracked
> before pushing. Run `git status` to verify.

### 4. Deploy on Koyeb

1. Go to [app.koyeb.com](https://app.koyeb.com) → **Create Service**
2. Select **GitHub** → choose your repository
3. Set **Service type** to **Worker** (not Web — no HTTP port needed)
4. Set **Run command** to `python bot.py` (or let Koyeb read `Procfile`)
5. Under **Environment variables**, add all 5 required vars:

| Key | Value |
|---|---|
| `BOT_TOKEN` | Your bot token from BotFather |
| `API_ID` | Your API ID from my.telegram.org |
| `API_HASH` | Your API hash from my.telegram.org |
| `MONGODB_URI` | Your Atlas connection string |
| `ENCRYPTION_KEY` | The Fernet key you generated |

6. Click **Deploy**
7. Watch the build logs — the bot should print `Bot starting…` on success

### 5. Verify

- Send `/start` to your bot on Telegram
- Check Koyeb logs for `Connected to MongoDB Atlas`
- Login with `/login` and confirm your session persists after a redeploy

---

## Environment Variables Reference

| Variable | Required | Description |
|---|---|---|
| `BOT_TOKEN` | ✅ Yes | Telegram Bot API token from [@BotFather](https://t.me/BotFather) |
| `API_ID` | ✅ Yes | Telegram MTProto App ID from [my.telegram.org](https://my.telegram.org) |
| `API_HASH` | ✅ Yes | Telegram MTProto App Hash from [my.telegram.org](https://my.telegram.org) |
| `MONGODB_URI` | ✅ Yes | MongoDB Atlas SRV connection string (includes DB name) |
| `ENCRYPTION_KEY` | ✅ Yes (prod) | Fernet key for session encryption. Falls back to `.encryption_key` file locally |

---

## File Structure

```
Universal Bot Stable/
├── bot.py              # Main bot — all handlers, conversations, copy engine
├── database.py         # MongoDB persistence layer (drop-in for sqlite3)
├── requirements.txt    # Python dependencies
├── Procfile            # Koyeb worker process definition
├── runtime.txt         # Python version for Koyeb buildpack
├── .env.example        # Template for environment variables
├── .gitignore          # Protects secrets and local data from git
├── WORKFLOW.md         # This document
└── downloads/          # Temporary media files (auto-cleaned after send)
```
