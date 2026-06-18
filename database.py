"""
Database module for storing bot data persistently.
Uses MongoDB Atlas (pymongo) for cloud persistence compatible with Koyeb's
ephemeral filesystem. All method signatures are identical to the original
sqlite3 version — bot.py requires no changes.

Collections (all scoped inside one MongoDB database):
  users               — Telegram user records
  sessions            — Encrypted Telethon string sessions
  destination_channels — Per-user destination channels
  source_channels     — Per-user last-used source channel
  forwarding_stats    — Copy/save job statistics
  admins              — Admin user IDs

Encryption:
  Session strings are encrypted with Fernet before storage.
  The key is loaded from the ENCRYPTION_KEY environment variable
  (a URL-safe base64-encoded 32-byte key).
  To generate: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
"""

import os
import logging
from datetime import datetime, timezone
from typing import List, Optional, Dict

from cryptography.fernet import Fernet
from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  Encryption Key Bootstrap
# ─────────────────────────────────────────────

_ENCRYPTION_KEY_FILE = ".encryption_key"


def _load_encryption_key() -> bytes:
    """
    Load the Fernet encryption key.

    Priority:
    1. ENCRYPTION_KEY environment variable (required in production / Koyeb)
    2. .encryption_key file  (local development fallback — NOT for production)

    Raises RuntimeError if neither source provides a valid key.
    """
    # 1. Environment variable (production / Koyeb)
    env_key = os.getenv("ENCRYPTION_KEY", "").strip()
    if env_key:
        try:
            # Validate the key by constructing a Fernet instance
            Fernet(env_key.encode())
            logger.info("Encryption key loaded from ENCRYPTION_KEY env var.")
            return env_key.encode()
        except Exception as exc:
            raise RuntimeError(
                "ENCRYPTION_KEY env var is set but invalid: "
                f"{exc}\n"
                "Generate a valid key with: "
                "python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
            ) from exc

    # 2. Local file fallback (development only)
    key_path = _ENCRYPTION_KEY_FILE
    if os.path.exists(key_path):
        logger.warning(
            "Using .encryption_key file — NOT suitable for production. "
            "Set the ENCRYPTION_KEY environment variable on Koyeb."
        )
        with open(key_path, "rb") as f:
            return f.read().strip()

    # 3. Generate a new key for local dev convenience
    logger.warning(
        "No ENCRYPTION_KEY env var and no .encryption_key file found. "
        "Generating a new key and saving to .encryption_key for local development. "
        "Set ENCRYPTION_KEY on Koyeb to persist sessions across restarts."
    )
    new_key = Fernet.generate_key()
    with open(key_path, "wb") as f:
        f.write(new_key)
    try:
        os.chmod(key_path, 0o600)
    except Exception:
        pass
    return new_key


# ─────────────────────────────────────────────
#  Database Class
# ─────────────────────────────────────────────

class Database:
    """
    MongoDB-backed persistent store for the Telegram Forward Bot.
    Drop-in replacement for the original sqlite3 Database class.
    """

    # Name of the MongoDB database used exclusively by this bot.
    # Hardcoded so it never accidentally clobbers another DB on the same cluster.
    DB_NAME = "tg_forwardbot"

    def __init__(self, db_path: str = "bot_data.db"):
        """
        db_path is accepted for API compatibility but ignored —
        MongoDB URI comes from the MONGODB_URI environment variable.
        """
        self.cipher = Fernet(_load_encryption_key())
        self._client = self._connect()
        # Use a dedicated database name — never the URI default path —
        # so this bot's collections never conflict with other projects.
        self._db = self._client.get_database(self.DB_NAME)
        self._ensure_indexes()
        logger.info(f"MongoDB database '{self.DB_NAME}' initialized successfully.")

    # ── Connection ────────────────────────────────────────────────

    def _connect(self) -> MongoClient:
        uri = os.getenv("MONGODB_URI", "")
        if not uri:
            raise RuntimeError(
                "MONGODB_URI environment variable is not set.\n"
                "Set it to your MongoDB Atlas connection string, e.g.:\n"
                "  mongodb+srv://user:pass@cluster0.xxxxx.mongodb.net/?retryWrites=true&w=majority\n"
                "The database name is set automatically to 'tg_forwardbot'."
            )
        client = MongoClient(uri, serverSelectionTimeoutMS=10_000)
        # Ping to validate the connection at startup
        client.admin.command("ping")
        logger.info("Connected to MongoDB Atlas.")
        return client

    def _ensure_indexes(self):
        """
        Create unique indexes for all collections.

        sparse=True means documents where the field is missing/null are skipped
        by the index — this prevents DuplicateKeyError when an existing collection
        on the cluster has null-valued documents.
        """
        try:
            db = self._db
            db["users"].create_index("user_id", unique=True, sparse=True)
            db["sessions"].create_index("user_id", unique=True, sparse=True)
            db["destination_channels"].create_index(
                [("user_id", ASCENDING), ("channel_id", ASCENDING)],
                unique=True,
                sparse=True,
            )
            db["source_channels"].create_index("user_id", unique=True, sparse=True)
            db["admins"].create_index("user_id", unique=True, sparse=True)
            db["watch_config"].create_index("user_id", unique=True, sparse=True)
            # ── New collections for route-based forwarding ─────────────
            db["user_sources"].create_index(
                [("user_id", ASCENDING), ("channel_id", ASCENDING)], unique=True, sparse=True
            )
            db["routes"].create_index(
                [("user_id", ASCENDING), ("source", ASCENDING), ("destination", ASCENDING)],
                sparse=True,
            )
            db["global_configs"].create_index("user_id", unique=True, sparse=True)
            db["blacklists"].create_index(
                [("user_id", ASCENDING), ("keyword", ASCENDING)], unique=True, sparse=True
            )
            db["whitelists"].create_index(
                [("user_id", ASCENDING), ("keyword", ASCENDING)], unique=True, sparse=True
            )
            db["user_filters"].create_index(
                [("user_id", ASCENDING), ("target_user_id", ASCENDING)], unique=True, sparse=True
            )
            db["message_mappings"].create_index(
                [("user_id", ASCENDING), ("source_chat", ASCENDING), ("source_msg_id", ASCENDING)]
            )
            # TTL index: auto-delete message mappings after 7 days
            db["message_mappings"].create_index("created_at", expireAfterSeconds=604_800)
            logger.info("MongoDB indexes ensured.")
        except Exception as exc:
            # Log but don't crash — indexes are performance/safety helpers,
            # not strictly required for the bot to function.
            logger.warning(f"Could not ensure all MongoDB indexes: {exc}")

    # ── Encryption helpers ────────────────────────────────────────

    def _encrypt(self, data: str) -> str:
        return self.cipher.encrypt(data.encode()).decode()

    def _decrypt(self, encrypted_data: str) -> str:
        try:
            return self.cipher.decrypt(encrypted_data.encode()).decode()
        except Exception as exc:
            logger.error(f"Decryption error: {exc}")
            return ""

    # ── Convenience ───────────────────────────────────────────────

    def _col(self, name: str) -> Collection:
        return self._db[name]

    @staticmethod
    def _now() -> datetime:
        return datetime.now(tz=timezone.utc)

    # ─────────────────────────────────────────────
    #  User Management
    # ─────────────────────────────────────────────

    def add_user(
        self,
        user_id: int,
        first_name: str,
        last_name: str = "",
        phone_number: str = "",
    ) -> bool:
        """Add or update a user record."""
        try:
            self._col("users").update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "user_id": user_id,
                        "first_name": first_name,
                        "last_name": last_name,
                        "phone_number": phone_number,
                        "updated_at": self._now(),
                    },
                    "$setOnInsert": {"created_at": self._now()},
                },
                upsert=True,
            )
            return True
        except Exception as exc:
            logger.error(f"Error adding user: {exc}")
            return False

    def get_user(self, user_id: int) -> Optional[Dict]:
        """Get user information."""
        try:
            doc = self._col("users").find_one({"user_id": user_id}, {"_id": 0})
            return doc
        except Exception as exc:
            logger.error(f"Error getting user: {exc}")
            return None

    # ─────────────────────────────────────────────
    #  Session Management
    # ─────────────────────────────────────────────

    def save_session(self, user_id: int, session_string: str) -> bool:
        """Save an encrypted Telethon string session for a user."""
        try:
            encrypted = self._encrypt(session_string)
            self._col("sessions").update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "user_id": user_id,
                        "string_session": encrypted,
                        "is_active": True,
                        "updated_at": self._now(),
                    },
                    "$setOnInsert": {"created_at": self._now()},
                },
                upsert=True,
            )
            logger.info(f"Session saved for user {user_id}")
            return True
        except Exception as exc:
            logger.error(f"Error saving session: {exc}")
            return False

    def get_session(self, user_id: int) -> Optional[str]:
        """Retrieve and decrypt the active string session for a user."""
        try:
            doc = self._col("sessions").find_one(
                {"user_id": user_id, "is_active": True}
            )
            if doc:
                return self._decrypt(doc["string_session"])
            return None
        except Exception as exc:
            logger.error(f"Error getting session: {exc}")
            return None

    def delete_session(self, user_id: int) -> bool:
        """Soft-delete (deactivate) the session for a user."""
        try:
            self._col("sessions").update_one(
                {"user_id": user_id},
                {"$set": {"is_active": False, "updated_at": self._now()}},
            )
            return True
        except Exception as exc:
            logger.error(f"Error deleting session: {exc}")
            return False

    # ─────────────────────────────────────────────
    #  Destination Channels Management
    # ─────────────────────────────────────────────

    def add_destination_channel(
        self, user_id: int, channel_id: str, channel_name: str = ""
    ) -> bool:
        """
        Add or re-activate a destination channel.
        Returns True if the channel was newly added or re-activated,
        False if it was already active.
        """
        try:
            col = self._col("destination_channels")
            doc = col.find_one({"user_id": user_id, "channel_id": channel_id})

            if doc is None:
                # Fresh insert
                col.insert_one(
                    {
                        "user_id": user_id,
                        "channel_id": channel_id,
                        "channel_name": channel_name,
                        "channel_type": "unknown",
                        "is_active": True,
                        "created_at": self._now(),
                    }
                )
                logger.info(f"Destination {channel_id} added for user {user_id}")
                return True

            if not doc.get("is_active", True):
                # Re-activate soft-deleted entry
                col.update_one(
                    {"user_id": user_id, "channel_id": channel_id},
                    {
                        "$set": {
                            "is_active": True,
                            "channel_name": channel_name,
                        }
                    },
                )
                logger.info(f"Destination {channel_id} re-activated for user {user_id}")
                return True

            # Already active
            return False
        except Exception as exc:
            logger.error(f"Error adding destination channel: {exc}")
            return False

    def get_destination_channels(self, user_id: int) -> List[Dict]:
        """Get all active destination channels for a user."""
        try:
            docs = list(
                self._col("destination_channels")
                .find(
                    {"user_id": user_id, "is_active": True},
                    {"_id": 0},
                )
                .sort("created_at", -1)
            )
            return docs
        except Exception as exc:
            logger.error(f"Error getting destination channels: {exc}")
            return []

    def get_destination_channel_ids(self, user_id: int) -> List[str]:
        """Get the list of active destination channel IDs for a user."""
        return [ch["channel_id"] for ch in self.get_destination_channels(user_id)]

    def remove_destination_channel(self, user_id: int, channel_id: str) -> bool:
        """Soft-delete a destination channel."""
        try:
            self._col("destination_channels").update_one(
                {"user_id": user_id, "channel_id": channel_id},
                {"$set": {"is_active": False}},
            )
            logger.info(f"Destination {channel_id} removed for user {user_id}")
            return True
        except Exception as exc:
            logger.error(f"Error removing destination channel: {exc}")
            return False

    def remove_destination_channel_by_index(self, user_id: int, index: int) -> bool:
        """Remove a destination channel by its position in the active list."""
        try:
            channels = self.get_destination_channels(user_id)
            if 0 <= index < len(channels):
                return self.remove_destination_channel(user_id, channels[index]["channel_id"])
            return False
        except Exception as exc:
            logger.error(f"Error removing destination channel by index: {exc}")
            return False

    # ─────────────────────────────────────────────
    #  Source Channel Management
    # ─────────────────────────────────────────────

    def set_source_channel(
        self, user_id: int, channel_id: str, channel_name: str = ""
    ) -> bool:
        """Set (upsert) the source channel for a user."""
        try:
            self._col("source_channels").update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "user_id": user_id,
                        "channel_id": channel_id,
                        "channel_name": channel_name,
                        "updated_at": self._now(),
                    },
                    "$setOnInsert": {"created_at": self._now()},
                },
                upsert=True,
            )
            logger.info(f"Source channel {channel_id} set for user {user_id}")
            return True
        except Exception as exc:
            logger.error(f"Error setting source channel: {exc}")
            return False

    def get_source_channel(self, user_id: int) -> Optional[str]:
        """Get the source channel ID for a user."""
        try:
            doc = self._col("source_channels").find_one(
                {"user_id": user_id}, {"channel_id": 1, "_id": 0}
            )
            return doc["channel_id"] if doc else None
        except Exception as exc:
            logger.error(f"Error getting source channel: {exc}")
            return None

    # ─────────────────────────────────────────────
    #  Forwarding Stats Management
    # ─────────────────────────────────────────────

    def start_forwarding_session(self, user_id: int) -> int:
        """
        Start a new forwarding/save job session.
        Returns a pseudo-integer session ID derived from MongoDB's ObjectId
        (truncated to 32-bit int for compatibility with the existing bot.py logic).
        """
        try:
            result = self._col("forwarding_stats").insert_one(
                {
                    "user_id": user_id,
                    "forwarded_count": 0,
                    "failed_count": 0,
                    "is_active": True,
                    "started_at": self._now(),
                    "stopped_at": None,
                    "created_at": self._now(),
                }
            )
            # Convert ObjectId to a stable integer for use as session_id in bot.py
            session_id = int(str(result.inserted_id)[:8], 16)
            # Store the mapping so we can look up by this int later
            self._col("forwarding_stats").update_one(
                {"_id": result.inserted_id},
                {"$set": {"session_id": session_id}},
            )
            logger.info(f"Forwarding session {session_id} started for user {user_id}")
            return session_id
        except Exception as exc:
            logger.error(f"Error starting forwarding session: {exc}")
            return -1

    def update_forwarding_stats(
        self, session_id: int, forwarded: int, failed: int
    ) -> bool:
        """Update forwarding statistics for a running session."""
        try:
            self._col("forwarding_stats").update_one(
                {"session_id": session_id},
                {
                    "$set": {
                        "forwarded_count": forwarded,
                        "failed_count": failed,
                    }
                },
            )
            return True
        except Exception as exc:
            logger.error(f"Error updating forwarding stats: {exc}")
            return False

    def end_forwarding_session(self, session_id: int) -> bool:
        """Mark a forwarding session as ended."""
        try:
            self._col("forwarding_stats").update_one(
                {"session_id": session_id},
                {
                    "$set": {
                        "is_active": False,
                        "stopped_at": self._now(),
                    }
                },
            )
            logger.info(f"Forwarding session {session_id} ended")
            return True
        except Exception as exc:
            logger.error(f"Error ending forwarding session: {exc}")
            return False

    def get_forwarding_stats(self, session_id: int) -> Optional[Dict]:
        """Get forwarding statistics for a session."""
        try:
            doc = self._col("forwarding_stats").find_one(
                {"session_id": session_id}, {"_id": 0}
            )
            return doc
        except Exception as exc:
            logger.error(f"Error getting forwarding stats: {exc}")
            return None

    # ─────────────────────────────────────────────
    #  Admin Management
    # ─────────────────────────────────────────────

    def add_admin(self, user_id: int) -> bool:
        """Add an admin user."""
        try:
            self._col("admins").update_one(
                {"user_id": user_id},
                {
                    "$setOnInsert": {
                        "user_id": user_id,
                        "created_at": self._now(),
                    }
                },
                upsert=True,
            )
            return True
        except Exception as exc:
            logger.error(f"Error adding admin: {exc}")
            return False

    def is_admin(self, user_id: int) -> bool:
        """Check if a user is an admin."""
        try:
            return (
                self._col("admins").find_one({"user_id": user_id}) is not None
            )
        except Exception as exc:
            logger.error(f"Error checking admin status: {exc}")
            return False

    def get_all_admins(self) -> List[int]:
        """Get all admin user IDs."""
        try:
            return [
                doc["user_id"]
                for doc in self._col("admins").find({}, {"user_id": 1, "_id": 0})
            ]
        except Exception as exc:
            logger.error(f"Error getting admins: {exc}")
            return []

    def remove_admin(self, user_id: int) -> bool:
        """Remove an admin user."""
        try:
            self._col("admins").delete_one({"user_id": user_id})
            return True
        except Exception as exc:
            logger.error(f"Error removing admin: {exc}")
            return False

    # ─────────────────────────────────────────────
    #  Watch Config Management  (auto-forward /watch)
    # ─────────────────────────────────────────────

    def save_watch_config(self, user_id: int, source_channel: str) -> bool:
        """Save (upsert) the auto-forward source channel for a user."""
        try:
            self._col("watch_config").update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "user_id": user_id,
                        "source_channel": source_channel,
                        "is_active": True,
                        "updated_at": self._now(),
                    },
                    "$setOnInsert": {"created_at": self._now()},
                },
                upsert=True,
            )
            logger.info(f"Watch config saved for user {user_id}: {source_channel}")
            return True
        except Exception as exc:
            logger.error(f"Error saving watch config: {exc}")
            return False

    def get_watch_config(self, user_id: int) -> Optional[Dict]:
        """Get the auto-forward config for a user, or None if not set."""
        try:
            return self._col("watch_config").find_one(
                {"user_id": user_id}, {"_id": 0}
            )
        except Exception as exc:
            logger.error(f"Error getting watch config: {exc}")
            return None

    def set_watch_active(self, user_id: int, is_active: bool) -> bool:
        """Enable or disable auto-forward for a user without clearing the config."""
        try:
            self._col("watch_config").update_one(
                {"user_id": user_id},
                {"$set": {"is_active": is_active, "updated_at": self._now()}},
            )
            return True
        except Exception as exc:
            logger.error(f"Error setting watch active: {exc}")
            return False

    def clear_watch_config(self, user_id: int) -> bool:
        """Fully delete the auto-forward config for a user."""
        try:
            self._col("watch_config").delete_one({"user_id": user_id})
            return True
        except Exception as exc:
            logger.error(f"Error clearing watch config: {exc}")
            return False

    def get_all_active_watches(self) -> List[Dict]:
        """
        Return all watch configs with is_active=True.
        Used on bot startup to restore auto-forward sessions after a restart.
        """
        try:
            return list(
                self._col("watch_config").find(
                    {"is_active": True}, {"_id": 0}
                )
            )
        except Exception as exc:
            logger.error(f"Error getting active watches: {exc}")
            return []

    # ─────────────────────────────────────────────
    #  User Source Channels  (route system)
    # ─────────────────────────────────────────────

    def add_source_channel(self, user_id: int, channel_id: str, channel_name: str = "") -> bool:
        """Add or re-activate a source channel for route-based forwarding."""
        try:
            self._col("user_sources").update_one(
                {"user_id": user_id, "channel_id": channel_id},
                {
                    "$set": {
                        "user_id": user_id,
                        "channel_id": channel_id,
                        "channel_name": channel_name,
                        "is_active": True,
                        "updated_at": self._now(),
                    },
                    "$setOnInsert": {"created_at": self._now()},
                },
                upsert=True,
            )
            return True
        except Exception as exc:
            logger.error(f"Error adding source channel: {exc}")
            return False

    def get_source_channels(self, user_id: int) -> List[Dict]:
        try:
            return list(self._col("user_sources").find(
                {"user_id": user_id, "is_active": True}, {"_id": 0}
            ).sort("created_at", -1))
        except Exception as exc:
            logger.error(f"Error getting source channels: {exc}")
            return []

    def get_source_channel_ids(self, user_id: int) -> List[str]:
        return [s["channel_id"] for s in self.get_source_channels(user_id)]

    def remove_source_channel(self, user_id: int, channel_id: str) -> bool:
        try:
            self._col("user_sources").update_one(
                {"user_id": user_id, "channel_id": channel_id},
                {"$set": {"is_active": False}},
            )
            return True
        except Exception as exc:
            logger.error(f"Error removing source channel: {exc}")
            return False

    def remove_source_channel_by_index(self, user_id: int, index: int) -> bool:
        try:
            sources = self.get_source_channels(user_id)
            if 0 <= index < len(sources):
                return self.remove_source_channel(user_id, sources[index]["channel_id"])
            return False
        except Exception as exc:
            return False

    # ─────────────────────────────────────────────
    #  Routes
    # ─────────────────────────────────────────────

    def add_route(self, user_id: int, source: str, destination: str) -> bool:
        """Create a forwarding route. Returns False if duplicate."""
        try:
            existing = self._col("routes").find_one({
                "user_id": user_id, "source": source,
                "destination": destination, "is_active": True,
            })
            if existing:
                return False
            self._col("routes").insert_one({
                "user_id": user_id,
                "source": source,
                "destination": destination,
                "is_active": True,
                "created_at": self._now(),
            })
            return True
        except Exception as exc:
            logger.error(f"Error adding route: {exc}")
            return False

    def get_routes(self, user_id: int) -> List[Dict]:
        try:
            return list(self._col("routes").find(
                {"user_id": user_id, "is_active": True}, {"_id": 0}
            ).sort("created_at", 1))
        except Exception as exc:
            logger.error(f"Error getting routes: {exc}")
            return []

    def remove_route_by_index(self, user_id: int, index: int) -> bool:
        try:
            routes = self.get_routes(user_id)
            if 0 <= index < len(routes):
                r = routes[index]
                self._col("routes").update_one(
                    {"user_id": user_id, "source": r["source"], "destination": r["destination"]},
                    {"$set": {"is_active": False}},
                )
                return True
            return False
        except Exception as exc:
            logger.error(f"Error removing route: {exc}")
            return False

    def get_destinations_for_source(self, user_id: int, source: str) -> List[str]:
        """Get all destination channels mapped to a given source."""
        try:
            docs = self._col("routes").find(
                {"user_id": user_id, "source": source, "is_active": True},
                {"destination": 1, "_id": 0},
            )
            return [d["destination"] for d in docs]
        except Exception as exc:
            logger.error(f"Error getting destinations for source: {exc}")
            return []

    def get_unique_active_sources(self, user_id: int) -> List[str]:
        """Unique source channels across all active routes."""
        try:
            seen: List[str] = []
            for r in self.get_routes(user_id):
                if r["source"] not in seen:
                    seen.append(r["source"])
            return seen
        except Exception as exc:
            logger.error(f"Error getting unique sources: {exc}")
            return []

    # ─────────────────────────────────────────────
    #  Global Config
    # ─────────────────────────────────────────────

    _CONFIG_DEFAULTS: Dict = {
        "delay": 0,
        "begin_text": "",
        "end_text": "",
        "copy_mode": True,      # True=copy (no fwd tag), False=native forward
        "media_enabled": True,
        "text_enabled": True,
        "url_preview": False,
        "edit_sync": False,
        "delete_sync": False,
    }

    def get_config(self, user_id: int) -> Dict:
        """Return user config merged with defaults."""
        try:
            doc = self._col("global_configs").find_one({"user_id": user_id}, {"_id": 0})
            cfg = dict(self._CONFIG_DEFAULTS)
            if doc:
                for k in self._CONFIG_DEFAULTS:
                    if k in doc:
                        cfg[k] = doc[k]
            return cfg
        except Exception as exc:
            logger.error(f"Error getting config: {exc}")
            return dict(self._CONFIG_DEFAULTS)

    def set_config(self, user_id: int, **kwargs) -> bool:
        try:
            valid = {k: v for k, v in kwargs.items() if k in self._CONFIG_DEFAULTS}
            if not valid:
                return False
            self._col("global_configs").update_one(
                {"user_id": user_id},
                {
                    "$set": {**valid, "updated_at": self._now()},
                    "$setOnInsert": {"user_id": user_id, "created_at": self._now()},
                },
                upsert=True,
            )
            return True
        except Exception as exc:
            logger.error(f"Error setting config: {exc}")
            return False

    def reset_config(self, user_id: int) -> bool:
        try:
            self._col("global_configs").delete_one({"user_id": user_id})
            return True
        except Exception as exc:
            logger.error(f"Error resetting config: {exc}")
            return False

    # ─────────────────────────────────────────────
    #  Text Replacement Filters
    # ─────────────────────────────────────────────

    def add_filter(self, user_id: int, pattern: str, replacement: str,
                   is_regex: bool = False) -> bool:
        try:
            self._col("filters").insert_one({
                "user_id": user_id,
                "pattern": pattern,
                "replacement": replacement,
                "is_regex": is_regex,
                "created_at": self._now(),
            })
            return True
        except Exception as exc:
            logger.error(f"Error adding filter: {exc}")
            return False

    def get_filters(self, user_id: int) -> List[Dict]:
        try:
            return list(self._col("filters").find(
                {"user_id": user_id}, {"_id": 0}
            ).sort("created_at", 1))
        except Exception as exc:
            logger.error(f"Error getting filters: {exc}")
            return []

    def remove_filter_by_index(self, user_id: int, index: int) -> bool:
        try:
            filters = self.get_filters(user_id)
            if 0 <= index < len(filters):
                f = filters[index]
                self._col("filters").delete_one({
                    "user_id": user_id,
                    "pattern": f["pattern"],
                    "replacement": f["replacement"],
                })
                return True
            return False
        except Exception as exc:
            logger.error(f"Error removing filter: {exc}")
            return False

    # ─────────────────────────────────────────────
    #  Blacklist
    # ─────────────────────────────────────────────

    def add_blacklist(self, user_id: int, keyword: str) -> bool:
        try:
            self._col("blacklists").update_one(
                {"user_id": user_id, "keyword": keyword.lower()},
                {"$setOnInsert": {
                    "user_id": user_id, "keyword": keyword.lower(),
                    "created_at": self._now(),
                }},
                upsert=True,
            )
            return True
        except Exception as exc:
            logger.error(f"Error adding blacklist keyword: {exc}")
            return False

    def get_blacklist(self, user_id: int) -> List[str]:
        try:
            return [d["keyword"] for d in self._col("blacklists").find(
                {"user_id": user_id}, {"keyword": 1, "_id": 0}
            ).sort("created_at", 1)]
        except Exception as exc:
            logger.error(f"Error getting blacklist: {exc}")
            return []

    def remove_blacklist(self, user_id: int, keyword: str) -> bool:
        try:
            self._col("blacklists").delete_one({"user_id": user_id, "keyword": keyword.lower()})
            return True
        except Exception as exc:
            logger.error(f"Error removing blacklist: {exc}")
            return False

    def remove_blacklist_by_index(self, user_id: int, index: int) -> bool:
        try:
            items = self.get_blacklist(user_id)
            if 0 <= index < len(items):
                return self.remove_blacklist(user_id, items[index])
            return False
        except Exception as exc:
            return False

    # ─────────────────────────────────────────────
    #  Whitelist
    # ─────────────────────────────────────────────

    def add_whitelist(self, user_id: int, keyword: str) -> bool:
        try:
            self._col("whitelists").update_one(
                {"user_id": user_id, "keyword": keyword.lower()},
                {"$setOnInsert": {
                    "user_id": user_id, "keyword": keyword.lower(),
                    "created_at": self._now(),
                }},
                upsert=True,
            )
            return True
        except Exception as exc:
            logger.error(f"Error adding whitelist keyword: {exc}")
            return False

    def get_whitelist(self, user_id: int) -> List[str]:
        try:
            return [d["keyword"] for d in self._col("whitelists").find(
                {"user_id": user_id}, {"keyword": 1, "_id": 0}
            ).sort("created_at", 1)]
        except Exception as exc:
            logger.error(f"Error getting whitelist: {exc}")
            return []

    def remove_whitelist(self, user_id: int, keyword: str) -> bool:
        try:
            self._col("whitelists").delete_one({"user_id": user_id, "keyword": keyword.lower()})
            return True
        except Exception as exc:
            logger.error(f"Error removing whitelist: {exc}")
            return False

    def remove_whitelist_by_index(self, user_id: int, index: int) -> bool:
        try:
            items = self.get_whitelist(user_id)
            if 0 <= index < len(items):
                return self.remove_whitelist(user_id, items[index])
            return False
        except Exception as exc:
            return False

    # ─────────────────────────────────────────────
    #  User Filters (forward only from these senders)
    # ─────────────────────────────────────────────

    def add_user_filter(self, user_id: int, target_user_id: int) -> bool:
        try:
            self._col("user_filters").update_one(
                {"user_id": user_id, "target_user_id": target_user_id},
                {"$setOnInsert": {
                    "user_id": user_id, "target_user_id": target_user_id,
                    "created_at": self._now(),
                }},
                upsert=True,
            )
            return True
        except Exception as exc:
            logger.error(f"Error adding user filter: {exc}")
            return False

    def get_user_filters(self, user_id: int) -> List[int]:
        try:
            return [d["target_user_id"] for d in self._col("user_filters").find(
                {"user_id": user_id}, {"target_user_id": 1, "_id": 0}
            )]
        except Exception as exc:
            logger.error(f"Error getting user filters: {exc}")
            return []

    def remove_user_filter(self, user_id: int, target_user_id: int) -> bool:
        try:
            self._col("user_filters").delete_one({
                "user_id": user_id, "target_user_id": target_user_id
            })
            return True
        except Exception as exc:
            logger.error(f"Error removing user filter: {exc}")
            return False

    def clear_user_filters(self, user_id: int) -> bool:
        try:
            self._col("user_filters").delete_many({"user_id": user_id})
            return True
        except Exception as exc:
            return False

    # ─────────────────────────────────────────────
    #  Message Mappings  (edit/delete sync)
    # ─────────────────────────────────────────────

    def save_message_mapping(self, user_id: int, source_chat: str, source_msg_id: int,
                             dest_chat: str, dest_msg_id: int) -> bool:
        try:
            self._col("message_mappings").insert_one({
                "user_id": user_id,
                "source_chat": source_chat,
                "source_msg_id": source_msg_id,
                "dest_chat": dest_chat,
                "dest_msg_id": dest_msg_id,
                "created_at": self._now(),
            })
            return True
        except Exception as exc:
            logger.error(f"Error saving message mapping: {exc}")
            return False

    def get_message_mappings(self, user_id: int, source_chat: str,
                             source_msg_id: int) -> List[Dict]:
        try:
            return list(self._col("message_mappings").find(
                {"user_id": user_id, "source_chat": source_chat,
                 "source_msg_id": source_msg_id},
                {"_id": 0},
            ))
        except Exception as exc:
            logger.error(f"Error getting message mappings: {exc}")
            return []

    def delete_message_mappings(self, user_id: int, source_chat: str,
                                source_msg_id: int) -> bool:
        try:
            self._col("message_mappings").delete_many({
                "user_id": user_id, "source_chat": source_chat,
                "source_msg_id": source_msg_id,
            })
            return True
        except Exception as exc:
            return False

    # ─────────────────────────────────────────────
    #  Delete All User Data
    # ─────────────────────────────────────────────

    def delete_all_user_data(self, user_id: int) -> bool:
        """Permanently delete every record belonging to a user across all collections."""
        try:
            for col in [
                "users", "sessions", "destination_channels", "source_channels",
                "admins", "watch_config", "user_sources", "routes",
                "global_configs", "filters", "blacklists", "whitelists",
                "user_filters", "message_mappings",
            ]:
                try:
                    if col in ("destination_channels", "user_sources", "routes"):
                        self._col(col).delete_many({"user_id": user_id})
                    else:
                        self._col(col).delete_many({"user_id": user_id})
                except Exception:
                    pass
            logger.info(f"All data deleted for user {user_id}")
            return True
        except Exception as exc:
            logger.error(f"Error deleting user data: {exc}")
            return False