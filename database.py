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