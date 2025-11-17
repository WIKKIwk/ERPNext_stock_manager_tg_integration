from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional
import json


def _utcnow() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


class StockStorage:
    """Minimal SQLite storage for stock manager credentials."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        if self.db_path.parent and not self.db_path.parent.exists():
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._initialise()

    @contextmanager
    def _connection(self) -> Iterable[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _initialise(self) -> None:
        with self._connection() as conn:
            conn.executescript(
                """
                PRAGMA foreign_keys = ON;

                CREATE TABLE IF NOT EXISTS users (
                    telegram_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS credentials (
                    telegram_id INTEGER PRIMARY KEY,
                    api_key TEXT,
                    api_secret TEXT,
                    status TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (telegram_id) REFERENCES users(telegram_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS entry_drafts (
                    telegram_id INTEGER PRIMARY KEY,
                    payload TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (telegram_id) REFERENCES users(telegram_id) ON DELETE CASCADE
                );
                """
            )

    # --------------------------------------------- users / credentials
    def record_user(
        self,
        telegram_id: int,
        *,
        username: Optional[str],
        first_name: Optional[str],
        last_name: Optional[str],
    ) -> None:
        now = _utcnow()
        with self._lock, self._connection() as conn:
            row = conn.execute(
                "SELECT telegram_id FROM users WHERE telegram_id = ?",
                (telegram_id,),
            ).fetchone()
            if row:
                conn.execute(
                    """
                    UPDATE users
                    SET username = ?, first_name = ?, last_name = ?, updated_at = ?
                    WHERE telegram_id = ?
                    """,
                    (username, first_name, last_name, now, telegram_id),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO users (telegram_id, username, first_name, last_name, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (telegram_id, username, first_name, last_name, now, now),
                )

    def get_credentials(self, telegram_id: int) -> Optional[Dict[str, Optional[str]]]:
        with self._lock, self._connection() as conn:
            row = conn.execute(
                """
                SELECT telegram_id, api_key, api_secret, status
                FROM credentials
                WHERE telegram_id = ?
                """,
                (telegram_id,),
            ).fetchone()
            if not row:
                return None
            return {
                "telegram_id": row["telegram_id"],
                "api_key": row["api_key"],
                "api_secret": row["api_secret"],
                "status": row["status"],
            }

    def store_api_key(self, telegram_id: int, api_key: str) -> None:
        now = _utcnow()
        with self._lock, self._connection() as conn:
            conn.execute(
                """
                INSERT INTO credentials (telegram_id, api_key, api_secret, status, updated_at)
                VALUES (?, ?, NULL, 'pending_secret', ?)
                ON CONFLICT(telegram_id) DO UPDATE
                SET api_key = excluded.api_key,
                    status = 'pending_secret',
                    updated_at = excluded.updated_at
                """,
                (telegram_id, api_key, now),
            )

    def store_api_secret(self, telegram_id: int, api_secret: str, *, verified: bool) -> None:
        now = _utcnow()
        status = "active" if verified else "pending_secret"
        with self._lock, self._connection() as conn:
            conn.execute(
                """
                INSERT INTO credentials (telegram_id, api_key, api_secret, status, updated_at)
                VALUES (?, NULL, ?, ?, ?)
                ON CONFLICT(telegram_id) DO UPDATE
                SET api_secret = excluded.api_secret,
                    status = excluded.status,
                    updated_at = excluded.updated_at
                """,
                (telegram_id, api_secret, status, now),
            )

    def reset_credentials(self, telegram_id: int) -> None:
        now = _utcnow()
        with self._lock, self._connection() as conn:
            conn.execute(
                """
                UPDATE credentials
                SET api_key = NULL,
                    api_secret = NULL,
                    status = 'pending_key',
                    updated_at = ?
                WHERE telegram_id = ?
                """,
                (now, telegram_id),
            )

    # --------------------------------------------- entry drafts
    def get_entry_draft(self, telegram_id: int) -> Optional[Dict[str, Any]]:
        with self._lock, self._connection() as conn:
            row = conn.execute(
                "SELECT payload FROM entry_drafts WHERE telegram_id = ?",
                (telegram_id,),
            ).fetchone()
            if not row:
                return None
            try:
                return json.loads(row["payload"])
            except json.JSONDecodeError:
                return None

    def save_entry_draft(self, telegram_id: int, payload: Dict[str, Any]) -> None:
        now = _utcnow()
        data = json.dumps(payload)
        with self._lock, self._connection() as conn:
            conn.execute(
                """
                INSERT INTO entry_drafts (telegram_id, payload, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(telegram_id) DO UPDATE
                SET payload = excluded.payload,
                    updated_at = excluded.updated_at
                """,
                (telegram_id, data, now),
            )

    def delete_entry_draft(self, telegram_id: int) -> None:
        with self._lock, self._connection() as conn:
            conn.execute(
                "DELETE FROM entry_drafts WHERE telegram_id = ?",
                (telegram_id,),
            )
