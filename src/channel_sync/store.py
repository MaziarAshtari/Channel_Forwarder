from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
import time

from .redaction import Redactor


class CircuitOpen(RuntimeError):
    pass


class Store:
    """Small synchronous transactions; never hold a transaction across an await."""

    def __init__(self, path: Path, redactor: Redactor | None = None):
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.redactor = redactor or Redactor()
        self.connection = sqlite3.connect(path, isolation_level=None, timeout=5)
        path.chmod(0o600)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.executescript(Path(__file__).with_name("schema.sql").read_text())

    def close(self):
        self.connection.close()

    def dumps(self, value):
        return json.dumps(self.redactor.data(value), ensure_ascii=False, sort_keys=True)

    @contextmanager
    def transaction(self):
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield self.connection
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise
        else:
            self.connection.execute("COMMIT")

    def offset(self, platform: str, scope: str) -> int | None:
        row = self.connection.execute(
            "SELECT next_offset FROM poll_offsets WHERE platform=? AND scope=?",
            (platform, scope),
        ).fetchone()
        return row[0] if row else None

    def ingest(self, platform: str, scope: str, updates: list[dict], *, encoder=None) -> int:
        """Durably store a whole batch BEFORE acknowledging it with the next poll."""
        if not updates:
            return 0
        inserted = 0
        with self.transaction() as db:
            for update in updates:
                update_id = update["update_id"]
                if not isinstance(update_id, int) or isinstance(update_id, bool) or update_id < 0:
                    raise ValueError("Invalid update_id; batch not acknowledged.")
                cursor = db.execute(
                    "INSERT OR IGNORE INTO processed_updates "
                    "(platform,scope,update_id,raw_json,received_at) VALUES (?,?,?,?,?)",
                    (platform, scope, update_id, self.dumps(update), time.time()),
                )
                inserted += cursor.rowcount
                if encoder is not None:
                    db.execute("INSERT OR IGNORE INTO inbox_payloads VALUES (?,?,?,?)",
                               (platform, scope, update_id, self.dumps(encoder(update))))
            next_offset = max(u["update_id"] for u in updates) + 1
            db.execute(
                "INSERT INTO poll_offsets VALUES (?,?,?) ON CONFLICT(platform,scope) "
                "DO UPDATE SET next_offset=MAX(next_offset,excluded.next_offset)",
                (platform, scope, next_offset),
            )
        return inserted

    def prepare_send(self, *, send_key: str, platform: str, chat_id: int, method: str,
                     request: dict, signatures: list[str], ttl: int, now: float | None = None):
        now = time.time() if now is None else now
        if not signatures:
            raise ValueError("A send requires at least one content signature.")
        with self.transaction() as db:
            db.execute(
                "INSERT INTO sent_messages (send_key,platform,chat_id,method,request_json,"
                "signature,signature_expires_at,status,created_at) VALUES (?,?,?,?,?,?,?,'prepared',?)",
                (send_key, platform, chat_id, method, self.dumps(request), signatures[0], now + ttl, now),
            )
            db.executemany("INSERT INTO sent_signatures VALUES (?,?,?)",
                           [(send_key, sig, now + ttl) for sig in set(signatures)])

    def claim_send(self, send_key: str, *, window: int, max_sends: int,
                   ttl: int, now: float | None = None):
        now = time.time() if now is None else now
        blocked = False
        with self.transaction() as db:
            if db.execute("SELECT paused FROM circuit_state WHERE singleton=1").fetchone()[0]:
                blocked = True
            elif db.execute(
                "SELECT COUNT(*) FROM sent_messages WHERE attempted_at>=? "
                "AND status IN ('sending','sent','uncertain')", (now - window,),
            ).fetchone()[0] >= max_sends:
                db.execute("UPDATE circuit_state SET paused=1,reason=?,paused_at=? WHERE singleton=1",
                           (f"At least {max_sends} sends in {window} seconds", now))
                blocked = True
            else:
                changed = db.execute(
                    "UPDATE sent_messages SET status='sending',attempted_at=?,signature_expires_at=? "
                    "WHERE send_key=? AND status='prepared'", (now, now + ttl, send_key),
                ).rowcount
                if changed != 1:
                    raise ValueError("Send already claimed or completed; refusing to resend.")
                db.execute("UPDATE sent_signatures SET expires_at=? WHERE send_key=?",
                           (now + ttl, send_key))
        if blocked:
            raise CircuitOpen("Circuit breaker paused sends; inspect the database before resuming.")

    def finish_send(self, send_key: str, *, status: str, response=None, error=None,
                    mappings: list[dict] | None = None):
        if status not in {"sent", "failed", "uncertain"}:
            raise ValueError("Invalid final send state.")
        with self.transaction() as db:
            changed = db.execute(
                "UPDATE sent_messages SET status=?,response_json=?,error_json=? "
                "WHERE send_key=? AND status='sending'",
                (status, self.dumps(response) if response is not None else None,
                 self.dumps(error) if error is not None else None, send_key),
            ).rowcount
            if changed != 1:
                raise ValueError("Send is not in progress.")
            for mapping in mappings or []:
                db.execute(
                    "INSERT INTO message_mapping VALUES (?,?,?,?,?,?,?,?)",
                    (mapping["source_platform"], mapping["source_chat_id"],
                     mapping["source_message_id"], mapping.get("source_media_group_id"),
                     mapping["target_platform"], mapping["target_chat_id"],
                     mapping["target_message_id"], send_key),
                )

    def recover_uncertain(self) -> int:
        # Call only after acquiring the single-process lock.
        return self.connection.execute(
            "UPDATE sent_messages SET status='uncertain' WHERE status='sending'"
        ).rowcount

    def record_send_attempt(self, send_key: str, attempt: int, response: dict):
        self.connection.execute(
            "INSERT INTO send_attempts VALUES (?,?,?,?)",
            (send_key, attempt, time.time(), self.dumps(response)),
        )

    def matches_sent(self, platform: str, chat_id: int, signature: str,
                     now: float | None = None) -> bool:
        now = time.time() if now is None else now
        return self.connection.execute(
            "SELECT 1 FROM sent_signatures s JOIN sent_messages m USING(send_key) "
            "WHERE m.platform=? AND m.chat_id=? AND s.signature=? AND s.expires_at>=? "
            "AND m.status IN ('sending','sent','uncertain') LIMIT 1",
            (platform, chat_id, signature, now),
        ).fetchone() is not None
