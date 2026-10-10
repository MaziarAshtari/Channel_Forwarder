import json
import time

from .store import CircuitOpen, Store

SCOPE = "sync"


class BridgeStore(Store):
    def pending_updates(self, platform):
        return self.connection.execute(
            "SELECT p.update_id,i.payload_json FROM processed_updates p JOIN inbox_payloads i "
            "USING(platform,scope,update_id) WHERE p.platform=? AND p.scope=? AND p.status='received' "
            "ORDER BY p.update_id", (platform, SCOPE),
        ).fetchall()

    def route(self, *, platform, update_id, message, encoded_message, signature, own, enabled,
              baseline=False, album_wait=3, now=None):
        now = time.time() if now is None else now
        direction = "tg_to_bale" if platform == "telegram" else "bale_to_tg"
        with self.transaction() as db:
            row = db.execute("SELECT status FROM processed_updates WHERE platform=? AND scope=? AND update_id=?",
                             (platform, SCOPE, update_id)).fetchone()
            if not row or row[0] != "received":
                return "duplicate update", None
            reason = "not a channel post"
            job_key = None
            if message is not None:
                chat_id, message_id = message["chat"]["id"], message["message_id"]
                seen = db.execute("SELECT reason FROM bridge_seen WHERE platform=? AND chat_id=? AND message_id=?",
                                  (platform, chat_id, message_id)).fetchone()
                if seen:
                    reason = "duplicate message"
                else:
                    match = db.execute(
                        "SELECT e.send_key,e.item_index FROM pending_echoes e JOIN sent_messages s USING(send_key) "
                        "WHERE e.platform=? AND e.chat_id=? AND e.signature=? AND e.remaining=1 "
                        "AND e.expires_at>=? AND s.status IN ('sending','sent','uncertain') "
                        "ORDER BY s.created_at,e.item_index LIMIT 1", (platform, chat_id, signature, now),
                    ).fetchone()
                    if match:
                        db.execute("UPDATE pending_echoes SET remaining=0 WHERE send_key=? AND item_index=?", tuple(match))
                    reason = "own bot" if own else "echo signature" if match else "startup baseline" if baseline else "direction disabled" if not enabled else "queued"
                    if reason == "queued":
                        group = str(message.get("media_group_id") or "")
                        suffix = f"album:{group}" if group else f"message:{message_id}"
                        job_key = f"{platform}:{chat_id}:{suffix}"
                        existing = db.execute("SELECT state FROM bridge_jobs WHERE job_key=?", (job_key,)).fetchone()
                        if existing and existing[0] != "pending":
                            reason = "late album item"
                            db.execute("UPDATE direction_circuits SET paused=1,reason=?,paused_at=? WHERE direction=?",
                                       ("Album item arrived after forwarding began", now, direction))
                        else:
                            db.execute("INSERT INTO bridge_jobs(job_key,source_platform,direction,created_at,due_at) "
                                       "VALUES(?,?,?,?,?) ON CONFLICT(job_key) DO UPDATE SET due_at=excluded.due_at",
                                       (job_key, platform, direction, now, now + album_wait if group else now))
                            db.execute("INSERT INTO bridge_items VALUES(?,?,?,?,?,?,?)",
                                       (platform, chat_id, message_id, update_id, group or None, job_key, self.dumps(encoded_message)))
                    db.execute("INSERT INTO bridge_seen VALUES(?,?,?,?,?)", (platform, chat_id, message_id, update_id, reason))
            db.execute("UPDATE processed_updates SET status=? WHERE platform=? AND scope=? AND update_id=?",
                       ("processed" if reason == "queued" else "ignored", platform, SCOPE, update_id))
        return reason, job_key

    def next_job(self, enabled_directions, now=None):
        now = time.time() if now is None else now
        candidates = []
        for direction in enabled_directions:
            if self.paused(direction):
                continue
            row = self.connection.execute("SELECT * FROM bridge_jobs WHERE direction=? AND state='pending' "
                                          "ORDER BY created_at,job_key LIMIT 1", (direction,)).fetchone()
            if row and row["due_at"] <= now:
                candidates.append(row)
        return min(candidates, key=lambda r: (r["created_at"], r["job_key"])) if candidates else None

    def items(self, key):
        return self.connection.execute("SELECT * FROM bridge_items WHERE job_key=? ORDER BY message_id", (key,)).fetchall()

    def set_job(self, key, state, detail=None):
        self.connection.execute("UPDATE bridge_jobs SET state=?,detail=?,finished_at=? WHERE job_key=?",
                                (state, detail, None if state in {"pending", "working"} else time.time(), key))

    def paused(self, direction):
        return bool(self.connection.execute("SELECT paused FROM direction_circuits WHERE direction=?", (direction,)).fetchone()[0])

    def pause(self, direction, reason):
        self.connection.execute("UPDATE direction_circuits SET paused=1,reason=?,paused_at=? WHERE direction=?",
                                (reason, time.time(), direction))

    def reserve_forward(self, key, direction, count, *, window, maximum, now=None):
        now = time.time() if now is None else now
        blocked = False
        with self.transaction() as db:
            if db.execute("SELECT paused FROM direction_circuits WHERE direction=?", (direction,)).fetchone()[0]:
                blocked = True
            elif not db.execute("SELECT 1 FROM forward_events WHERE job_key=?", (key,)).fetchone():
                total = db.execute("SELECT COALESCE(SUM(item_count),0) FROM forward_events WHERE direction=? AND occurred_at>=?",
                                   (direction, now - window)).fetchone()[0]
                if total + count > maximum:
                    db.execute("UPDATE direction_circuits SET paused=1,reason=?,paused_at=? WHERE direction=?",
                               (f"Forward rate would exceed {maximum} items per {window}s", now, direction))
                    blocked = True
                else:
                    db.execute("INSERT INTO forward_events VALUES(?,?,?,?)", (key, direction, now, count))
        if blocked:
            raise CircuitOpen(f"{direction} paused by circuit breaker")

    def prepare_forward_send(self, *, send_key, platform, chat_id, method, request, signatures, ttl):
        # Each identical album item gets its own count, rather than a set of hashes.
        now = time.time()
        with self.transaction() as db:
            db.execute("INSERT INTO sent_messages(send_key,platform,chat_id,method,request_json,signature,"
                       "signature_expires_at,status,created_at) VALUES(?,?,?,?,?,?,?,'prepared',?)",
                       (send_key, platform, chat_id, method, self.dumps(request), signatures[0], now + ttl, now))
            db.executemany("INSERT INTO pending_echoes VALUES(?,?,?,?,?,1,?)",
                           [(send_key, index, platform, chat_id, signature, now + ttl) for index, signature in enumerate(signatures)])

    def claim_forward_send(self, key, ttl):
        with self.transaction() as db:
            if db.execute("UPDATE sent_messages SET status='sending',attempted_at=? WHERE send_key=? AND status='prepared'",
                          (time.time(), key)).rowcount != 1:
                raise ValueError("Send already attempted; refusing duplicate")
            db.execute("UPDATE pending_echoes SET expires_at=? WHERE send_key=?", (time.time() + ttl, key))

    def finish_forward_send(self, key, *, status, response=None, error=None, mappings=None, ttl=600):
        with self.transaction() as db:
            if db.execute("UPDATE sent_messages SET status=?,response_json=?,error_json=? WHERE send_key=? AND status='sending'",
                          (status, self.dumps(response) if response is not None else None,
                           self.dumps(error) if error is not None else None, key)).rowcount != 1:
                raise ValueError("Send not in progress")
            db.execute("UPDATE pending_echoes SET expires_at=?,remaining=CASE WHEN ?='failed' THEN 0 ELSE remaining END WHERE send_key=?",
                       (time.time() + ttl, status, key))
            for mapping in mappings or []:
                db.execute("INSERT INTO message_mapping VALUES(?,?,?,?,?,?,?,?)", (*mapping, key))

    def send(self, key):
        return self.connection.execute("SELECT * FROM sent_messages WHERE send_key=?", (key,)).fetchone()

    def recover_jobs(self):
        self.recover_uncertain()
        self.connection.execute("UPDATE bridge_jobs SET state='pending' WHERE state='working'")

    def snapshot(self):
        return {"jobs": list(reversed([dict(row) for row in self.connection.execute("SELECT * FROM bridge_jobs ORDER BY created_at DESC,job_key DESC LIMIT 1000")])),
                "job_counts": dict(self.connection.execute("SELECT state,COUNT(*) FROM bridge_jobs GROUP BY state")),
                "circuits": [dict(row) for row in self.connection.execute("SELECT * FROM direction_circuits")],
                "pending_echo_items": self.connection.execute("SELECT COALESCE(SUM(remaining),0) FROM pending_echoes WHERE expires_at>=?", (time.time(),)).fetchone()[0]}
