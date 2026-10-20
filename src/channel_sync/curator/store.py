import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from ..bridge_store import BridgeStore


class CuratorStore(BridgeStore):
    def day(self, timezone="Asia/Tehran"):
        return datetime.now(ZoneInfo(timezone)).date().isoformat()

    def reader(self):
        return dict(self.connection.execute("SELECT * FROM curator_reader").fetchone())

    def pause_reader(self, reason, *, challenge=False):
        self.connection.execute(
            "UPDATE curator_reader SET paused=1,challenge=MAX(challenge,?),reason=?",
            (int(challenge), reason),
        )

    def resume_reader(self):
        self.connection.execute(
            "UPDATE curator_reader SET paused=0,challenge=0,reason=NULL,next_run=0,last_error=NULL"
        )

    def add_post(self, post, config, *, test=False):
        now = time.time()
        day = self.day(config.x_timezone)
        with self.transaction() as db:
            count = db.execute(
                "SELECT COUNT(*) FROM curator_posts WHERE day=? AND state!='quota'",
                (day,),
            ).fetchone()[0]
            state = "queued" if test or count < config.x_max_posts_per_day else "quota"
            inserted = db.execute(
                "INSERT OR IGNORE INTO curator_posts(id,payload_json,state,day,created_at) VALUES(?,?,?,?,?)",
                (post["id"], self.dumps(post), state, day, now),
            ).rowcount
        return bool(inserted and state == "queued")

    def post_done(self, post_id, state, scores=None, error=None):
        self.connection.execute(
            "UPDATE curator_posts SET state=?,scores_json=COALESCE(?,scores_json),error=? WHERE id=?",
            (state, self.dumps(scores) if scores is not None else None, error, post_id),
        )

    def topic(self, topic_id):
        row = self.connection.execute(
            "SELECT * FROM curator_topics WHERE id=?", (topic_id,)
        ).fetchone()
        return dict(row) if row else None

    def topics(self, days, *, exclude=None):
        return [
            dict(r)
            for r in self.connection.execute(
                "SELECT * FROM curator_topics WHERE merged_into IS NULL AND created_at>=? AND id!=? ORDER BY created_at DESC",
                (time.time() - days * 86400, exclude or -1),
            )
        ]

    def exact(self, key, days, exclude=None):
        row = self.connection.execute(
            "SELECT t.* FROM curator_topic_keys k JOIN curator_topics t ON t.id=k.topic_id "
            "WHERE k.topic_key=? AND t.merged_into IS NULL AND t.id!=? "
            "AND (k.permanent=1 OR t.created_at>=?) ORDER BY t.created_at DESC LIMIT 1",
            (key, exclude or -1, time.time() - days * 86400),
        ).fetchone()
        return dict(row) if row else None

    def add_key(self, topic_id, topic):
        self.connection.execute(
            "INSERT INTO curator_topic_keys(topic_key,topic_id,permanent,created_at) VALUES(?,?,?,?) "
            "ON CONFLICT(topic_key,topic_id) DO UPDATE SET permanent=MAX(permanent,excluded.permanent)",
            (
                topic["topic_key"],
                topic_id,
                int(topic["permanent"]),
                self.topic(topic_id)["created_at"],
            ),
        )

    def reserve(self, topic, post_id, days):
        now = time.time()
        with self.transaction() as db:
            existing = self.exact(topic["topic_key"], days)
            if existing:
                topic_id = existing["id"]
            else:
                topic_id = db.execute(
                    "INSERT INTO curator_topics(topic_key,permanent,entity,event,summary,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (
                        topic["topic_key"],
                        int(topic["permanent"]),
                        topic["entity"],
                        topic["event"],
                        topic["summary"],
                        now,
                        now,
                    ),
                ).lastrowid
            if not existing:
                db.execute(
                    "INSERT OR IGNORE INTO curator_sources VALUES(?,?)",
                    (topic_id, post_id),
                )
            self.add_key(topic_id, topic)
        return topic_id, bool(existing)

    def attach(self, topic_id, post_id):
        return (
            self.connection.execute(
                "INSERT OR IGNORE INTO curator_sources VALUES(?,?)", (topic_id, post_id)
            ).rowcount
            == 1
        )

    def sources(self, topic_id):
        return [
            json.loads(r[0])
            for r in self.connection.execute(
                "SELECT p.payload_json FROM curator_sources s JOIN curator_posts p ON p.id=s.post_id WHERE s.topic_id=? ORDER BY p.created_at",
                (topic_id,),
            )
        ]

    def dirty(self, topic_id):
        self.connection.execute(
            "UPDATE curator_previews SET dirty=1 WHERE topic_id=?", (topic_id,)
        )

    def draft_sources(self, topic_id):
        research = json.loads(self.topic(topic_id)["research_json"])
        return (
            research["posts"]
            if isinstance(research.get("posts"), list)
            else self.sources(topic_id)
        )

    def save_draft(self, topic_id, topic, result, *, sources=None, state="pending"):
        if state not in {"pending", "published"}:
            raise ValueError("Invalid generated draft state")
        result = {
            **result,
            "posts": self.sources(topic_id) if sources is None else sources,
        }
        with self.transaction() as db:
            self.add_key(topic_id, topic)
            db.execute(
                "UPDATE curator_topics SET topic_key=?,permanent=?,entity=?,event=?,summary=?,text=?,research_json=?,"
                "state=?,revision=revision+1,updated_at=? WHERE id=? AND state IN ('researching','pending','rejected','error')",
                (
                    topic["topic_key"],
                    int(topic["permanent"]),
                    topic["entity"],
                    topic["event"],
                    topic["summary"],
                    result["text"],
                    self.dumps(result),
                    state,
                    time.time(),
                    topic_id,
                ),
            )
            self.dirty(topic_id)

    def merge(self, source, target):
        with self.transaction() as db:
            for row in db.execute(
                "SELECT topic_key,permanent FROM curator_topic_keys WHERE topic_id=?",
                (source,),
            ).fetchall():
                self.add_key(target, dict(row))
            db.execute("DELETE FROM curator_topic_keys WHERE topic_id=?", (source,))
            db.execute(
                "UPDATE curator_topics SET state='merged',merged_into=?,updated_at=? WHERE id=?",
                (target, time.time(), source),
            )
            self.dirty(source)

    def enqueue(self, kind, payload):
        return self.connection.execute(
            "INSERT INTO curator_tasks(kind,payload_json,created_at) VALUES(?,?,?)",
            (kind, self.dumps(payload), time.time()),
        ).lastrowid

    def action(self, platform, update_id):
        return (
            self.connection.execute(
                "INSERT OR IGNORE INTO curator_actions VALUES(?,?,?)",
                (platform, update_id, time.time()),
            ).rowcount
            == 1
        )

    def decide(self, topic_id, revision, action, actor):
        with self.transaction() as db:
            topic = self.topic(topic_id)
            if (
                not topic
                or topic["revision"] != revision
                or topic["state"] != "pending"
            ):
                return False
            if action == "approve":
                db.execute(
                    "UPDATE curator_topics SET state='approved',actor=?,updated_at=? WHERE id=?",
                    (actor, time.time(), topic_id),
                )
                snapshot = self.dumps({**topic, "posts": self.draft_sources(topic_id)})
                db.executemany(
                    "INSERT OR IGNORE INTO curator_publications(topic_id,platform,snapshot_json) VALUES(?,?,?)",
                    [(topic_id, p, snapshot) for p in ("telegram", "bale")],
                )
            elif action == "reject":
                db.execute(
                    "UPDATE curator_topics SET state='rejected',actor=?,updated_at=? WHERE id=?",
                    (actor, time.time(), topic_id),
                )
            else:
                return False
            self.dirty(topic_id)
        return True

    def recover(self):
        # Removed controls must not be revived by a restart or old bot messages.
        self.connection.execute(
            "DELETE FROM curator_replies WHERE mode NOT IN ('menu','replace','rewrite')"
        )
        for row in self.connection.execute(
            "SELECT payload_json FROM curator_tasks WHERE kind='regenerate' AND state IN ('queued','working')"
        ).fetchall():
            payload = json.loads(row[0])
            self.connection.execute(
                "UPDATE curator_topics SET state=?,revision=revision+1 WHERE id=? AND state='researching'",
                (
                    "published" if payload.get("preview_only") else "pending",
                    payload["topic_id"],
                ),
            )
        self.connection.execute(
            "UPDATE curator_tasks SET state='cancelled' WHERE kind='regenerate' AND state IN ('queued','working')"
        )
        # Refresh persisted English controls and removed buttons on upgrade.
        self.connection.execute("UPDATE curator_previews SET dirty=1")
        self.connection.execute(
            "UPDATE curator_tasks SET state='queued' WHERE state='working'"
        )
        self.connection.execute(
            "UPDATE curator_posts SET state='queued' WHERE state='working'"
        )
        # A topic reserved before a crash is resumed through its original source.
        self.connection.execute(
            "UPDATE curator_publications SET state='queued' WHERE state='publishing'"
        )
        self.connection.execute(
            "UPDATE sent_messages SET status='uncertain' WHERE send_key LIKE 'curator:%' AND status='sending'"
        )

    def stats(self, config):
        return {
            "reader": self.reader(),
            "day": self.day(config.x_timezone),
            "today": dict(
                self.connection.execute(
                    "SELECT state,COUNT(*) FROM curator_posts WHERE day=? GROUP BY state",
                    (self.day(config.x_timezone),),
                )
            ),
            "topics": dict(
                self.connection.execute(
                    "SELECT state,COUNT(*) FROM curator_topics GROUP BY state"
                )
            ),
            "publications": dict(
                self.connection.execute(
                    "SELECT state,COUNT(*) FROM curator_publications GROUP BY state"
                )
            ),
        }
