import asyncio
import hashlib
import json
import logging
import random
import time

from ..jev import JevClient
from ..signatures import PayloadCodec
from ..persian import challenge_alert
from .approvals import Approvals
from .publisher import Publisher
from .reader import active_now, run_browser, tweet_url
from .research import LLM, research
from .mcp import load_servers
from .topics import find_duplicate, questions

log = logging.getLogger(__name__)


class Curator:
    def __init__(
        self,
        config,
        clients,
        store,
        *,
        jev=None,
        llm=None,
        researcher=research,
        browser=run_browser,
    ):
        self.config, self.clients, self.store = config, clients, store
        self.jev = jev or JevClient(config)
        self.llm = llm or LLM(config)
        self.researcher, self.browser = researcher, browser
        self.publisher = Publisher(config, clients, store)
        self.approvals = Approvals(config, clients, store, self.publisher)
        self.topic_lock = asyncio.Lock()
        self.stop = asyncio.Event()
        self.browser_task = None

    def setup_errors(self):
        errors = []
        if (
            not self.config.admin_telegram_user_ids
            or not self.config.admin_bale_user_ids
        ):
            errors.append("Fill both ADMIN_TELEGRAM_USER_IDS and ADMIN_BALE_USER_IDS")
        if not all(
            (self.config.llm_base_url, self.config.llm_api_key, self.config.llm_model)
        ):
            errors.append("Fill LLM_BASE_URL, LLM_API_KEY and LLM_MODEL")
        if not self.config.typesafe_api_key:
            errors.append("Fill TYPESAFE_API_KEY")
        if not self.config.mcp_servers_path.is_file():
            errors.append("Configure MCP_SERVERS_PATH")
        else:
            try:
                if not any(
                    s.get("enabled", True) for s in load_servers(self.config).values()
                ):
                    errors.append(
                        "Configure at least one enabled research server in mcp_servers.json"
                    )
            except Exception:
                errors.append(
                    "MCP server configuration or .env references are incomplete"
                )
        return errors

    def route_inbox(self, platform):
        codec = PayloadCodec(self.config)
        for row in self.store.pending_updates(platform):
            update = codec.decode(json.loads(row["payload_json"]))
            if (
                "callback_query" not in update
                and update.get("message", {}).get("chat", {}).get("type") != "private"
            ):
                continue
            with self.store.transaction():
                if self.approvals.authorized(platform, update) and self.store.action(
                    platform, row["update_id"]
                ):
                    self.store.enqueue(
                        "admin", {"platform": platform, "update": update}
                    )
                self.store.mark_moderated(platform, row["update_id"])

    async def event(self, event):
        kind = event["kind"]
        if kind == "challenge":
            self.store.pause_reader(event["reason"], challenge=True)
            self.store.connection.execute(
                "UPDATE curator_reader SET logged_in=0,checked_at=?", (time.time(),)
            )
            self.store.enqueue(
                "alert",
                {
                    "text": challenge_alert(event["reason"]),
                    "markup": {
                        "inline_keyboard": [
                            [{"text": "ادامهٔ خواندن", "callback_data": "c:resume"}]
                        ]
                    },
                },
            )
            return False
        if kind == "heartbeat":
            self.store.connection.execute(
                "UPDATE curator_reader SET logged_in=?,checked_at=?,heartbeat=?",
                (int(event["logged_in"]), time.time(), time.time()),
            )
        if kind == "post":
            if self.store.reader()["paused"]:
                return False
            self.store.add_post(event["post"], self.config)
            count = sum(self.store.stats(self.config)["today"].values())
            if count >= self.config.x_max_posts_per_day:
                return False
        if kind == "error":
            raise RuntimeError(event["reason"])
        return True

    async def read_loop(self):
        failures = 0
        while not self.stop.is_set():
            state = self.store.reader()
            today = sum(self.store.stats(self.config)["today"].values())
            if (
                self.setup_errors()
                or state["paused"]
                or not active_now(self.config)
                or today >= self.config.x_max_posts_per_day
                or state["next_run"] > time.time()
            ):
                await asyncio.sleep(2)
                continue
            self.browser_task = asyncio.create_task(
                self.browser(self.config, self.event)
            )
            try:
                # Parent independently enforces hours, quota and pause, even during a child hang.
                while not self.browser_task.done():
                    await asyncio.sleep(1)
                    if self.store.reader()["paused"] or not active_now(self.config):
                        self.browser_task.cancel()
                        break
                await self.browser_task
                failures = 0
                delay = (
                    random.uniform(
                        self.config.x_break_minutes_min, self.config.x_break_minutes_max
                    )
                    * 60
                )
            except asyncio.CancelledError:
                if self.stop.is_set() or asyncio.current_task().cancelling():
                    raise
                delay = 2
            except Exception as exc:
                failures += 1
                delay = min(1800, 15 * 2 ** min(failures - 1, 7)) + random.uniform(
                    0, 10
                )
                self.store.connection.execute(
                    "UPDATE curator_reader SET last_error=?",
                    (
                        f"Browser restart after {type(exc).__name__}; backoff {int(delay)} seconds",
                    ),
                )
                log.warning(
                    "Curator browser failed (%s); restarting in %.0fs",
                    type(exc).__name__,
                    delay,
                )
            finally:
                if self.browser_task and not self.browser_task.done():
                    self.browser_task.cancel()
                    await asyncio.gather(self.browser_task, return_exceptions=True)
                self.browser_task = None
            self.store.connection.execute(
                "UPDATE curator_reader SET next_run=?", (time.time() + delay,)
            )

    async def duplicate(self, topic, post):
        log.info("Curator duplicate skipped topic=%s post=%s", topic["id"], post["id"])
        self.store.post_done(post["id"], "duplicate")

    async def process_post(self, post):
        q = questions()
        scores = await self.jev.evaluate(
            {"post": post},
            {k: q[k] for k in ("is_ai_tech_news", "is_useful", "should_curate")},
        )
        log.info("Curator relevance post=%s scores=%s", post["id"], scores)
        self.store.post_done(post["id"], "working", scores)
        if scores["should_curate"] < self.config.curator_relevance_threshold:
            self.store.post_done(post["id"], "filtered", scores)
            return
        topic = await self.llm.topic(post)
        async with self.topic_lock:
            existing = await find_duplicate(self.store, self.jev, self.config, topic)
            if existing:
                self.store.add_key(existing["id"], topic)
                # Recover a research reservation owned by this post after a crash.
                own = self.store.connection.execute(
                    "SELECT COUNT(*) FROM curator_sources WHERE topic_id=? AND post_id=?",
                    (existing["id"], post["id"]),
                ).fetchone()[0]
                if existing["state"] == "researching" and own:
                    topic_id = existing["id"]
                else:
                    await self.duplicate(existing, post)
                    return
            else:
                topic_id, is_duplicate = self.store.reserve(
                    topic, post["id"], self.config.topic_dedup_days
                )
                if is_duplicate:
                    await self.duplicate(self.store.topic(topic_id), post)
                    return
        if await self.generate(topic_id):
            self.store.post_done(post["id"], "drafted", scores)

    async def generate(self, topic_id, instruction=""):
        posts = self.store.sources(topic_id)
        result = await self.researcher(
            self.config, self.llm, posts, instruction=instruction
        )
        refreshed = await self.llm.topic(
            {"posts": posts, "verified_facts": result["facts"]}
        )
        async with self.topic_lock:
            duplicate = await find_duplicate(
                self.store, self.jev, self.config, refreshed, exclude=topic_id
            )
            if duplicate:
                self.store.add_key(duplicate["id"], refreshed)
                self.store.merge(topic_id, duplicate["id"])
                for post in posts:
                    await self.duplicate(duplicate, post)
                return False
            self.store.save_draft(
                topic_id,
                refreshed,
                result,
                sources=posts,
            )
        log.info("Curator draft ready topic=%s", topic_id)
        return True

    async def test_input(self, value):
        try:
            url, ident = tweet_url(value)
        except ValueError:
            ident = "text:" + hashlib.sha256(value.encode()).hexdigest()
            # Plain text has no original tweet; an explicitly labelled local source
            # is retained, while the writer only requires tweet URLs when present.
            post = {
                "id": ident,
                "text": value,
                "url": "",
                "author": "manual",
                "created_at": None,
                "media": [],
                "external_links": [],
            }
            self.store.add_post(post, self.config, test=True)
            return
        if self.store.connection.execute(
            "SELECT 1 FROM curator_posts WHERE id=?", (ident,)
        ).fetchone():
            return
        if self.store.reader()["challenge"]:
            raise ValueError(
                "X reader challenge is latched; resolve manually and resume first"
            )

        async def callback(event):
            if event["kind"] == "post":
                if event["post"]["id"] == ident:
                    self.store.add_post(event["post"], self.config, test=True)
                    return False
                return True
            return await self.event(event)

        await self.browser(self.config, callback, mode="tweet", url=url)
        if not self.store.connection.execute(
            "SELECT 1 FROM curator_posts WHERE id=?", (ident,)
        ).fetchone():
            raise ValueError("Requested X post was not visible; no draft was created")

    async def pipeline_once(self):
        task = self.store.connection.execute(
            "SELECT * FROM curator_tasks WHERE kind IN ('input','rewrite') AND state='queued' ORDER BY id LIMIT 1"
        ).fetchone()
        if task:
            self.store.connection.execute(
                "UPDATE curator_tasks SET state='working' WHERE id=?", (task["id"],)
            )
            payload = json.loads(task["payload_json"])
            try:
                if task["kind"] == "input":
                    await self.test_input(payload["value"])
                else:
                    await self.generate(
                        payload["topic_id"],
                        payload.get("instruction", ""),
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                error = self.store.redactor.text(str(exc))
                self.store.connection.execute(
                    "UPDATE curator_tasks SET state='error',error=? WHERE id=?",
                    (error, task["id"]),
                )
                if task["kind"] == "rewrite":
                    self.store.connection.execute(
                        "UPDATE curator_topics SET state='error' WHERE id=? AND state='researching'",
                        (payload["topic_id"],),
                    )
                    self.store.dirty(payload["topic_id"])
                log.warning("Curator task failed id=%s error=%s", task["id"], error)
                await self.approvals.alert(
                    "خطا در انجام درخواست رخ داد؛ جزئیات در گزارش سرویس ثبت شده است."
                )
            else:
                self.store.connection.execute(
                    "UPDATE curator_tasks SET state='done' WHERE id=?", (task["id"],)
                )
            return True
        row = self.store.connection.execute(
            "SELECT * FROM curator_posts WHERE state='queued' ORDER BY created_at LIMIT 1"
        ).fetchone()
        if not row:
            return False
        self.store.connection.execute(
            "UPDATE curator_posts SET state='working' WHERE id=?", (row["id"],)
        )
        try:
            await self.process_post(json.loads(row["payload_json"]))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            error = self.store.redactor.text(str(exc))
            self.store.post_done(row["id"], "error", error=error)
            topics = self.store.connection.execute(
                "SELECT t.id FROM curator_topics t JOIN curator_sources s ON s.topic_id=t.id WHERE s.post_id=? AND t.state='researching'",
                (row["id"],),
            ).fetchall()
            for item in topics:
                self.store.connection.execute(
                    "UPDATE curator_topics SET state='error' WHERE id=?", (item[0],)
                )
                self.store.dirty(item[0])
            log.warning("Curator post failed id=%s error=%s", row["id"], error)
            await self.approvals.alert(
                "تهیهٔ پیش‌نویس ناموفق بود؛ جزئیات در گزارش سرویس ثبت شده است."
            )
        return True

    async def pipeline_loop(self):
        while not self.stop.is_set():
            if self.setup_errors() or not await self.pipeline_once():
                await asyncio.sleep(1)

    async def admin_once(self):
        row = self.store.connection.execute(
            "SELECT * FROM curator_tasks WHERE kind IN ('admin','alert') AND state='queued' ORDER BY id LIMIT 1"
        ).fetchone()
        if not row:
            return False
        # Local decisions use CAS; ambiguous remote prompts are not replayed.
        self.store.connection.execute(
            "UPDATE curator_tasks SET state='done' WHERE id=?", (row["id"],)
        )
        payload = json.loads(row["payload_json"])
        try:
            if row["kind"] == "alert":
                await self.approvals.alert(payload["text"], payload.get("markup"))
            else:
                await self.approvals.handle(payload["platform"], payload["update"])
        except Exception as exc:
            log.warning("Curator admin action failed (%s)", type(exc).__name__)
            if row["kind"] == "admin":
                update = payload["update"]
                if self.approvals.authorized(payload["platform"], update):
                    message = (
                        update.get("callback_query", {}).get("message")
                        or update["message"]
                    )
                    await self.approvals.message(
                        payload["platform"],
                        message["chat"]["id"],
                        "خطا در انجام این اقدام رخ داد؛ دوباره تلاش کنید.",
                    )
        return True

    async def admin_loop(self):
        while not self.stop.is_set():
            if not await self.admin_once():
                await asyncio.sleep(0.2)

    async def outbox_once(self):
        publications = self.store.connection.execute(
            "SELECT topic_id,platform FROM curator_publications WHERE state='queued'"
        ).fetchall()
        for row in publications:
            await self.publisher.publish(
                row["topic_id"], row["platform"], retry_failed=True
            )
        topics = self.store.connection.execute(
            "SELECT t.id FROM curator_topics t WHERE t.state NOT IN ('researching','merged') AND "
            "(EXISTS(SELECT 1 FROM curator_previews p WHERE p.topic_id=t.id AND p.dirty=1) OR "
            "(SELECT COUNT(*) FROM curator_previews p WHERE p.topic_id=t.id)<?)",
            (
                len(self.config.admin_telegram_user_ids)
                + len(self.config.admin_bale_user_ids),
            ),
        ).fetchall()
        for row in topics:
            await self.approvals.sync_topic(row[0])

    async def outbox_loop(self):
        while not self.stop.is_set():
            await self.outbox_once()
            await asyncio.sleep(5)

    async def supervise(self, label, operation):
        while not self.stop.is_set():
            try:
                await operation()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error(
                    "Curator %s task failed (%s); bridge continues",
                    label,
                    type(exc).__name__,
                )
                await asyncio.sleep(10)

    async def run(self):
        self.store.recover()
        errors = self.setup_errors()
        if errors:
            log.warning("Curator setup incomplete: %s", "; ".join(errors))
        tasks = [
            asyncio.create_task(self.supervise(label, operation))
            for label, operation in (
                ("reader", self.read_loop),
                ("pipeline", self.pipeline_loop),
                ("approvals", self.admin_loop),
                ("outbox", self.outbox_loop),
            )
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            self.stop.set()
            for task in tasks:
                task.cancel()
            if self.browser_task:
                self.browser_task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.aclose()

    async def aclose(self):
        await asyncio.gather(
            self.jev.aclose(), self.llm.aclose(), self.publisher.aclose()
        )
