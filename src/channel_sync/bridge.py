import asyncio
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import time

from .bridge_store import BridgeStore, SCOPE
from .clients import ApiError
from .formatting import RichText, bale_to_telegram, convert, escape_bale
from .health import health
from .media import FileTooLarge, download_attachment, media_kind, stable_properties
from .messages import extract_message, own_author
from .moderation import Moderator
from .redaction import Redactor
from .reports import save_report
from .signatures import PayloadCodec, echo_signature
from .store import CircuitOpen

log = logging.getLogger(__name__)


class ObservationComplete(Exception):
    """Normal completion of an explicitly armed human-test observation."""


def _format_rejected(error: ApiError) -> bool:
    if error.code != 400 or error.uncertain:
        return False
    description = str(error).lower()
    return any(term in description for term in ("can't parse", "cannot parse", "parse entit", "entity", "entities", "markdown", "formatting", "parse_mode"))


class Bridge:
    def __init__(self, config, clients, store: BridgeStore, *, output: Path | None = None,
                 stop_on_error=False, new_posts_only=False, phase=None,
                 watch_duplicates=False, session_control: Path | None = None):
        self.config, self.clients, self.store = config, clients, store
        self.channel_ids = {"telegram": config.telegram_channel_id, "bale": config.bale_channel_id}
        self.enabled = {"telegram": config.telegram_to_bale_enabled, "bale": config.bale_to_telegram_enabled}
        self.bot_ids = {}
        self.codec = PayloadCodec(config)
        self.redactor = Redactor(config.telegram_token, config.bale_token, config.telegram_proxy_url,
                                 config.typesafe_api_key)
        self.moderator = Moderator(config, clients.get("telegram"), store)
        self.stop_on_error, self.new_posts_only, self.output = stop_on_error, new_posts_only, output
        self.ready = {platform: asyncio.Event() for platform in clients}
        self.wakeup, self.stop = asyncio.Event(), asyncio.Event()
        self.started = False
        self.watch_duplicates, self.session_control = watch_duplicates, session_control
        self.last_source_at = time.time()
        self.report = {"started_at": datetime.now(timezone.utc).isoformat(), "phase": phase,
                       "status": "starting", "dry_run": config.dry_run, "directions": self.enabled,
                       "updates": [], "events": [], "sends": [], "errors": []}

    def checkpoint(self):
        if not self.report["phase"]:
            # Normal 24/7 runs keep a bounded recent view; SQLite retains the audit.
            for key in ("updates", "events", "sends", "errors"):
                self.report[key] = self.report[key][-1000:]
        if self.output:
            save_report(self.output, self.report, self.redactor)

    def event(self, kind, **details):
        self.report["events"].append({"kind": kind, "at": time.time(), **details})
        self.checkpoint()

    def problem(self, direction, message):
        self.store.pause(direction, self.redactor.text(message))
        self.report["errors"].append({"direction": direction, "error": self.redactor.text(message)})
        self.checkpoint()
        log.error("%s paused: %s", direction, message)
        if self.stop_on_error:
            raise RuntimeError(message)

    def process_inbox(self, platform, *, baseline=False):
        for row in self.store.pending_updates(platform):
            update = self.codec.decode(json.loads(row["payload_json"]))
            message = extract_message(update, platform)
            if message and (message.get("chat", {}).get("id") != self.channel_ids[platform]
                            or message.get("chat", {}).get("type") != "channel"
                            or not isinstance(message.get("message_id"), int)):
                message = None
            signature = echo_signature(message, platform) if message else ""
            if self.watch_duplicates and message:
                # An API redelivery of the SAME message is harmless. A new ID
                # reusing a consumed bridge signature can be an extra echo/loop.
                # Identical human reposts within this TTL are indistinguishable;
                # use distinct labels during a supervised test.
                seen = self.store.connection.execute(
                    "SELECT 1 FROM bridge_seen WHERE platform=? AND chat_id=? AND message_id=?",
                    (platform, message["chat"]["id"], message["message_id"])).fetchone()
                matches = self.store.connection.execute(
                    "SELECT e.remaining FROM pending_echoes e JOIN sent_messages s USING(send_key) "
                    "WHERE e.platform=? AND e.chat_id=? AND e.signature=? AND e.expires_at>=? "
                    "AND s.status IN ('sending','sent','uncertain')",
                    (platform, message["chat"]["id"], signature, time.time())).fetchall()
                if not seen and matches and not any(r[0] for r in matches):
                    self.problem("tg_to_bale" if platform == "telegram" else "bale_to_tg",
                                 "Repeated bridge content with a new message ID: possible duplicate or loop")
            reason, key = self.store.route(
                platform=platform, update_id=row["update_id"], message=message,
                encoded_message=self.codec.encode(message), signature=signature,
                own=bool(message and own_author(message, self.bot_ids[platform])),
                enabled=self.enabled[platform], baseline=baseline, album_wait=self.config.album_wait,
            )
            self.event("incoming", platform=platform, update_id=row["update_id"],
                       message_id=message.get("message_id") if message else None, reason=reason, job_key=key)
            log.info("%s update %s: %s", platform, row["update_id"], reason)
            if reason == "queued":
                self.last_source_at = time.time()
            if reason == "late album item":
                self.problem("tg_to_bale" if platform == "telegram" else "bale_to_tg", "Late album item requires reconciliation")
        self.wakeup.set()

    async def moderate_inbox(self, platform):
        if not self.config.moderation_enabled or platform != "telegram":
            return
        for row in self.store.pending_updates(platform):
            update = self.codec.decode(json.loads(row["payload_json"]))
            # Moderation also covers discussion comments in a new-posts-only batch;
            # that option skips historical forwarding, not moderation.
            result = await self.moderator.handle(platform, update)
            if result is not None:
                self.store.mark_moderated(platform, row["update_id"])
                self.event("moderation", platform=platform, update_id=row["update_id"], **result)

    async def supervise(self):
        while not self.stop.is_set():
            if self.session_control and self.session_control.exists():
                control = json.loads(self.session_control.read_text())
                if control.get("finish_requested"):
                    seconds = float(control.get("observation_seconds", 180))
                    if not 1 <= seconds <= 3600:
                        raise ValueError("Observation duration must be 1–3600 seconds")
                    deadline = max(self.last_source_at, float(control["finish_requested_at"])) + seconds
                    if time.time() >= deadline:
                        self.event("observation_complete", deadline=deadline,
                                   last_source_at=self.last_source_at, observation_seconds=seconds)
                        raise ObservationComplete()
            await asyncio.sleep(0.25)

    async def poll(self, platform, client):
        first = True
        # New-posts-only is an explicit live-test option; normal run replays queued work.
        baseline = self.new_posts_only
        await self.moderate_inbox(platform)
        self.process_inbox(platform, baseline=False)
        while not self.stop.is_set():
            envelope = await client.get_updates(self.store.offset(platform, SCOPE), timeout=0 if first or baseline else None)
            updates = envelope["result"]
            if not isinstance(updates, list):
                raise RuntimeError(f"{platform} returned invalid updates")
            self.report["updates"].extend({"platform": platform, "at": time.time(), "update": update} for update in updates)
            self.checkpoint()
            self.store.ingest(platform, SCOPE, updates, encoder=self.codec.encode)
            await self.moderate_inbox(platform)
            self.process_inbox(platform, baseline=baseline)
            if not baseline or not updates:
                baseline = False
                if first:
                    self.ready[platform].set()
                    first = False
            if not updates:
                await asyncio.sleep(0.1)

    async def prepare_items(self, job):
        target = "bale" if job["source_platform"] == "telegram" else "telegram"
        items = []
        for row in self.store.items(job["job_key"]):
            message = self.codec.decode(json.loads(row["message_json"]))
            if message.get("batch_fixture"):
                raise RuntimeError("Interrupted live-batch fixture requires reconciliation using its batch report")
            body = message.get("text", message.get("caption", ""))
            entities = message.get("entities", message.get("caption_entities", []))
            try:
                rich = convert(body, entities, job["source_platform"], target)
            except ValueError:
                log.warning("%s invalid source entities; retaining plain text", job["job_key"])
                rich = RichText(escape_bale(body) if target == "bale" else body, [])
            plain = bale_to_telegram(body).text if job["source_platform"] == "bale" else body
            item = {"kind": media_kind(message), "text": rich.text, "entities": rich.entities,
                    "plain_text": plain, "source": {"platform": row["platform"], "chat_id": row["chat_id"],
                                                   "message_id": row["message_id"], "media_group_id": row["media_group_id"]}}
            if item["kind"] != "text":
                if self.config.dry_run:
                    item["properties"] = stable_properties(message)
                else:
                    try:
                        attachment = await download_attachment(self.clients[job["source_platform"]], message)
                    except FileTooLarge as exc:
                        self.event("skip_file", job_key=job["job_key"], message_id=row["message_id"], reason=str(exc))
                        log.warning("Skipping %s message %s: %s", row["platform"], row["message_id"], exc)
                        continue
                    item.update(content=attachment.content, filename=attachment.filename,
                                mime_type=attachment.mime_type, properties=attachment.properties)
            items.append(item)
        return target, items

    @staticmethod
    def expected_message(item):
        if item["kind"] == "text":
            return {"text": item["text"]}
        info = item["properties"]
        return {item["kind"]: [info] if item["kind"] == "photo" else info, "caption": item["text"]}

    async def transmit(self, key, target, items, *, job_key):
        existing = self.store.send(key)
        if existing:
            if existing["status"] == "sent":
                return json.loads(existing["response_json"])
            if existing["status"] in {"sending", "uncertain"}:
                raise ApiError(target, existing["method"], "Uncertain prior send; refusing replay", uncertain=True)
            if existing["status"] == "failed":
                error = json.loads(existing["error_json"])
                raise ApiError(target, existing["method"], error["error"], code=error.get("code"),
                               uncertain=error.get("uncertain", False), response=error.get("response"))
        signatures = [echo_signature(self.expected_message(item), target) for item in items]
        method = "sendMediaGroup" if len(items) > 1 else {"text": "sendMessage", "photo": "sendPhoto", "video": "sendVideo", "document": "sendDocument"}[items[0]["kind"]]
        if not existing:
            self.store.prepare_forward_send(send_key=key, platform=target, chat_id=self.channel_ids[target], method=method,
                                            request={"job_key": job_key, "items": [{k: v for k, v in item.items() if k != "content"} for item in items]},
                                            signatures=signatures, ttl=self.config.signature_ttl)
        self.store.claim_forward_send(key, self.config.signature_ttl)
        self.event("send_started", send_key=key, job_key=job_key, target=target, item_count=len(items))
        client = self.clients[target]
        previous = client.attempt_recorder

        def record(attempt):
            if attempt["method"] == method:
                self.store.record_send_attempt(key, attempt["attempt"], attempt)
                self.report["sends"].append({"send_key": key, "job_key": job_key, **attempt})
                self.checkpoint()
        client.attempt_recorder = record
        try:
            response = await client.send_content(self.channel_ids[target], items)
            result = response["result"] if isinstance(response["result"], list) else [response["result"]]
            if len(result) != len(items) or any(not isinstance(r, dict) or not isinstance(r.get("message_id"), int) for r in result):
                raise ApiError(target, method, "Success response has incomplete message mapping", uncertain=True, response=response)
            mappings = [(item["source"]["platform"], item["source"]["chat_id"], item["source"]["message_id"],
                         item["source"]["media_group_id"], target, self.channel_ids[target], message["message_id"])
                        for item, message in zip(items, result)]
        except ApiError as exc:
            self.store.finish_forward_send(key, status="uncertain" if exc.uncertain else "failed", error=exc.as_dict(), ttl=self.config.signature_ttl)
            raise
        except BaseException:
            self.store.finish_forward_send(key, status="uncertain", error={"error": "Interrupted during send", "uncertain": True}, ttl=self.config.signature_ttl)
            raise
        else:
            self.store.finish_forward_send(key, status="sent", response=response, mappings=mappings, ttl=self.config.signature_ttl)
            self.event("sent", send_key=key, job_key=job_key, target=target, message_ids=[r["message_id"] for r in result])
            return response
        finally:
            client.attempt_recorder = previous

    async def send_with_format_fallback(self, key, target, items, job_key):
        try:
            return await self.transmit(key, target, items, job_key=job_key)
        except ApiError as exc:
            if not _format_rejected(exc):
                raise
            log.warning("%s rejected formatting; trying plain text once", key)
            self.event("plain_fallback", job_key=job_key, send_key=key)
            plain = [{**item, "text": item["plain_text"], "entities": []} for item in items]
            return await self.transmit(key + ":plain", target, plain, job_key=job_key)

    async def forward(self, job):
        key = job["job_key"]
        self.store.set_job(key, "working")
        try:
            target, items = await self.prepare_items(job)
            if not items:
                self.store.set_job(key, "skipped", "All files exceed limits")
                return
            if self.config.dry_run:
                self.store.set_job(key, "dry_run")
                self.event("dry_run", job_key=key, target=target, item_count=len(items))
                return
            self.store.reserve_forward(key, job["direction"], len(items), window=self.config.circuit_window,
                                       maximum=self.config.circuit_max_sends)
            try:
                await self.send_with_format_fallback(key + ":primary", target, items, key)
            except ApiError as exc:
                if len(items) <= 1 or target != "bale" or exc.code != 400 or exc.uncertain or "UNSUPPORTED_FILE" not in str(exc).upper():
                    raise
                log.warning("%s Bale rejected album; sending its items separately once", key)
                self.event("album_fallback", job_key=key, item_count=len(items))
                for item in items:
                    await self.send_with_format_fallback(f"{key}:separate:{item['source']['message_id']}", target, [item], key)
            self.store.set_job(key, "sent")
            self.event("forwarded", job_key=key, direction=job["direction"], item_count=len(items))
        except CircuitOpen as exc:
            self.store.set_job(key, "pending")
            self.problem(job["direction"], str(exc))
        except ApiError as exc:
            self.store.set_job(key, "uncertain" if exc.uncertain else "failed", str(exc))
            self.problem(job["direction"], str(exc))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.store.set_job(key, "failed", self.redactor.text(str(exc)))
            self.problem(job["direction"], str(exc))

    async def work(self):
        await asyncio.gather(*(event.wait() for event in self.ready.values()))
        self.started = True
        self.report["status"] = "running"
        self.event("ready")
        log.info("Bridge ready: TG_TO_BALE=%s BALE_TO_TG=%s DRY_RUN=%s", self.enabled["telegram"], self.enabled["bale"], self.config.dry_run)
        directions = [direction for platform, direction in (("telegram", "tg_to_bale"), ("bale", "bale_to_tg")) if self.enabled[platform]]
        while not self.stop.is_set():
            self.wakeup.clear()
            job = self.store.next_job(directions)
            if job:
                await self.forward(job)
            else:
                try:
                    await asyncio.wait_for(self.wakeup.wait(), timeout=0.1)
                except TimeoutError:
                    pass

    async def run(self):
        tasks = []
        try:
            self.checkpoint()
            # A jev outage must not prevent polling/forwarding or violate fail-open.
            report = await health(self.clients, self.channel_ids, check_moderation=False)
            self.report["health"] = report
            self.checkpoint()
            if not report["ok"]:
                raise RuntimeError("Health check failed; see the run report")
            self.bot_ids = {item["platform"]: item["getMe"]["result"]["id"] for item in report["bots"]}
            telegram_health = next(item for item in report["bots"] if item["platform"] == "telegram")
            self.report["moderation"] = await self.moderator.prepare(telegram_health)
            self.store.recover_moderation()
            self.checkpoint()
            for platform, client in self.clients.items():
                webhook = await client.call("getWebhookInfo")
                if webhook["result"].get("url"):
                    raise RuntimeError(f"{platform} has an active webhook")
            self.store.recover_jobs()
            # An unresolved send blocks its direction even before polling resumes.
            for row in self.store.connection.execute("SELECT j.direction,s.send_key FROM bridge_jobs j JOIN sent_messages s ON s.send_key LIKE j.job_key || ':%' WHERE s.status='uncertain'"):
                self.problem(row["direction"], f"Uncertain send {row['send_key']} requires reconciliation")
            if self.stop_on_error and any(self.store.paused(d) for d in ("tg_to_bale", "bale_to_tg")):
                raise RuntimeError("A direction is paused; inspect status before live tests")
            tasks = [asyncio.create_task(self.poll(platform, client)) for platform, client in self.clients.items()]
            tasks.append(asyncio.create_task(self.work()))
            if self.session_control:
                tasks.append(asyncio.create_task(self.supervise()))
            await asyncio.gather(*tasks)
        except ObservationComplete:
            self.report["status"] = "stopped"
        except BaseException as exc:
            self.report["status"] = "stopped" if self.started else "startup_failed"
            if not isinstance(exc, asyncio.CancelledError):
                self.report["errors"].append({"error": self.redactor.text(str(exc))})
            raise
        finally:
            self.stop.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.moderator.aclose()
            self.report["finished_at"] = datetime.now(timezone.utc).isoformat()
            self.report["database"] = self.store.snapshot()
            self.checkpoint()
