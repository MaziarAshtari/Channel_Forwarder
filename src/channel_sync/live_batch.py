"""Authorized bot-published integration fixtures; normal routing stays unchanged.

Bot-authored source posts cannot exercise human-post admission. Explicit durable
test jobs use actual send-response message IDs, without forging raw updates or
disabling own-bot filtering. Missing Bale album attachment metadata is reported.
"""
import asyncio
from dataclasses import replace
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import time

from .bridge import Bridge
from .clients import ApiError
from .formatting import convert, telegram_to_bale
from .media import download_attachment
from .messages import extract_message, utf16_length
from .reports import save_report
from .signatures import echo_signature, normalized_text

log = logging.getLogger(__name__)
CASES = ("plain", "formatted", "photo", "video", "document", "photo-album", "mixed-album")


def fixture_items(case, label, platform, video):
    text = label
    entities = []
    if case == "formatted":
        text += "\n😀 bold italic link"
        for word, kind in (("bold", "bold"), ("italic", "italic"), ("link", "text_link")):
            entity = {"type": kind, "offset": utf16_length(text[:text.index(word)]), "length": len(word)}
            if kind == "text_link":
                entity["url"] = "https://example.com/"
            entities.append(entity)
    if platform == "bale":
        text, entities = telegram_to_bale(text, entities), []
    if case in {"plain", "formatted"}:
        return [{"kind": "text", "text": text, "entities": entities}]
    assets = Path(__file__).with_name("assets")

    def photo(color, caption):
        return {"kind": "photo", "text": caption, "entities": [],
                "content": (assets / f"{color}.png").read_bytes(), "filename": f"{color}.png",
                "mime_type": "image/png", "properties": {"width": 128, "height": 128}}

    def clip(caption):
        return {"kind": "video", "text": caption, "entities": [], "content": video.read_bytes(),
                "filename": "clip.mp4", "mime_type": "video/mp4", "properties": {"width": 64, "height": 64}}

    if case == "photo":
        return [photo("blue", label)]
    if case == "video":
        return [clip(label)]
    if case == "document":
        content = f"{label}\nchannel-sync document transfer fixture.\n".encode()
        filename = f"{label}.txt"
        return [{"kind": "document", "text": label, "entities": [], "content": content,
                 "filename": filename, "mime_type": "text/plain",
                 "properties": {"file_name": filename, "file_size": len(content)}}]
    if case == "photo-album":
        return [photo("blue", label + "/1"), photo("orange", label + "/2")]
    if case == "mixed-album":
        return [photo("orange", label + "/1"), clip(label + "/2")]
    raise ValueError("Unknown fixture case")


def enqueue_fixture(bridge, key, platform, response, items):
    results = response["result"] if isinstance(response["result"], list) else [response["result"]]
    if len(results) != len(items) or any(not isinstance(m.get("message_id"), int) for m in results):
        raise RuntimeError("Fixture publication returned incomplete message IDs")
    now = time.time()
    with bridge.store.transaction() as db:
        db.execute("INSERT INTO bridge_jobs(job_key,source_platform,direction,created_at,due_at) VALUES(?,?,?,?,?)",
                   (key, platform, "tg_to_bale" if platform == "telegram" else "bale_to_tg",
                    now, now + bridge.config.album_wait if len(items) > 1 else now))
        for message, item in zip(results, items):
            # The actual response may omit album content. Preserve publication
            # metadata explicitly, rather than inventing file IDs or raw updates.
            source = {**message, "chat": {"id": bridge.channel_ids[platform], "type": "channel"},
                      "batch_fixture": True}
            source.setdefault("text" if item["kind"] == "text" else "caption", item["text"])
            if item.get("entities"):
                source.setdefault("entities" if item["kind"] == "text" else "caption_entities", item["entities"])
            db.execute("INSERT INTO bridge_items VALUES(?,?,?,?,?,?,?)",
                       (platform, bridge.channel_ids[platform], message["message_id"], 0,
                        message.get("media_group_id"), key, bridge.store.dumps(bridge.codec.encode(source))))
    bridge.fixtures[key] = items
    bridge.wakeup.set()
    return [m["message_id"] for m in results]


class BatchBridge(Bridge):
    def __init__(self, *args, prefix, **kwargs):
        super().__init__(*args, **kwargs)
        self.prefix = prefix
        self.fixtures = {}
        self.download_modes = {}

    def process_inbox(self, platform, *, baseline=False):
        for row in self.store.pending_updates(platform):
            update = self.codec.decode(json.loads(row["payload_json"]))
            message = extract_message(update, platform)
            if not message or message.get("chat", {}).get("id") != self.channel_ids[platform]:
                continue
            text = normalized_text(message.get("text", message.get("caption", "")), platform)
            if self.prefix not in text:
                continue
            seen = self.store.connection.execute("SELECT 1 FROM bridge_seen WHERE platform=? AND chat_id=? AND message_id=?",
                                                 (platform, self.channel_ids[platform], message["message_id"])).fetchone()
            pending = self.store.connection.execute(
                "SELECT 1 FROM pending_echoes e JOIN sent_messages s USING(send_key) "
                "WHERE e.platform=? AND e.chat_id=? AND e.signature=? AND e.remaining=1 AND e.expires_at>=? "
                "AND s.status IN ('sending','sent','uncertain') LIMIT 1",
                (platform, self.channel_ids[platform], echo_signature(message, platform), time.time())).fetchone()
            if not seen and not pending:
                self.problem("tg_to_bale" if platform == "telegram" else "bale_to_tg",
                             "Unexpected batch content: duplicate echo or signature mismatch")
        super().process_inbox(platform, baseline=baseline)

    async def prepare_items(self, job):
        target = "bale" if job["source_platform"] == "telegram" else "telegram"
        rows = self.store.items(job["job_key"])
        original = self.fixtures[job["job_key"]]
        items, modes = [], []
        for row, fixture in zip(rows, original):
            source = self.codec.decode(json.loads(row["message_json"]))
            rich = convert(source.get("text", source.get("caption", "")),
                           source.get("entities", source.get("caption_entities", [])), job["source_platform"], target)
            item = {**fixture, "text": rich.text, "entities": rich.entities,
                    "source": {"platform": row["platform"], "chat_id": row["chat_id"],
                               "message_id": row["message_id"], "media_group_id": row["media_group_id"]}}
            if fixture["kind"] != "text" and source.get(fixture["kind"]):
                attachment = await download_attachment(self.clients[job["source_platform"]], source)
                if attachment is None:
                    raise RuntimeError("Published fixture attachment missing")
                item.update(content=attachment.content, filename=attachment.filename,
                            mime_type=attachment.mime_type, properties=attachment.properties)
                modes.append("source API download")
            elif fixture["kind"] != "text":
                modes.append("original upload bytes: source album response omitted file metadata")
            else:
                modes.append("text")
            items.append(item)
        self.download_modes[job["job_key"]] = modes
        return target, items

    async def forward(self, job):
        key = job["job_key"]
        if key not in self.fixtures:
            self.problem(job["direction"], "Unexpected forward job during supervised batch; stopping")
        self.store.set_job(key, "working")
        try:
            target, items = await self.prepare_items(job)
            self.store.reserve_forward(key, job["direction"], len(items), window=self.config.circuit_window,
                                       maximum=self.config.circuit_max_sends)
            await self.transmit(key + ":primary", target, items, job_key=key)
            self.store.set_job(key, "sent")
            self.event("forwarded", job_key=key, direction=job["direction"], item_count=len(items))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.store.set_job(key, "uncertain" if isinstance(exc, ApiError) and exc.uncertain else "failed", str(exc))
            self.problem(job["direction"], str(exc))


async def publish_fixture(bridge, key, platform, items):
    method = "sendMediaGroup" if len(items) > 1 else {"text": "sendMessage", "photo": "sendPhoto", "video": "sendVideo", "document": "sendDocument"}[items[0]["kind"]]
    bridge.store.prepare_forward_send(send_key=key, platform=platform, chat_id=bridge.channel_ids[platform], method=method,
                                      request={"purpose": "authorized source fixture", "items": [{k: v for k, v in i.items() if k != "content"} for i in items]},
                                      signatures=[echo_signature(bridge.expected_message(i), platform) for i in items], ttl=bridge.config.signature_ttl)
    bridge.store.claim_forward_send(key, bridge.config.signature_ttl)
    client = bridge.clients[platform]
    previous = client.attempt_recorder

    def record(attempt):
        if attempt["method"] == method:
            bridge.store.record_send_attempt(key, attempt["attempt"], attempt)
            bridge.report["sends"].append({"send_key": key, "purpose": "source fixture", **attempt})
            bridge.checkpoint()
    client.attempt_recorder = record
    try:
        response = await client.send_content(bridge.channel_ids[platform], items)
        results = response["result"] if isinstance(response["result"], list) else [response["result"]]
        if len(results) != len(items) or any(not isinstance(m.get("message_id"), int) for m in results):
            raise ApiError(platform, method, "Incomplete fixture send response", uncertain=True)
    except BaseException as exc:
        uncertain = not isinstance(exc, ApiError) or exc.uncertain
        bridge.store.finish_forward_send(key, status="uncertain" if uncertain else "failed",
                                         error={"error": str(exc), "uncertain": uncertain}, ttl=bridge.config.signature_ttl)
        raise
    else:
        bridge.store.finish_forward_send(key, status="sent", response=response, ttl=bridge.config.signature_ttl)
        return response
    finally:
        client.attempt_recorder = previous


async def live_batch(config, clients, store, output, video, *, observation_seconds=60):
    if config.dry_run:
        raise ValueError("Live batch requires DRY_RUN=false")
    if not video.is_file() or video.stat().st_size > 20 * 1024 * 1024:
        raise ValueError("Provide a 64×64 MP4 fixture under 20 MiB before starting live-batch")
    prefix = "CS-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    redactor = next(iter(clients.values())).redactor
    report = {"run_id": prefix, "status": "starting", "cases": [], "phase_reports": [], "errors": [],
              "test_mode": "bot-published fixtures with explicit durable jobs; human-post admission is not exercised",
              "visual_confirmation": "pending user inspection", "observation_seconds_per_phase": observation_seconds}
    checkpoint = lambda: save_report(output, report, redactor)
    checkpoint()
    task = None

    async def wait_until(predicate, bridge, timeout=90):
        async with asyncio.timeout(timeout):
            while not predicate():
                if task.done():
                    await task
                    raise RuntimeError("Bridge stopped before the batch completed")
                if bridge.report["errors"]:
                    raise RuntimeError("Bridge reported a forwarding error")
                await asyncio.sleep(0.1)

    async def publish_while_healthy(bridge, key, source, fixtures):
        publication = asyncio.create_task(publish_fixture(bridge, key, source, fixtures))
        try:
            done, _ = await asyncio.wait({publication, task}, return_when=asyncio.FIRST_COMPLETED)
            if task in done:
                await task
                raise RuntimeError("Bridge stopped during fixture publication")
            return await publication
        finally:
            if not publication.done():
                publication.cancel()
                await asyncio.gather(publication, return_exceptions=True)

    try:
        for phase in ("b", "c"):
            conf = replace(config, telegram_to_bale_enabled=phase == "c", bale_to_telegram_enabled=True)
            phase_output = output.with_name(output.stem + f"-phase-{phase}.json")
            bridge = BatchBridge(conf, clients, store, output=phase_output, stop_on_error=True,
                                 new_posts_only=True, phase=phase, prefix=prefix)
            report["phase_reports"].append(str(phase_output))
            checkpoint()
            task = asyncio.create_task(bridge.run())
            try:
                await wait_until(lambda: bridge.started, bridge)
                report["status"] = "running"
                checkpoint()
                log.info("Batch phase %s ready; strict permanent-error stop enabled", phase.upper())
                for source in (("bale",) if phase == "b" else ("telegram", "bale")):
                    direction = "BT" if source == "bale" else "TB"
                    for case in CASES:
                        label = f"{prefix}-{phase.upper()}-{direction}-{case}"
                        key = label + ":job"
                        item = {"phase": phase.upper(), "case": case, "label": label, "source": source,
                                "source_channel_id": bridge.channel_ids[source],
                                "target": "telegram" if source == "bale" else "bale", "status": "publishing"}
                        report["cases"].append(item)
                        checkpoint()
                        fixtures = fixture_items(case, label, source, video)
                        item["item_labels"] = [i["text"] for i in fixtures]
                        response = await publish_while_healthy(bridge, label + ":publish", source, fixtures)
                        item["source_message_ids"] = enqueue_fixture(bridge, key, source, response, fixtures)
                        item["status"] = "forwarding"
                        checkpoint()
                        await wait_until(lambda: store.connection.execute("SELECT state FROM bridge_jobs WHERE job_key=?", (key,)).fetchone()[0] == "sent", bridge)
                        mappings = store.connection.execute("SELECT source_message_id,target_message_id FROM message_mapping WHERE send_key=? ORDER BY source_message_id", (key + ":primary",)).fetchall()
                        if len(mappings) != len(fixtures) or len({r["target_message_id"] for r in mappings}) != len(fixtures):
                            raise RuntimeError("Missing or duplicate target message mapping")
                        sends = [s for s in bridge.report["sends"] if s.get("job_key") == key]
                        if len([s for s in sends if s.get("response", {}).get("ok")]) != 1:
                            raise RuntimeError("Unexpected successful send count")
                        item.update(status="api_passed", target_message_ids=[r["target_message_id"] for r in mappings],
                                    transfer_modes=bridge.download_modes[key], forward_attempts=len(sends), fallback=None)
                        checkpoint()
                        log.info("%s: API passed; source=%s target=%s", label, item["source_message_ids"], item["target_message_ids"])
                        await asyncio.sleep(1)
                deadline = time.monotonic() + observation_seconds
                log.info("Phase %s publishing complete; observing echoes for %ss", phase.upper(), observation_seconds)
                await wait_until(lambda: time.monotonic() >= deadline, bridge, timeout=observation_seconds + 10)
                report.setdefault("phase_observations", {})[phase.upper()] = {
                    "echoes_ignored": sum(e.get("reason") == "echo signature" for e in bridge.report["events"]),
                    "own_bot_updates_ignored": sum(e.get("reason") == "own bot" for e in bridge.report["events"]),
                    "errors": bridge.report["errors"], "circuits": store.snapshot()["circuits"]}
                checkpoint()
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                report.setdefault("phase_observations", {})[phase.upper()] = {
                    **report.get("phase_observations", {}).get(phase.upper(), {}),
                    "echoes_ignored": sum(e.get("reason") == "echo signature" for e in bridge.report["events"]),
                    "own_bot_updates_ignored": sum(e.get("reason") == "own bot" for e in bridge.report["events"]),
                    "errors": bridge.report["errors"], "circuits": store.snapshot()["circuits"],
                    "status": bridge.report["status"]}
                task = None
        report["status"] = "api_passed_awaiting_visual_inspection"
    except BaseException as exc:
        report["status"] = "stopped_on_problem"
        report["errors"].append(redactor.text(str(exc)))
        if report["cases"] and report["cases"][-1]["status"] != "api_passed":
            report["cases"][-1]["status"] = "failed"
        raise
    finally:
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        report["database"] = store.snapshot()
        checkpoint()
    return report
