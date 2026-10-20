import asyncio
from datetime import datetime, timezone
from importlib.resources import files
import logging
from pathlib import Path
import uuid

from .clients import ApiError, BotClient
from .config import Config
from .health import health
from .messages import content_signature, extract_message, own_author, utf16_length
from .moderation import Moderator
from .redaction import Redactor, config_secrets
from .reports import save_report
from .store import CircuitOpen, Store

log = logging.getLogger(__name__)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def probe_specs(run_id: str, platform: str) -> list[dict]:
    tag = f"channel-sync {run_id} {platform}"
    plain = f"{tag} plain"
    formatted = f"{tag} formatted 😀 bold italic link"
    entities = []
    for label, kind in (("bold", "bold"), ("italic", "italic"), ("link", "text_link")):
        start = formatted.index(label, len(tag) + len(" formatted"))
        entity = {"type": kind, "offset": utf16_length(formatted[:start]), "length": utf16_length(label)}
        if kind == "text_link":
            entity["url"] = "https://example.com/"
        entities.append(entity)
    if platform == "bale":
        formatted = f"{tag} formatted 😀 *bold* _italic_ [link](https://example.com/)"
        entities = []
    return [
        {"name": "plain", "method": "sendMessage", "text": plain, "entities": []},
        {"name": "formatted", "method": "sendMessage", "text": formatted, "entities": entities},
        {"name": "photo", "method": "sendPhoto", "captions": [f"{tag} photo"]},
        {"name": "album", "method": "sendMediaGroup", "captions": [f"{tag} album/1", f"{tag} album/2"]},
    ]


def probe_messages(spec: dict) -> list[dict]:
    if spec["method"] == "sendMessage":
        return [{"text": spec["text"]}]
    return [{"photo": [], "caption": caption} for caption in spec["captions"]]


def analyze_echoes(report: dict) -> dict:
    findings = {}
    for platform, data in report["platforms"].items():
        bot = data.get("bot", {})
        cases = []
        matched_ids = set()
        for probe in data["sends"]:
            marker = f"channel-sync {report['run_id']} {platform} {probe['name']}"
            response = probe.get("response", {}).get("result", [])
            response_messages = response if isinstance(response, list) else [response]
            send_ids = [item.get("message_id") for item in response_messages if isinstance(item, dict)]
            echoes = []
            for entry in data["updates"]:
                message = extract_message(entry["update"], platform)
                if not message:
                    continue
                body = message.get("text", message.get("caption", ""))
                if marker not in body:
                    continue
                identity = (entry["update"].get("update_id"), message.get("message_id"))
                if identity in matched_ids:
                    continue
                matched_ids.add(identity)
                echoes.append({
                    "update_id": identity[0], "message_id": identity[1],
                    "send_response_id_matches": identity[1] in send_ids,
                    "update_field": "channel_post" if platform == "telegram" else "message",
                    "chat": message.get("chat"), "from": message.get("from"),
                    "sender_chat": message.get("sender_chat"), "via_bot": message.get("via_bot"),
                    "media_group_id": message.get("media_group_id"),
                    "entities": message.get("entities"), "caption_entities": message.get("caption_entities"),
                    "text": message.get("text"), "caption": message.get("caption"),
                    "own_author_field": own_author(message, bot.get("id")),
                    "signature_matches": content_signature(message, platform) in probe.get("signatures", []),
                })
            cases.append({"probe": probe["name"], "send_status": probe["status"],
                          "send_response_message_ids": send_ids, "echoes": echoes})
        all_echoes = [echo for case in cases for echo in case["echoes"]]
        reliable = [key for key in ("from.id", "via_bot.id")
                    if all_echoes and all(echo["own_author_field"] == key for echo in all_echoes)]
        findings[platform] = {
            "observed_echo_count": len(all_echoes),
            "identity_fields_present_on_every_observed_echo": reliable,
            "every_observed_echo_matches_content_signature": bool(all_echoes) and all(e["signature_matches"] for e in all_echoes),
            "message_id_changed": any(not e["send_response_id_matches"] for e in all_echoes),
            "interpretation": (
                "No echoes observed; cannot establish an author field for own posts."
                if not all_echoes else
                "These fields identify the captured test echoes only; verify against human posts before relying on them."
            ),
            "sender_chat_is_not_own_bot_proof": True,
            "cases": cases,
        }
    return findings


async def inspect_echo(config: Config, clients: dict[str, BotClient], store: Store,
                       output: Path, *, observation_seconds: float = 60) -> dict:
    run_id = uuid.uuid4().hex[:12]
    redactor = Redactor(*config_secrets(config))
    channel_ids = {"telegram": config.telegram_channel_id, "bale": config.bale_channel_id}
    report = {"schema_version": 1, "run_id": run_id, "started_at": utc_now(),
              "status": "starting", "dry_run": config.dry_run, "observation_seconds": observation_seconds,
              "platforms": {platform: {"channel_id": channel_ids[platform], "sends": [], "updates": [],
                                       "poll_responses": [], "poll_errors": [], "send_attempts": []} for platform in clients}}

    def checkpoint():
        save_report(output, report, redactor)

    checkpoint()
    if config.dry_run:
        report["status"] = "dry_run"
        report["plan"] = {p: probe_specs(run_id, p) for p in clients}
        report["note"] = "No API calls or sends. Set DRY_RUN=false in .env for the live diagnostic."
        checkpoint()
        return report
    report["health"] = await health(clients, channel_ids, check_moderation=False)
    checkpoint()
    if not report["health"]["ok"]:
        report["status"] = "health_failed"
        checkpoint()
        return report
    for item in report["health"]["bots"]:
        report["platforms"][item["platform"]]["bot"] = item["getMe"]["result"]
    # Do not silently disable another application's webhook or discard pending updates.
    for platform, client in clients.items():
        try:
            webhook = await client.call("getWebhookInfo")
        except ApiError as exc:
            report["platforms"][platform]["poll_errors"].append(exc.as_dict())
            report["status"] = "preflight_failed"
            checkpoint()
            return report
        report["platforms"][platform]["getWebhookInfo"] = webhook
        if webhook["result"].get("url"):
            report["status"] = "webhook_conflict"
            report["error"] = f"{platform} has a webhook. Use dedicated test bots with polling."
            checkpoint()
            return report
    moderator = Moderator(config, clients["telegram"], store)
    telegram_health = next(item for item in report["health"]["bots"] if item["platform"] == "telegram")
    report["moderation"] = await moderator.prepare(telegram_health)
    store.recover_moderation()
    report["status"] = "polling"
    checkpoint()
    previous_recorders = {platform: client.attempt_recorder for platform, client in clients.items()}

    def record_attempt(attempt: dict):
        if attempt["method"].startswith("send"):
            data = report["platforms"][attempt["platform"]]
            send_key = data["sends"][-1]["send_key"]
            attempt = {**attempt, "send_key": send_key}
            data["send_attempts"].append(attempt)
            checkpoint()
            store.record_send_attempt(send_key, attempt["attempt"], attempt)

    for client in clients.values():
        client.attempt_recorder = record_attempt
    ready = {p: asyncio.Event() for p in clients}
    failure = asyncio.Event()
    stop = asyncio.Event()

    async def poll(platform: str, client: BotClient):
        data = report["platforms"][platform]
        scope = f"inspect:{run_id}"
        first = True
        try:
            while not stop.is_set():
                envelope = await client.get_updates(store.offset(platform, scope), timeout=0 if first else None)
                updates = envelope["result"]
                if not isinstance(updates, list) or not all(isinstance(u, dict) for u in updates):
                    raise ValueError("getUpdates returned an invalid update list.")
                received = utc_now()
                data["poll_responses"].append({"received_at": received, "response": envelope})
                data["updates"].extend({"received_at": received, "update": update} for update in updates)
                checkpoint()
                store.ingest(platform, scope, updates)
                for update in updates:
                    await moderator.handle(platform, update)
                if first:
                    data["polling_ready_at"] = utc_now()
                    checkpoint()
                    ready[platform].set()
                    first = False
                # Some Bale deployments ignore timeout; avoid a busy poll loop.
                if not updates:
                    await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            error = exc.as_dict() if isinstance(exc, ApiError) else {"error": redactor.text(str(exc))}
            data["poll_errors"].append(error)
            failure.set()
            checkpoint()

    tasks = [asyncio.create_task(poll(p, c), name=f"poll-{p}") for p, c in clients.items()]
    ready_task = asyncio.create_task(_all_ready(ready))
    failure_task = asyncio.create_task(failure.wait())
    try:
        done, _ = await asyncio.wait({ready_task, failure_task}, timeout=config.request_timeout * config.retry_attempts + 30,
                                     return_when=asyncio.FIRST_COMPLETED)
        if ready_task not in done or failure.is_set():
            report["status"] = "polling_failed"
            report["error"] = "Both pollers must be active before any test send."
            return report
        report["status"] = "sending_probes"
        checkpoint()
        assets = files("channel_sync").joinpath("assets")
        photos = [assets.joinpath(name).read_bytes() for name in ("blue.png", "orange.png")]
        for platform, client in clients.items():
            for spec in probe_specs(run_id, platform):
                if failure.is_set():
                    report["status"] = "polling_failed"
                    return report
                key = f"inspect:{run_id}:{platform}:{spec['name']}"
                signatures = [content_signature(message, platform) for message in probe_messages(spec)]
                record = {**spec, "send_key": key, "signatures": signatures, "status": "prepared"}
                report["platforms"][platform]["sends"].append(record)
                store.prepare_send(send_key=key, platform=platform, chat_id=channel_ids[platform],
                                   method=spec["method"], request=spec, signatures=signatures, ttl=config.signature_ttl)
                checkpoint()
                try:
                    store.claim_send(key, window=config.circuit_window, max_sends=config.circuit_max_sends,
                                     ttl=config.signature_ttl)
                except CircuitOpen as exc:
                    record["status"] = "blocked"
                    record["error"] = str(exc)
                    report["status"] = "circuit_paused"
                    return report
                record["status"] = "sending"
                record["started_at"] = utc_now()
                checkpoint()
                try:
                    if spec["method"] == "sendMessage":
                        response = await client.send_message(channel_ids[platform], spec["text"], entities=spec["entities"])
                    elif spec["method"] == "sendPhoto":
                        response = await client.send_photo(channel_ids[platform], photos[0], spec["captions"][0])
                    else:
                        response = await client.send_photo_album(channel_ids[platform], list(zip(photos, spec["captions"])))
                except ApiError as exc:
                    record["status"] = "uncertain" if exc.uncertain else "failed"
                    record["error"] = exc.as_dict()
                    store.finish_send(key, status=record["status"], error=record["error"])
                except BaseException:
                    record["status"] = "uncertain"
                    store.finish_send(key, status="uncertain", error={"error": "Interrupted or unexpected failure during send"})
                    raise
                else:
                    record["status"] = "sent"
                    record["response"] = response
                    store.finish_send(key, status="sent", response=response)
                record["finished_at"] = utc_now()
                checkpoint()
        report["status"] = "observing"
        report["observation_started_at"] = utc_now()
        checkpoint()
        log.info("Probes finished; collecting updates for %.0f seconds", observation_seconds)
        try:
            await asyncio.wait_for(failure.wait(), timeout=observation_seconds)
            report["status"] = "polling_failed"
        except TimeoutError:
            statuses = [s["status"] for d in report["platforms"].values() for s in d["sends"]]
            report["status"] = "complete" if all(s == "sent" for s in statuses) else "complete_with_send_errors"
    except BaseException:
        report["status"] = "interrupted"
        raise
    finally:
        stop.set()
        for task in [*tasks, ready_task, failure_task]:
            task.cancel()
        await asyncio.gather(*tasks, ready_task, failure_task, return_exceptions=True)
        await moderator.aclose()
        for platform, client in clients.items():
            client.attempt_recorder = previous_recorders[platform]
        report["finished_at"] = utc_now()
        report["findings"] = analyze_echoes(report)
        checkpoint()
    return report


async def _all_ready(ready: dict[str, asyncio.Event]):
    await asyncio.gather(*(event.wait() for event in ready.values()))
