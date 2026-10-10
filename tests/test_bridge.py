import asyncio
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import time
import unittest

from channel_sync.bridge import Bridge
from channel_sync.bridge_store import BridgeStore, SCOPE
from channel_sync.clients import ApiError, DOWNLOAD_LIMIT
from channel_sync.redaction import Redactor
from channel_sync.signatures import echo_signature
from channel_sync.store import CircuitOpen
from helpers import config, TELEGRAM_TOKEN, BALE_TOKEN


class StubClient:
    def __init__(self, platform, conf):
        self.platform, self.config = platform, conf
        self.attempt_recorder = None
        self.calls, self.downloads = [], []
        self.policy = None
        self.on_send = None
        self.files = {}

    async def download_file(self, file_id):
        self.downloads.append(file_id)
        return self.files.get(file_id, b"fixture content")

    async def send_content(self, chat_id, items):
        self.calls.append(items)
        if self.policy:
            error = self.policy(items)
            if error:
                raise error
        if self.on_send:
            self.on_send(items)
        result = [{"message_id": 100 + len(self.calls) * 10 + i, "chat": {"id": chat_id}} for i in range(len(items))]
        return {"ok": True, "result": result if len(items) > 1 else result[0]}


class BridgeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.conf = config(self.root, dry_run=False, telegram_to_bale_enabled=True, bale_to_telegram_enabled=True)
        self.store = BridgeStore(self.conf.database_path, Redactor(TELEGRAM_TOKEN, BALE_TOKEN))
        self.addCleanup(self.store.close)
        self.clients = {p: StubClient(p, self.conf) for p in ("telegram", "bale")}
        self.bridge = Bridge(self.conf, self.clients, self.store)
        self.bridge.bot_ids = {"telegram": 123456789, "bale": 987654321}
        self.next_update = 1

    def message(self, platform="telegram", message_id=1, text="post", *, kind="text", group=None):
        message = {"message_id": message_id, "chat": {"id": self.bridge.channel_ids[platform], "type": "channel"},
                   "from": {"id": 0, "is_bot": False}}
        if kind == "text":
            message["text"] = text
        else:
            message["caption"] = text
            info = {"file_id": f"{kind}-{message_id}", "file_size": 15}
            if kind in {"photo", "video"}:
                info.update(width=128, height=128)
            else:
                info.update(file_name="sample.pdf", mime_type="application/pdf")
            message[kind] = [info] if kind == "photo" else info
        if group:
            message["media_group_id"] = group
        return message

    def incoming(self, message, platform="telegram", *, update_id=None, process=True):
        update_id = self.next_update if update_id is None else update_id
        self.next_update = max(self.next_update, update_id + 1)
        update = {"update_id": update_id, "channel_post" if platform == "telegram" else "message": message}
        self.store.ingest(platform, SCOPE, [update], encoder=self.bridge.codec.encode)
        if process:
            self.bridge.process_inbox(platform)
        return update

    async def forward_next(self):
        job = self.store.next_job(["tg_to_bale", "bale_to_tg"], now=time.time() + 10)
        self.assertIsNotNone(job)
        await self.bridge.forward(job)
        return job["job_key"]

    async def test_text_forwarding_maps_and_restart_does_not_duplicate(self):
        message = self.message()
        update = self.incoming(message)
        key = await self.forward_next()
        self.assertEqual(len(self.clients["bale"].calls), 1)
        self.assertEqual(self.store.snapshot()["jobs"][0]["state"], "sent")
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM message_mapping").fetchone()[0], 1)
        self.store.set_job(key, "working")  # crash after recording response, before job completion
        self.store.recover_jobs()
        await self.forward_next()
        self.store.ingest("telegram", SCOPE, [update], encoder=self.bridge.codec.encode)
        self.bridge.process_inbox("telegram")
        self.assertEqual(len(self.clients["bale"].calls), 1)

    async def test_bale_formatted_text_to_entities(self):
        self.incoming(self.message("bale", text="😀 *bold* _italic_ [link](https://example.com/)"), "bale")
        await self.forward_next()
        item = self.clients["telegram"].calls[0][0]
        self.assertEqual(item["text"], "😀 bold italic link")
        self.assertEqual({e["type"] for e in item["entities"]}, {"bold", "italic", "text_link"})

    async def test_photo_video_document_download_upload(self):
        for index, kind in enumerate(("photo", "video", "document"), start=1):
            self.incoming(self.message(message_id=index, kind=kind, text=f"{kind} caption"))
            await self.forward_next()
        self.assertEqual([call[0]["kind"] for call in self.clients["bale"].calls], ["photo", "video", "document"])
        self.assertEqual(self.clients["telegram"].downloads, ["photo-1", "video-2", "document-3"])

    async def test_album_buffers_sorts_and_sends_once(self):
        self.incoming(self.message(message_id=2, kind="photo", group="album", text="second"))
        self.incoming(self.message(message_id=1, kind="photo", group="album", text="first"))
        self.assertIsNone(self.store.next_job(["tg_to_bale"]))
        await self.forward_next()
        self.assertEqual(len(self.clients["bale"].calls), 1)
        self.assertEqual([item["source"]["message_id"] for item in self.clients["bale"].calls[0]], [1, 2])
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM message_mapping").fetchone()[0], 2)

    async def test_mixed_album_unsupported_falls_back_once_per_item(self):
        self.clients["bale"].policy = lambda items: ApiError("bale", "sendMediaGroup", "UNSUPPORTED_FILE", code=400) if len(items) > 1 else None
        self.incoming(self.message(message_id=1, kind="photo", group="mixed"))
        self.incoming(self.message(message_id=2, kind="video", group="mixed"))
        key = await self.forward_next()
        self.assertEqual([len(items) for items in self.clients["bale"].calls], [2, 1, 1])
        self.store.set_job(key, "working")
        self.store.recover_jobs()
        await self.forward_next()
        self.assertEqual([len(items) for items in self.clients["bale"].calls], [2, 1, 1])

    async def test_restart_between_separate_items_resumes_only_unsent_item(self):
        self.clients["bale"].policy = lambda items: ApiError("bale", "sendMediaGroup", "UNSUPPORTED_FILE", code=400) if len(items) > 1 else None
        for index, kind in ((1, "photo"), (2, "video")):
            self.incoming(self.message(message_id=index, kind=kind, group="partial"))
        original = self.bridge.transmit

        async def crash_between(key, *args, **kwargs):
            if key.endswith(":separate:2"):
                raise asyncio.CancelledError()
            return await original(key, *args, **kwargs)
        self.bridge.transmit = crash_between
        with self.assertRaises(asyncio.CancelledError):
            await self.forward_next()
        self.assertEqual([len(items) for items in self.clients["bale"].calls], [2, 1])
        self.bridge.transmit = original
        self.store.recover_jobs()
        await self.forward_next()
        self.assertEqual([len(items) for items in self.clients["bale"].calls], [2, 1, 1])

    async def test_other_album_400_does_not_trigger_fallback(self):
        self.clients["bale"].policy = lambda items: ApiError("bale", "sendMediaGroup", "BAD_REQUEST", code=400)
        for index in (1, 2):
            self.incoming(self.message(message_id=index, kind="photo", group="album"))
        await self.forward_next()
        self.assertEqual(len(self.clients["bale"].calls), 1)
        self.assertTrue(self.store.paused("tg_to_bale"))
        self.assertFalse(self.store.paused("bale_to_tg"))

    async def test_plain_fallback_only_for_explicit_format_rejection(self):
        def policy(items):
            if items[0]["entities"]:
                return ApiError("telegram", "sendMessage", "can't parse entities", code=400)
        self.clients["telegram"].policy = policy
        self.incoming(self.message("bale", text="*bold*"), "bale")
        await self.forward_next()
        self.assertEqual([call[0]["text"] for call in self.clients["telegram"].calls], ["bold", "bold"])
        self.assertEqual(self.clients["telegram"].calls[1][0]["entities"], [])
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM message_mapping").fetchone()[0], 1)

    async def test_uncertain_send_never_retried_or_fallen_back(self):
        self.clients["bale"].policy = lambda items: ApiError("bale", "sendMediaGroup", "connection lost", uncertain=True)
        for index in (1, 2):
            self.incoming(self.message(message_id=index, kind="photo", group="album"))
        key = await self.forward_next()
        self.assertEqual(len(self.clients["bale"].calls), 1)
        self.assertEqual(self.store.snapshot()["jobs"][0]["state"], "uncertain")
        self.store.set_job(key, "pending")
        self.store.connection.execute("UPDATE direction_circuits SET paused=0")
        await self.forward_next()
        self.assertEqual(len(self.clients["bale"].calls), 1)

    async def test_real_captured_bale_album_echo_is_consumed_before_queueing(self):
        fixture = json.loads((Path(__file__).parent / "fixtures/bale_album_echo.json").read_text())
        updates = fixture["updates"]
        signatures = [echo_signature(update["message"], "bale") for update in updates]
        self.store.prepare_forward_send(send_key="captured", platform="bale", chat_id=self.conf.bale_channel_id,
                                        method="sendMediaGroup", request={}, signatures=signatures, ttl=600)
        self.store.claim_forward_send("captured", 600)
        self.store.ingest("bale", SCOPE, updates, encoder=self.bridge.codec.encode)
        self.bridge.process_inbox("bale")
        self.assertEqual(self.store.snapshot()["jobs"], [])
        self.assertEqual(self.store.snapshot()["pending_echo_items"], 0)
        reasons = [row[0] for row in self.store.connection.execute("SELECT reason FROM bridge_seen")]
        self.assertEqual(reasons, ["echo signature", "echo signature"])

    async def test_signature_registered_before_send_can_echo_inside_request(self):
        def echo_inside_send(items):
            echo = {**Bridge.expected_message(items[0]), "message_id": 999,
                    "from": {"id": 0, "is_bot": False},
                    "chat": {"id": self.conf.bale_channel_id, "type": "channel"}}
            self.incoming(echo, "bale")
        self.clients["bale"].on_send = echo_inside_send
        self.incoming(self.message(kind="photo"))
        await self.forward_next()
        self.assertEqual(len(self.store.snapshot()["jobs"]), 1)
        self.assertEqual(self.clients["telegram"].calls, [])
        self.assertEqual(self.store.snapshot()["pending_echo_items"], 0)

    async def test_identical_album_items_have_two_counts_not_one(self):
        signature = echo_signature(self.message("bale"), "bale")
        self.store.prepare_forward_send(send_key="identical", platform="bale", chat_id=self.conf.bale_channel_id,
                                        method="sendMediaGroup", request={}, signatures=[signature, signature], ttl=600)
        self.store.claim_forward_send("identical", 600)
        self.incoming(self.message("bale", message_id=5), "bale")
        self.assertEqual(self.store.snapshot()["pending_echo_items"], 1)
        self.incoming(self.message("bale", message_id=5), "bale")
        self.assertEqual(self.store.snapshot()["pending_echo_items"], 1)
        self.incoming(self.message("bale", message_id=6), "bale")
        self.assertEqual(self.store.snapshot()["pending_echo_items"], 0)
        self.assertEqual(self.store.snapshot()["jobs"], [])

    async def test_expired_signature_does_not_suppress_human_post(self):
        self.store.prepare_forward_send(send_key="expired", platform="bale", chat_id=self.conf.bale_channel_id,
                                        method="sendMessage", request={}, signatures=[echo_signature(self.message("bale"), "bale")], ttl=600)
        self.store.claim_forward_send("expired", 600)
        self.store.connection.execute("UPDATE pending_echoes SET expires_at=0")
        self.incoming(self.message("bale"), "bale")
        self.assertEqual(len(self.store.snapshot()["jobs"]), 1)

    async def test_own_author_suppressed_in_both_directions(self):
        for platform in ("telegram", "bale"):
            message = self.message(platform)
            message["from"]["id"] = self.bridge.bot_ids[platform]
            self.incoming(message, platform)
        self.assertEqual(self.store.snapshot()["jobs"], [])

    async def test_disabled_direction_still_consumes_echoes(self):
        self.bridge.enabled["bale"] = False
        signature = echo_signature(self.message("bale"), "bale")
        self.store.prepare_forward_send(send_key="disabled", platform="bale", chat_id=self.conf.bale_channel_id,
                                        method="sendMessage", request={}, signatures=[signature], ttl=600)
        self.store.claim_forward_send("disabled", 600)
        self.incoming(self.message("bale"), "bale")
        self.assertEqual(self.store.connection.execute("SELECT reason FROM bridge_seen").fetchone()[0], "echo signature")

    async def test_restart_preserves_pending_signature_and_download_handle(self):
        message = self.message("bale", kind="document")
        message["document"]["file_id"] = f"prefix:{BALE_TOKEN}:suffix"
        self.incoming(message, "bale", process=False)
        stored = self.store.connection.execute("SELECT payload_json FROM inbox_payloads").fetchone()[0]
        self.assertNotIn(BALE_TOKEN, stored)
        self.assertEqual(self.store.offset("bale", SCOPE), 2)
        other = BridgeStore(self.conf.database_path, self.store.redactor)
        try:
            bridge = Bridge(self.conf, self.clients, other)
            bridge.bot_ids = self.bridge.bot_ids
            bridge.process_inbox("bale")
            await bridge.forward(other.next_job(["bale_to_tg"]))
            self.assertEqual(self.clients["bale"].downloads, [f"prefix:{BALE_TOKEN}:suffix"])
        finally:
            other.close()

    async def test_pending_signature_survives_database_reopen(self):
        signature = echo_signature(self.message("bale"), "bale")
        self.store.prepare_forward_send(send_key="restart-echo", platform="bale", chat_id=self.conf.bale_channel_id,
                                        method="sendMessage", request={}, signatures=[signature], ttl=600)
        self.store.claim_forward_send("restart-echo", 600)
        self.store.finish_forward_send("restart-echo", status="sent", response={"ok": True})
        other = BridgeStore(self.conf.database_path, self.store.redactor)
        try:
            bridge = Bridge(self.conf, self.clients, other)
            bridge.bot_ids = self.bridge.bot_ids
            update = {"update_id": 100, "message": self.message("bale", message_id=50)}
            other.ingest("bale", SCOPE, [update], encoder=bridge.codec.encode)
            bridge.process_inbox("bale")
            self.assertEqual(other.snapshot()["jobs"], [])
            self.assertEqual(other.snapshot()["pending_echo_items"], 0)
        finally:
            other.close()

    async def test_oversize_file_is_skipped_without_send(self):
        message = self.message(kind="video")
        message["video"]["file_size"] = DOWNLOAD_LIMIT + 1
        self.incoming(message)
        await self.forward_next()
        self.assertEqual(self.clients["bale"].calls, [])
        self.assertEqual(self.clients["telegram"].downloads, [])
        self.assertEqual(self.store.snapshot()["jobs"][0]["state"], "skipped")

    async def test_dry_run_does_not_download_send_or_reserve_signature(self):
        self.bridge.config = replace(self.conf, dry_run=True)
        self.incoming(self.message(kind="photo"))
        await self.forward_next()
        self.assertEqual(self.clients["bale"].calls, [])
        self.assertEqual(self.clients["telegram"].downloads, [])
        self.assertEqual(self.store.snapshot()["pending_echo_items"], 0)
        self.assertEqual(self.store.snapshot()["jobs"][0]["state"], "dry_run")

    async def test_direction_circuit_breaker_isolated_and_persistent(self):
        self.bridge.config = replace(self.conf, circuit_max_sends=1)
        self.incoming(self.message(message_id=1))
        await self.forward_next()
        self.incoming(self.message(message_id=2, text="second"))
        await self.forward_next()
        self.assertTrue(self.store.paused("tg_to_bale"))
        self.assertFalse(self.store.paused("bale_to_tg"))
        self.assertEqual(len(self.clients["bale"].calls), 1)
        other = BridgeStore(self.conf.database_path)
        try:
            self.assertTrue(other.paused("tg_to_bale"))
        finally:
            other.close()

    async def test_late_album_item_pauses_direction(self):
        self.incoming(self.message(message_id=1, kind="photo", group="album"))
        await self.forward_next()
        self.incoming(self.message(message_id=2, kind="photo", group="album"))
        self.assertTrue(self.store.paused("tg_to_bale"))

    async def test_stop_on_error_ends_live_phase(self):
        self.bridge.stop_on_error = True
        self.clients["bale"].policy = lambda items: ApiError("bale", "sendMessage", "Forbidden", code=403)
        self.incoming(self.message())
        with self.assertRaises(RuntimeError):
            await self.forward_next()
