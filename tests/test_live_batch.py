import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

import httpx

from channel_sync.bridge import Bridge
from channel_sync.bridge_store import BridgeStore, SCOPE
from channel_sync.clients import ApiError, BaleClient, TelegramClient
from channel_sync.formatting import bale_to_telegram
from channel_sync.live_batch import BatchBridge, enqueue_fixture, fixture_items, publish_fixture, live_batch
from channel_sync.messages import utf16_slice
from channel_sync.redaction import Redactor
from helpers import config
from test_bridge import StubClient
from test_run_worker import AnonymousBaleAPIs


class LiveBatchTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.video = self.root / "clip.mp4"
        self.video.write_bytes(b"fixture video")
        self.conf = config(self.root, dry_run=False, telegram_to_bale_enabled=True, bale_to_telegram_enabled=True)
        self.store = BridgeStore(self.conf.database_path, Redactor())
        self.addCleanup(self.store.close)
        self.clients = {p: StubClient(p, self.conf) for p in ("telegram", "bale")}
        self.bridge = BatchBridge(self.conf, self.clients, self.store, prefix="CS-test", stop_on_error=True)
        self.bridge.bot_ids = {"telegram": 123456789, "bale": 987654321}

    def fixture(self, case="plain", platform="bale"):
        return fixture_items(case, "CS-test-" + case, platform, self.video)

    def queue(self, items, platform="bale", key="CS-test:job"):
        messages = [{"message_id": 500 + i, "from": {"id": self.bridge.bot_ids[platform], "is_bot": True},
                     **({"media_group_id": "group"} if len(items) > 1 else {})} for i in range(len(items))]
        response = {"ok": True, "result": messages if len(items) > 1 else messages[0]}
        enqueue_fixture(self.bridge, key, platform, response, items)
        return self.store.next_job(["tg_to_bale", "bale_to_tg"], now=time.time() + 10)

    def incoming(self, message, update_id):
        update = {"update_id": update_id, "message": message}
        self.store.ingest("bale", SCOPE, [update], encoder=self.bridge.codec.encode)
        self.bridge.process_inbox("bale")

    async def test_utf16_formatted_fixture_and_bale_roundtrip(self):
        tg = self.fixture("formatted", "telegram")[0]
        self.assertEqual([utf16_slice(tg["text"], e["offset"], e["length"]) for e in tg["entities"]], ["bold", "italic", "link"])
        bale = self.fixture("formatted")[0]
        rich = bale_to_telegram(bale["text"])
        self.assertEqual(rich.text, tg["text"])
        self.assertEqual({e["type"] for e in rich.entities}, {"bold", "italic", "text_link"})

    async def test_album_items_have_distinct_labels_and_correct_kinds(self):
        photo = self.fixture("photo-album")
        mixed = self.fixture("mixed-album")
        self.assertEqual([i["kind"] for i in photo], ["photo", "photo"])
        self.assertEqual([i["kind"] for i in mixed], ["photo", "video"])
        self.assertNotEqual(photo[0]["text"], photo[1]["text"])
        self.assertNotEqual(photo[0]["content"], photo[1]["content"])

    async def test_publication_registers_signature_before_echo_and_does_not_queue_it(self):
        items = self.fixture()
        message = {"message_id": 900, "chat": {"id": self.conf.bale_channel_id, "type": "channel"},
                   "from": {"id": 0, "is_bot": False}, "text": items[0]["text"]}
        def echo(_):
            self.assertEqual(self.store.send("CS-test:publish")["status"], "sending")
            self.incoming(message, 1)
        self.clients["bale"].on_send = echo
        await publish_fixture(self.bridge, "CS-test:publish", "bale", items)
        self.assertEqual(self.store.snapshot()["job_counts"], {})
        self.assertEqual(self.bridge.report["events"][-1]["reason"], "echo signature")
        self.assertEqual(self.store.connection.execute("SELECT remaining FROM pending_echoes").fetchone()[0], 0)

    async def test_extra_anonymous_echo_stops_before_forwarding(self):
        items = self.fixture()
        await publish_fixture(self.bridge, "CS-test:publish", "bale", items)
        message = {"message_id": 900, "chat": {"id": self.conf.bale_channel_id, "type": "channel"},
                   "from": {"id": 0, "is_bot": False}, "text": items[0]["text"]}
        self.incoming(message, 1)
        with self.assertRaisesRegex(RuntimeError, "duplicate echo"):
            self.incoming({**message, "message_id": 901}, 2)
        self.assertTrue(self.store.paused("bale_to_tg"))
        self.assertEqual(len(self.clients["telegram"].calls), 0)

    async def test_own_bot_filter_is_not_disabled(self):
        items = self.fixture()
        await publish_fixture(self.bridge, "CS-test:publish", "bale", items)
        self.incoming({"message_id": 900, "chat": {"id": self.conf.bale_channel_id, "type": "channel"},
                       "from": {"id": self.bridge.bot_ids["bale"], "is_bot": True}, "text": items[0]["text"]}, 1)
        self.assertEqual(self.bridge.report["events"][-1]["reason"], "own bot")
        self.assertEqual(self.store.snapshot()["job_counts"], {})

    async def test_explicit_source_job_preserves_bot_author_and_maps_actual_ids(self):
        job = self.queue(self.fixture("formatted"))
        source = json.loads(self.store.items(job["job_key"])[0]["message_json"])
        self.assertTrue(source["from"]["is_bot"])
        await self.bridge.forward(job)
        item = self.clients["telegram"].calls[0][0]
        self.assertEqual(item["source"]["message_id"], 500)
        self.assertEqual({e["type"] for e in item["entities"]}, {"bold", "italic", "text_link"})
        self.assertEqual(self.store.snapshot()["job_counts"], {"sent": 1})

    async def test_permanent_mixed_album_error_stops_without_fallback(self):
        job = self.queue(self.fixture("mixed-album", "telegram"), "telegram")
        self.clients["bale"].policy = lambda items: ApiError("bale", "sendMediaGroup", "UNSUPPORTED_FILE", code=400)
        with self.assertRaisesRegex(RuntimeError, "UNSUPPORTED_FILE"):
            await self.bridge.forward(job)
        self.assertEqual(len(self.clients["bale"].calls), 1)
        self.assertTrue(self.store.paused("tg_to_bale"))

    async def test_permanent_format_error_stops_without_plain_retry(self):
        job = self.queue(self.fixture("formatted"))
        self.clients["telegram"].policy = lambda items: ApiError("telegram", "sendMessage", "cannot parse entities", code=400)
        with self.assertRaisesRegex(RuntimeError, "cannot parse"):
            await self.bridge.forward(job)
        self.assertEqual(len(self.clients["telegram"].calls), 1)

    async def test_circuit_stops_batch_before_sending(self):
        job = self.queue(self.fixture())
        self.store.pause("bale_to_tg", "test circuit")
        with self.assertRaisesRegex(RuntimeError, "circuit"):
            await self.bridge.forward(job)
        self.assertEqual(len(self.clients["telegram"].calls), 0)

    async def test_normal_restart_refuses_unfinished_controlled_fixture(self):
        job = self.queue(self.fixture("photo-album"))
        normal = Bridge(self.conf, self.clients, self.store)
        await normal.forward(job)
        self.assertTrue(self.store.paused("bale_to_tg"))
        self.assertEqual(self.store.snapshot()["job_counts"], {"failed": 1})
        self.assertEqual(len(self.clients["telegram"].calls), 0)

    async def test_batch_automatically_transitions_b_to_c_and_stops(self):
        api = AnonymousBaleAPIs(self.conf)
        transport = httpx.MockTransport(api.handle)
        clients = {"telegram": TelegramClient(self.conf.telegram_token, self.conf, transport=transport),
                   "bale": BaleClient(self.conf.bale_token, self.conf, transport=transport)}
        for client in clients.values():
            self.addAsyncCleanup(client.aclose)
        output = self.root / "batch.json"
        with patch("channel_sync.live_batch.CASES", ("plain",)):
            report = await live_batch(self.conf, clients, self.store, output, self.video, observation_seconds=0.01)
        self.assertEqual(report["status"], "api_passed_awaiting_visual_inspection")
        self.assertEqual([(x["phase"], x["source"], x["status"]) for x in report["cases"]],
                         [("B", "bale", "api_passed"), ("C", "telegram", "api_passed"), ("C", "bale", "api_passed")])
        self.assertEqual(self.store.snapshot()["job_counts"], {"sent": 3})
        self.assertEqual(self.store.connection.execute("SELECT count(*) FROM message_mapping").fetchone()[0], 3)
        for path in report["phase_reports"]:
            self.assertEqual(json.loads(Path(path).read_text())["status"], "stopped")

    async def test_batch_source_publication_400_prevents_c_and_saves_failure(self):
        api = AnonymousBaleAPIs(self.conf)
        api.send_failure = "sendMessage"
        transport = httpx.MockTransport(api.handle)
        clients = {"telegram": TelegramClient(self.conf.telegram_token, self.conf, transport=transport),
                   "bale": BaleClient(self.conf.bale_token, self.conf, transport=transport)}
        for client in clients.values():
            self.addAsyncCleanup(client.aclose)
        output = self.root / "failure.json"
        with self.assertRaises(ApiError):
            await live_batch(self.conf, clients, self.store, output, self.video, observation_seconds=0.01)
        report = json.loads(output.read_text())
        self.assertEqual(report["status"], "stopped_on_problem")
        self.assertEqual(len(report["phase_reports"]), 1)
        self.assertEqual(report["cases"][0]["status"], "failed")
        self.assertEqual(api.events.count(("bale", "sendMessage")), 1)
        self.assertEqual(api.events.count(("telegram", "sendMessage")), 0)
