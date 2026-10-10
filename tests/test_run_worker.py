import asyncio
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest

import httpx

from channel_sync.bridge import Bridge
from channel_sync.bridge_store import BridgeStore
from channel_sync.clients import TelegramClient, BaleClient
from channel_sync.redaction import Redactor
from helpers import config, TELEGRAM_TOKEN, BALE_TOKEN
from test_inspection_health import FakeAPIs


class AnonymousBaleAPIs(FakeAPIs):
    async def handle(self, request):
        response = await super().handle(request)
        # Reflect the real captured author metadata, rather than the test bot ID.
        for update in self.queues["bale"]:
            if update.get("message", {}).get("from", {}).get("is_bot"):
                update["message"]["from"] = {"id": 0, "is_bot": False}
        return response


class RunWorkerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.conf = config(self.root, dry_run=False, telegram_to_bale_enabled=True, bale_to_telegram_enabled=True)
        self.api = AnonymousBaleAPIs(self.conf)
        transport = httpx.MockTransport(self.api.handle)
        self.clients = {"telegram": TelegramClient(TELEGRAM_TOKEN, self.conf, transport=transport),
                        "bale": BaleClient(BALE_TOKEN, self.conf, transport=transport)}
        for client in self.clients.values():
            self.addAsyncCleanup(client.aclose)
        self.store = BridgeStore(self.conf.database_path, Redactor(TELEGRAM_TOKEN, BALE_TOKEN))
        self.addCleanup(self.store.close)
        self.bridge = None
        self.task = None
        self.addAsyncCleanup(self.stop)

    async def stop(self):
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass

    async def until(self, predicate):
        async def wait():
            while not predicate():
                if self.task.done():
                    await self.task
                    self.fail("Bridge unexpectedly stopped")
                await asyncio.sleep(0.01)
        await asyncio.wait_for(wait(), timeout=3)

    def post(self, platform, update_id, message_id, text):
        chat_id = self.conf.telegram_channel_id if platform == "telegram" else self.conf.bale_channel_id
        self.api.queues[platform].append({"update_id": update_id, "channel_post" if platform == "telegram" else "message": {
            "message_id": message_id, "chat": {"id": chat_id, "type": "channel"},
            "from": {"id": 0, "is_bot": False}, "text": text}})

    async def start(self, *, conf=None, new_posts_only=False):
        self.bridge = Bridge(conf or self.conf, self.clients, self.store, output=self.root / "run.json",
                             new_posts_only=new_posts_only, stop_on_error=True)
        self.task = asyncio.create_task(self.bridge.run())
        await self.until(lambda: self.bridge.started)

    async def test_both_directions_forward_once_with_anonymous_bale_echo(self):
        self.post("telegram", 100, 1, "TG manual *literal*")
        self.post("bale", 101, 2, "Bale manual *bold*")
        self.api.next_update = 102
        await self.start()
        await self.until(lambda: len([event for event in self.bridge.report["events"] if event["kind"] == "forwarded"]) == 2)
        await self.until(lambda: any(event.get("reason") == "echo signature" for event in self.bridge.report["events"]))
        self.assertEqual(self.api.events.count(("telegram", "sendMessage")), 1)
        self.assertEqual(self.api.events.count(("bale", "sendMessage")), 1)
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM message_mapping").fetchone()[0], 2)
        await self.stop()
        self.task = None
        self.assertEqual(json.loads((self.root / "run.json").read_text())["status"], "stopped")

    async def test_phase_a_disables_bale_sources_but_consumes_echo(self):
        self.post("telegram", 100, 1, "phase A TG")
        self.post("bale", 101, 2, "phase A Bale direction disabled")
        self.api.next_update = 102
        await self.start(conf=replace(self.conf, bale_to_telegram_enabled=False))
        await self.until(lambda: any(event.get("reason") == "echo signature" for event in self.bridge.report["events"]))
        self.assertEqual(self.api.events.count(("bale", "sendMessage")), 1)
        self.assertEqual(self.api.events.count(("telegram", "sendMessage")), 0)
        self.assertTrue(any(event.get("reason") == "direction disabled" for event in self.bridge.report["events"]))

    async def test_new_posts_only_drains_old_queue_before_ready(self):
        self.post("telegram", 100, 1, "old queue")
        await self.start(new_posts_only=True)
        self.assertFalse(any(method.startswith("send") for _, method in self.api.events))
        self.assertTrue(any(event.get("reason") == "startup baseline" for event in self.bridge.report["events"]))
        self.post("telegram", 101, 2, "new manual post")
        await self.until(lambda: self.api.events.count(("bale", "sendMessage")) == 1)

    async def test_uncertain_recovered_send_stops_before_polling(self):
        # Prepare a pending human post without any network request.
        self.bridge = Bridge(self.conf, self.clients, self.store)
        self.bridge.bot_ids = {"telegram": 123456789, "bale": 987654321}
        update = {"update_id": 1, "channel_post": {"message_id": 1, "chat": {"id": self.conf.telegram_channel_id, "type": "channel"}, "text": "uncertain"}}
        self.store.ingest("telegram", "sync", [update], encoder=self.bridge.codec.encode)
        self.bridge.process_inbox("telegram")
        key = self.store.snapshot()["jobs"][0]["job_key"] + ":primary"
        self.store.prepare_forward_send(send_key=key, platform="bale", chat_id=self.conf.bale_channel_id,
                                        method="sendMessage", request={}, signatures=["sig"], ttl=600)
        self.store.claim_forward_send(key, 600)
        bridge = Bridge(self.conf, self.clients, self.store, stop_on_error=True)
        with self.assertRaises(RuntimeError):
            await bridge.run()
        self.assertFalse(any(method == "getUpdates" for _, method in self.api.events))
        self.assertTrue(self.store.paused("tg_to_bale"))
