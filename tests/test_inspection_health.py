import asyncio
from dataclasses import replace
from email.parser import BytesParser
from email.policy import default
import json
from pathlib import Path
import tempfile
import unittest

import httpx

from channel_sync.clients import BaleClient, TelegramClient
from channel_sync.health import health
from channel_sync.inspection import inspect_echo
from channel_sync.redaction import Redactor
from channel_sync.store import Store
from helpers import config, TELEGRAM_TOKEN, BALE_TOKEN


class FakeAPIs:
    """Transport-only fixture: absolutely no live network traffic."""
    def __init__(self, conf):
        self.conf = conf
        self.events = []
        self.ready = set()
        self.queues = {"telegram": [], "bale": []}
        self.next_message = {"telegram": 25, "bale": 25}
        self.next_update = 1
        self.poll_failure = None
        self.send_failure = None
        self.membership = "administrator"
        self.can_post = True
        self.webhook = ""
        self.uncertain = False
        self.pending_sends = 0
        self.send_entered = asyncio.Event()
        self.block_send = False
        self.rate_limit_once = False
        self.rate_limited = False

    async def handle(self, request):
        platform = "telegram" if request.url.host == "api.telegram.org" else "bale"
        method = request.url.path.rsplit("/", 1)[1]
        self.events.append((platform, method))
        chat_id = self.conf.telegram_channel_id if platform == "telegram" else self.conf.bale_channel_id
        bot_id = 123456789 if platform == "telegram" else 987654321
        if method == "getMe":
            return httpx.Response(200, json={"ok": True, "result": {"id": bot_id, "is_bot": True, "username": f"{platform}_test_bot"}})
        if method == "getChat":
            return httpx.Response(200, json={"ok": True, "result": {"id": chat_id, "type": "channel" if platform == "telegram" else "group"}})
        if method == "getChatMember":
            return httpx.Response(200, json={"ok": True, "result": {"status": self.membership, "can_post_messages": self.can_post}})
        if method == "getWebhookInfo":
            return httpx.Response(200, json={"ok": True, "result": {"url": self.webhook}})
        if method == "getUpdates":
            await asyncio.sleep(0)
            if platform == self.poll_failure:
                return httpx.Response(400, json={"ok": False, "error_code": 400, "description": "Polling unsupported"})
            self.ready.add(platform)
            queue, self.queues[platform] = self.queues[platform], []
            return httpx.Response(200, json={"ok": True, "result": queue})
        if method.startswith("send"):
            if self.ready != {"telegram", "bale"}:
                raise AssertionError("Send began before both initial polls completed")
            self.send_entered.set()
            if self.rate_limit_once and not self.rate_limited:
                self.rate_limited = True
                return httpx.Response(429, json={"ok": False, "error_code": 429, "parameters": {"retry_after": 0}})
            if self.block_send:
                await asyncio.Event().wait()
            if self.uncertain:
                raise httpx.ReadTimeout(f"response lost {request.url}", request=request)
            if method == self.send_failure:
                return httpx.Response(400, json={"ok": False, "error_code": 400, "description": f"UNSUPPORTED_FILE {BALE_TOKEN}"})
            if request.headers.get("content-type", "").startswith("multipart/"):
                body = await request.aread()
                mime = BytesParser(policy=default).parsebytes(
                    f"Content-Type: {request.headers['content-type']}\r\nMIME-Version: 1.0\r\n\r\n".encode() + body
                )
                params = {part.get_param("name", header="content-disposition"): part.get_payload(decode=True).decode()
                          for part in mime.iter_parts() if not part.get_filename()}
            else:
                params = json.loads(request.content)
            message_id = self.next_message[platform]
            self.next_message[platform] += 5
            if method == "sendMessage":
                messages = [{"text": params["text"], "entities": params.get("entities", [])}]
            elif method == "sendPhoto":
                messages = [{"caption": params["caption"], "photo": [{"file_id": "send-photo"}]}]
            else:
                messages = [{"caption": item["caption"], "photo": [{"file_id": "send-photo"}], "media_group_id": "send-album"}
                            for item in json.loads(params["media"])]
            responses = []
            for index, message in enumerate(messages):
                responses.append({**message, "message_id": message_id + index * 2,
                                  "chat": {"id": chat_id, "type": "group" if platform == "bale" else "channel"}})
                if platform == "bale":
                    echo = {**message, "message_id": message_id + index * 2 + 1,
                            "from": {"id": bot_id, "is_bot": True},
                            "sender_chat": {"id": chat_id}, "chat": {"id": chat_id, "type": "channel"}}
                    if "photo" in echo:
                        echo["photo"] = [{"file_id": "changed-echo-file"}]
                    if "media_group_id" in echo:
                        echo["media_group_id"] = "different-echo-album"
                    self.queues[platform].append({"update_id": self.next_update, "message": echo})
                    self.next_update += 1
            return httpx.Response(200, json={"ok": True, "result": responses if method == "sendMediaGroup" else responses[0]})
        raise AssertionError(f"Unexpected fixture request: {platform} {method}")


class InspectionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.conf = config(self.root, dry_run=False)
        self.api = FakeAPIs(self.conf)
        transport = httpx.MockTransport(self.api.handle)
        self.clients = {"telegram": TelegramClient(TELEGRAM_TOKEN, self.conf, transport=transport),
                        "bale": BaleClient(BALE_TOKEN, self.conf, transport=transport)}
        for client in self.clients.values():
            self.addAsyncCleanup(client.aclose)
        self.store = Store(self.conf.database_path, Redactor(TELEGRAM_TOKEN, BALE_TOKEN))
        self.addCleanup(self.store.close)
        self.output = self.root / "report.json"

    async def inspect(self):
        return await inspect_echo(self.conf, self.clients, self.store, self.output, observation_seconds=0.2)

    async def test_polling_first_and_all_four_probes_with_changed_bale_ids(self):
        report = await self.inspect()
        self.assertEqual(report["status"], "complete")
        for platform in self.clients:
            sends = report["platforms"][platform]["sends"]
            self.assertEqual([item["name"] for item in sends], ["plain", "formatted", "photo", "album"])
            self.assertTrue(all(item["response"]["ok"] for item in sends))
        self.assertEqual(report["findings"]["telegram"]["observed_echo_count"], 0)
        bale = report["findings"]["bale"]
        self.assertEqual(bale["observed_echo_count"], 5)
        self.assertTrue(bale["message_id_changed"])
        self.assertTrue(bale["every_observed_echo_matches_content_signature"])
        self.assertEqual(bale["identity_fields_present_on_every_observed_echo"], ["from.id"])
        self.assertEqual(len(report["platforms"]["bale"]["updates"]), 5)
        saved = self.output.read_text()
        self.assertNotIn(TELEGRAM_TOKEN, saved)
        self.assertNotIn(BALE_TOKEN, saved)
        self.assertEqual(json.loads(saved)["status"], "complete")
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM sent_messages WHERE status='sent'").fetchone()[0], 8)
        self.assertIsNone(self.store.offset("telegram", "sync"))

    async def test_poll_failure_prevents_any_send(self):
        self.api.poll_failure = "bale"
        report = await self.inspect()
        self.assertEqual(report["status"], "polling_failed")
        self.assertFalse(any(method.startswith("send") for _, method in self.api.events))

    async def test_album_400_saved_without_fallback_or_retry(self):
        self.api.send_failure = "sendMediaGroup"
        report = await self.inspect()
        self.assertEqual(report["status"], "complete_with_send_errors")
        for platform in self.clients:
            sends = report["platforms"][platform]["sends"]
            self.assertEqual(sends[-1]["status"], "failed")
            self.assertEqual(sends[-1]["error"]["response"]["error_code"], 400)
            self.assertEqual(self.api.events.count((platform, "sendMediaGroup")), 1)
            self.assertEqual(self.api.events.count((platform, "sendPhoto")), 1)
        self.assertNotIn(BALE_TOKEN, self.output.read_text())

    async def test_dry_run_has_no_api_calls(self):
        report = await inspect_echo(replace(self.conf, dry_run=True), self.clients, self.store, self.output)
        self.assertEqual(report["status"], "dry_run")
        self.assertEqual(self.api.events, [])
        self.assertEqual(len(report["plan"]["bale"]), 4)

    async def test_intermediate_rate_limit_send_response_is_saved(self):
        self.api.rate_limit_once = True
        for client in self.clients.values():
            async def no_sleep(delay):
                pass
            client._sleep = no_sleep
        report = await self.inspect()
        attempts = report["platforms"]["telegram"]["send_attempts"]
        self.assertEqual(len(attempts), 5)
        self.assertEqual(attempts[0]["response"]["error_code"], 429)
        self.assertEqual(attempts[1]["attempt"], 2)
        self.assertTrue(attempts[1]["response"]["ok"])
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM send_attempts").fetchone()[0], 9)

    async def test_webhook_conflict_stops_before_poll_or_send(self):
        self.api.webhook = "https://example.com/webhook"
        report = await self.inspect()
        self.assertEqual(report["status"], "webhook_conflict")
        self.assertFalse(any(method == "getUpdates" or method.startswith("send") for _, method in self.api.events))

    async def test_cancel_during_send_preserves_uncertain_journal_and_report(self):
        self.api.block_send = True
        task = asyncio.create_task(self.inspect())
        await asyncio.wait_for(self.api.send_entered.wait(), timeout=2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.store.connection.execute("SELECT status FROM sent_messages").fetchone()[0], "uncertain")
        saved = json.loads(self.output.read_text())
        self.assertEqual(saved["status"], "interrupted")
        self.assertEqual(saved["platforms"]["telegram"]["sends"][0]["status"], "uncertain")

    async def test_circuit_pause_stops_diagnostic_sends(self):
        report = await inspect_echo(replace(self.conf, circuit_max_sends=1), self.clients, self.store,
                                    self.output, observation_seconds=0.2)
        self.assertEqual(report["status"], "circuit_paused")
        self.assertEqual(sum(method.startswith("send") for _, method in self.api.events), 1)

    async def test_health_accepts_observed_bale_group_type(self):
        report = await health(self.clients, {"telegram": self.conf.telegram_channel_id, "bale": self.conf.bale_channel_id})
        self.assertTrue(report["ok"])
        self.assertEqual(report["bots"][1]["chat_type"], "group")

    async def test_health_rejects_missing_post_permission(self):
        self.api.can_post = False
        report = await health(self.clients, {"telegram": self.conf.telegram_channel_id, "bale": self.conf.bale_channel_id})
        self.assertFalse(report["ok"])
        self.assertIn("cannot post", report["bots"][0]["error"])

    async def test_health_rejects_non_administrator(self):
        self.api.membership = "member"
        report = await health(self.clients, {"telegram": self.conf.telegram_channel_id, "bale": self.conf.bale_channel_id})
        self.assertFalse(report["ok"])
        self.assertIn("administrator", report["bots"][0]["error"])
