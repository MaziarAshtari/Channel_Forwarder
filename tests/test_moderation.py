import asyncio
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import httpx

from channel_sync.bridge import Bridge
from channel_sync.bridge_store import BridgeStore, SCOPE
from channel_sync.clients import ApiError, BaleClient, TelegramClient
from channel_sync.jev import JevClient, QUESTION_IDS
from channel_sync.inspection import inspect_echo
from channel_sync.moderation import Moderator
from channel_sync.redaction import Redactor
from helpers import config, TELEGRAM_TOKEN, BALE_TOKEN
from test_inspection_health import FakeAPIs

TYPESAFE_KEY = "typesafe_test_secret_abcdef"
GROUP_ID = -100777111


class ModerationAPIs(FakeAPIs):
    def __init__(self, conf):
        super().__init__(conf)
        self.scores = {key: 0.1 for key in QUESTION_IDS}
        self.scores["should_delete"] = 0.55
        self.jev_requests, self.deletions, self.alerts, self.allowed_updates = [], [], [], []
        self.jev_status, self.jev_failure = 200, None
        self.delete_status, self.delete_uncertain = 200, False
        self.linked_group = GROUP_ID
        self.group_type = "supergroup"
        self.group_status, self.can_delete = "administrator", True
        self.author_status, self.author_failure = "member", False
        self.override_payload = None

    async def handle(self, request):
        params = json.loads(request.content) if request.headers.get("content-type") == "application/json" else {}
        if request.url.host == "api.typesafe.ai":
            self.jev_requests.append(request)
            if self.jev_failure:
                raise self.jev_failure(f"failed {TYPESAFE_KEY}", request=request)
            payload = self.override_payload if self.override_payload is not None else {
                "answers": {key: {"type": "noul", "noul": score} for key, score in self.scores.items()}}
            return httpx.Response(self.jev_status, json=payload)
        method = request.url.path.rsplit("/", 1)[1]
        if request.url.host == "api.telegram.org":
            if method == "getChat" and params["chat_id"] == GROUP_ID:
                return httpx.Response(200, json={"ok": True, "result": {"id": GROUP_ID, "type": self.group_type}})
            if method == "getChatMember" and params["chat_id"] == GROUP_ID:
                bot = params["user_id"] == 123456789
                if not bot and self.author_failure:
                    return httpx.Response(403, json={"ok": False, "error_code": 403, "description": "Unavailable"})
                return httpx.Response(200, json={"ok": True, "result": {
                    "status": self.group_status if bot else self.author_status, "can_delete_messages": self.can_delete}})
            if method == "deleteMessage":
                self.deletions.append(params)
                if self.delete_uncertain:
                    raise httpx.ReadTimeout("lost delete response", request=request)
                return httpx.Response(self.delete_status, json={"ok": self.delete_status == 200,
                                      "result": True, "error_code": self.delete_status})
            if method == "sendMessage" and params["chat_id"] != self.conf.telegram_channel_id:
                self.alerts.append(params)
                return httpx.Response(200, json={"ok": True, "result": {"message_id": 123}})
            if method == "getUpdates":
                self.allowed_updates.append(params["allowed_updates"])
        response = await super().handle(request)
        if method == "getChat" and request.url.host == "api.telegram.org":
            payload = response.json()
            payload["result"]["linked_chat_id"] = self.linked_group
            return httpx.Response(200, json=payload)
        return response


class ModerationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.conf = config(self.root, moderation_enabled=True, typesafe_api_key=TYPESAFE_KEY,
                           telegram_discussion_group_id=GROUP_ID, dry_run=False)
        self.api = ModerationAPIs(self.conf)
        transport = httpx.MockTransport(self.api.handle)
        self.telegram = TelegramClient(TELEGRAM_TOKEN, self.conf, transport=transport)
        self.bale = BaleClient(BALE_TOKEN, self.conf, transport=transport)
        self.delays = []

        async def sleep(delay):
            self.delays.append(delay)
        self.jev = JevClient(self.conf, transport=transport, sleeper=sleep)
        for client in (self.telegram, self.bale, self.jev):
            self.addAsyncCleanup(client.aclose)
        self.redactor = Redactor(TELEGRAM_TOKEN, BALE_TOKEN, TYPESAFE_KEY)
        self.store = BridgeStore(self.conf.database_path, self.redactor)
        self.addCleanup(self.store.close)
        self.moderator = Moderator(self.conf, self.telegram, self.store, jev=self.jev)
        self.moderator.bot_id, self.moderator.can_delete = 123456789, True

    def update(self, message_id=1, **changes):
        message = {"message_id": message_id, "chat": {"id": GROUP_ID, "type": "supergroup"},
                   "from": {"id": 999, "is_bot": False}, "text": "A user comment"}
        message.update(changes)
        return {"update_id": message_id, "message": message}

    def row(self):
        return self.store.connection.execute("SELECT * FROM moderation_decisions ORDER BY message_id DESC").fetchone()

    async def test_threshold_is_inclusive_and_only_should_delete_decides(self):
        for index, probability in enumerate((0.5499, 0.55, 0.9), start=1):
            self.api.scores = {key: 0.99 for key in QUESTION_IDS}
            self.api.scores["should_delete"] = probability
            result = await self.moderator.handle("telegram", self.update(index))
            self.assertEqual(result["action"], "review" if index == 1 else "deleted")
            self.assertEqual(json.loads(self.row()["scores_json"]), self.api.scores)
        self.assertEqual([item["message_id"] for item in self.api.deletions], [2, 3])
        self.assertTrue(all(item["chat_id"] == GROUP_ID for item in self.api.deletions))
        self.assertEqual(self.row()["user_id"], 999)
        self.assertEqual(self.row()["text"], "A user comment")
        self.assertGreater(self.row()["created_at"], 0)

    async def test_timeout_fail_open_after_bounded_retries(self):
        self.api.jev_failure = httpx.ReadTimeout
        with self.assertLogs("channel_sync.moderation", level="WARNING") as logs:
            result = await self.moderator.handle("telegram", self.update())
        self.assertEqual(result["action"], "kept_error")
        self.assertEqual(len(self.api.jev_requests), self.conf.retry_attempts)
        self.assertEqual(len(self.delays), self.conf.retry_attempts - 1)
        self.assertEqual(self.api.deletions, [])
        self.assertIn("failed open", " ".join(logs.output))
        self.assertNotIn(TYPESAFE_KEY, " ".join(logs.output))

    async def test_missing_key_fails_open_and_does_not_use_shell_environment(self):
        self.jev.config = replace(self.conf, typesafe_api_key="")
        result = await self.moderator.handle("telegram", self.update())
        self.assertEqual(result["action"], "kept_error")
        self.assertEqual(self.api.jev_requests, [])
        self.assertEqual(self.api.deletions, [])

    async def test_skip_rules_make_no_jev_or_delete_requests(self):
        cases = [
            {"is_automatic_forward": True},
            {"sender_chat": {"id": self.conf.telegram_channel_id}},
            {"sender_chat": {"id": GROUP_ID}},
            {"from": {"id": 555, "is_bot": True}},
            {"from": {"id": 123456789, "is_bot": False}},
            {"from": None}, {"from": {"id": 0, "is_bot": False}},
            {"text": None, "photo": [{"file_id": "media-only"}]},
            {"text": "  ", "caption": ""},
        ]
        for index, changes in enumerate(cases, start=1):
            result = await self.moderator.handle("telegram", self.update(index, **changes))
            self.assertEqual(result["action"], "skipped")
        self.assertEqual(self.api.jev_requests, [])
        self.assertEqual(self.api.deletions, [])

    async def test_admin_exemption_is_configurable(self):
        for index, status in enumerate(("administrator", "creator"), start=1):
            self.api.author_status = status
            self.assertEqual((await self.moderator.handle("telegram", self.update(index)))["action"], "skipped")
        self.assertEqual(self.api.jev_requests, [])
        self.moderator.config = replace(self.conf, moderation_skip_admins=False)
        self.assertEqual((await self.moderator.handle("telegram", self.update(3)))["action"], "deleted")

    async def test_unknown_admin_status_keeps_comment(self):
        for index, status in enumerate((None, "unknown"), start=1):
            self.api.author_status = status
            self.assertEqual((await self.moderator.handle("telegram", self.update(index)))["action"], "kept_error")
        self.api.author_failure = True
        self.assertEqual((await self.moderator.handle("telegram", self.update(3)))["action"], "kept_error")
        self.assertEqual(self.api.jev_requests, [])
        self.assertEqual(self.api.deletions, [])

    async def test_captions_are_classified_with_exact_state_and_questions(self):
        await self.moderator.handle("telegram", self.update(text="", caption="A caption"))
        request = self.api.jev_requests[0]
        self.assertEqual(str(request.url), "https://api.typesafe.ai/v1/systemone")
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.headers["Authorization"], f"Bearer {TYPESAFE_KEY}")
        body = json.loads(request.content)
        self.assertEqual(body["model"], "jev-latest")
        self.assertEqual(body["state"], {"comment": "A caption", "channel_topic": "Technology news and tutorials",
                                         "comment_language": "Persian or English"})
        self.assertEqual(body["questions"], json.loads((Path(__file__).parents[1] / "src/channel_sync/moderation_questions.json").read_text()))

    async def test_dry_run_classifies_without_deleting_or_alerting(self):
        self.moderator.config = replace(self.conf, dry_run=True, moderation_alert_chat_id=1234)
        self.assertEqual((await self.moderator.handle("telegram", self.update()))["action"], "dry_run")
        self.api.scores["should_delete"] = 0.5
        self.assertEqual((await self.moderator.handle("telegram", self.update(2)))["action"], "review")
        self.assertEqual(len(self.api.jev_requests), 2)
        self.assertEqual(self.row()["alert_status"], "dry_run")
        self.assertEqual(self.api.deletions, [])
        self.assertEqual(self.api.alerts, [])

    async def test_review_interval_is_lower_inclusive_upper_exclusive(self):
        self.moderator.config = replace(self.conf, moderation_alert_chat_id=1234)
        for index, score in enumerate((0.4499, 0.45, 0.5499, 0.55), start=1):
            self.api.scores["should_delete"] = score
            await self.moderator.handle("telegram", self.update(index))
        self.assertEqual(len(self.api.alerts), 2)
        self.assertEqual([d["message_id"] for d in self.api.deletions], [4])
        self.assertIn("امتیازها:", self.api.alerts[0]["text"])

    async def test_duplicate_comment_is_not_classified_deleted_or_alerted_twice(self):
        update = self.update()
        await self.moderator.handle("telegram", update)
        self.assertEqual((await self.moderator.handle("telegram", {**update, "update_id": 900}))["action"], "duplicate")
        other = BridgeStore(self.conf.database_path, self.redactor)
        try:
            moderator = Moderator(self.conf, self.telegram, other, jev=self.jev)
            await moderator.handle("telegram", update)
        finally:
            other.close()
        self.assertEqual(len(self.api.jev_requests), 1)
        self.assertEqual(len(self.api.deletions), 1)

    async def test_persian_review_labels_cover_multiline_comments_and_fit_message_limit(self):
        self.moderator.config = replace(self.conf, moderation_alert_chat_id=1234)
        await self.moderator.alert(GROUP_ID, 1, 999, "English comment\n" + "🧠\n" * 2000, self.api.scores)
        text = self.api.alerts[-1]["text"]
        self.assertLessEqual(len(text.encode("utf-16-le")) // 2, 3900)
        for line in text.splitlines():
            first = next((char for char in line if char.isalpha()), None)
            if first:
                self.assertTrue("\u0600" <= first <= "\u06ff", line)

    async def test_delete_failure_does_not_pause_forwarding_or_repeat_delete(self):
        self.api.delete_uncertain = True
        self.assertEqual((await self.moderator.handle("telegram", self.update()))["action"], "delete_uncertain")
        await self.moderator.handle("telegram", self.update())
        self.assertEqual(len(self.api.deletions), 1)
        self.assertFalse(self.store.paused("tg_to_bale"))
        self.assertFalse(self.store.paused("bale_to_tg"))
        self.api.delete_uncertain, self.api.delete_status = False, 403
        self.assertEqual((await self.moderator.handle("telegram", self.update(2)))["action"], "delete_failed")

    async def test_without_delete_rights_classifies_and_keeps(self):
        self.moderator.can_delete = False
        self.assertEqual((await self.moderator.handle("telegram", self.update()))["action"], "kept_no_delete_rights")
        self.assertEqual(len(self.api.jev_requests), 1)
        self.assertEqual(self.api.deletions, [])

    async def test_disabled_moderation_is_a_no_op(self):
        self.moderator.config = replace(self.conf, moderation_enabled=False)
        self.assertEqual(await self.moderator.prepare({}), {"enabled": False})
        self.assertIsNone(await self.moderator.handle("telegram", self.update()))
        self.assertIsNone(self.row())
        self.assertEqual(self.api.jev_requests, [])

    async def test_review_alert_failure_preserves_decision_and_is_not_replayed(self):
        self.moderator.config = replace(self.conf, moderation_alert_chat_id=1234)
        self.api.scores["should_delete"] = 0.5
        with patch.object(self.telegram, "send_message", side_effect=ApiError("telegram", "sendMessage", "Denied", code=403)) as send:
            self.assertEqual((await self.moderator.handle("telegram", self.update()))["action"], "review")
            await self.moderator.handle("telegram", self.update())
        self.assertEqual(send.call_count, 1)
        self.assertEqual(self.row()["action"], "review")
        self.assertEqual(self.row()["alert_status"], "failed")

    async def test_decision_text_errors_and_alerts_redact_known_secrets(self):
        self.moderator.config = replace(self.conf, moderation_alert_chat_id=1234)
        self.api.scores["should_delete"] = 0.5
        await self.moderator.handle("telegram", self.update(text=f"comment {TYPESAFE_KEY} {TELEGRAM_TOKEN}"))
        stored = json.dumps(dict(self.row()))
        alert = json.dumps(self.api.alerts)
        for secret in (TYPESAFE_KEY, TELEGRAM_TOKEN):
            self.assertNotIn(secret, stored)
            self.assertNotIn(secret, alert)

    async def test_other_chats_edits_and_bale_are_no_ops(self):
        for platform, update in [
            ("telegram", self.update(chat={"id": -999, "type": "supergroup"})),
            ("telegram", self.update(chat={"id": self.conf.telegram_channel_id, "type": "channel"})),
            ("telegram", {"update_id": 1, "edited_message": self.update()["message"]}),
            ("bale", self.update()),
        ]:
            self.assertIsNone(await self.moderator.handle(platform, update))
        self.assertIsNone(self.row())
        self.assertEqual(self.api.jev_requests, [])

    async def test_startup_discovers_group_and_warns_about_bale(self):
        self.moderator.config = replace(self.conf, telegram_discussion_group_id=None)
        with self.assertLogs("channel_sync.moderation", level="WARNING") as logs:
            report = await self.moderator.prepare({})
        self.assertTrue(report["telegram"]["ok"])
        self.assertTrue(report["telegram"]["auto_detected"])
        self.assertEqual(self.moderator.group_id, GROUP_ID)
        self.assertFalse(report["bale"]["supported"])
        self.assertIn("Bale comment moderation is disabled", " ".join(logs.output))

    async def test_interrupted_claims_are_audited_and_not_replayed(self):
        self.store.claim_moderation("telegram", GROUP_ID, 1, 999, "comment")
        self.store.claim_moderation("telegram", GROUP_ID, 2, 999, "comment")
        self.store.finish_moderation("telegram", GROUP_ID, 2, action="deleting", scores=self.api.scores)
        self.store.recover_moderation()
        rows = self.store.connection.execute("SELECT action FROM moderation_decisions ORDER BY message_id").fetchall()
        self.assertEqual([r[0] for r in rows], ["kept_interrupted", "delete_uncertain"])
        await self.moderator.handle("telegram", self.update(1))
        await self.moderator.handle("telegram", self.update(2))
        self.assertEqual(self.api.jev_requests, [])

    async def test_existing_pollers_moderate_and_forward_only_channel_posts(self):
        conf = replace(self.conf, telegram_to_bale_enabled=True, bale_to_telegram_enabled=True)
        clients = {"telegram": self.telegram, "bale": self.bale}
        bridge = Bridge(conf, clients, self.store)
        bridge.moderator = self.moderator
        self.api.queues["telegram"] = [self.update(), {"update_id": 2, "channel_post": {
            "message_id": 7, "chat": {"id": conf.telegram_channel_id, "type": "channel"},
            "text": "A channel post", "from": {"id": 999}}}]
        self.api.queues["bale"] = [{"update_id": 3, "message": {
            "message_id": 8, "chat": {"id": conf.bale_channel_id, "type": "channel"},
            "text": "A Bale post", "from": {"id": 999}}}]
        task = asyncio.create_task(bridge.run())
        try:
            async def done():
                while self.store.snapshot()["job_counts"] != {"sent": 2}:
                    if task.done():
                        await task
                        self.fail("Bridge unexpectedly stopped")
                    await asyncio.sleep(0.01)
            await asyncio.wait_for(done(), timeout=3)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(len(self.api.jev_requests), 1)
        self.assertEqual(len(self.api.deletions), 1)
        self.assertTrue(all(v == ["channel_post", "message"] for v in self.api.allowed_updates))
        self.assertEqual(len(self.store.snapshot()["jobs"]), 2)
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM bridge_items WHERE chat_id=?", (GROUP_ID,)).fetchone()[0], 0)

    async def test_durable_comment_backlog_is_moderated_before_routing(self):
        bridge = Bridge(self.conf, {"telegram": self.telegram, "bale": self.bale}, self.store)
        bridge.moderator = self.moderator
        self.store.ingest("telegram", SCOPE, [self.update()], encoder=bridge.codec.encode)
        await bridge.moderate_inbox("telegram")
        bridge.process_inbox("telegram", baseline=True)
        self.assertEqual(self.store.snapshot()["jobs"], [])
        self.assertEqual(self.row()["action"], "deleted")
        self.assertEqual(self.store.pending_updates("telegram"), [])

    async def test_inspection_poller_moderates_without_mixing_alerts_with_probe_records(self):
        conf = replace(self.conf, moderation_alert_chat_id=1234)
        self.api.scores["should_delete"] = 0.5
        self.api.queues["telegram"] = [self.update()]
        with patch("channel_sync.moderation.JevClient", return_value=self.jev):
            report = await inspect_echo(conf, {"telegram": self.telegram, "bale": self.bale},
                                        self.store, self.root / "inspect.json", observation_seconds=0.01)
        self.assertEqual(report["status"], "complete")
        self.assertEqual(self.row()["action"], "review")
        self.assertEqual(self.row()["alert_status"], "sent")
        self.assertEqual(len(self.api.alerts), 1)
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM send_attempts").fetchone()[0], 8)
