from contextlib import redirect_stdout
from dataclasses import replace
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import httpx

from channel_sync.cli import main
from channel_sync.clients import ApiError, BaleClient, TelegramClient
from channel_sync.config import ConfigError, load_config
from channel_sync.health import health
from channel_sync.jev import JevClient, load_questions
from channel_sync.redaction import RedactingFormatter, Redactor
from helpers import config, TELEGRAM_TOKEN, BALE_TOKEN
from test_moderation import ModerationAPIs, TYPESAFE_KEY, GROUP_ID


class JevHealthTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.conf = config(self.root, moderation_enabled=True, typesafe_api_key=TYPESAFE_KEY)
        self.api = ModerationAPIs(self.conf)
        transport = httpx.MockTransport(self.api.handle)
        self.delays = []

        async def sleep(delay):
            self.delays.append(delay)
        self.jev = JevClient(self.conf, transport=transport, sleeper=sleep)
        self.clients = {"telegram": TelegramClient(TELEGRAM_TOKEN, self.conf, transport=transport),
                        "bale": BaleClient(BALE_TOKEN, self.conf, transport=transport)}
        for client in (*self.clients.values(), self.jev):
            self.addAsyncCleanup(client.aclose)

    async def report(self):
        return await health(self.clients, {"telegram": self.conf.telegram_channel_id,
                                          "bale": self.conf.bale_channel_id}, jev_client=self.jev)

    async def test_jev_uses_telegram_proxy_timeout_and_ignores_ambient_proxies(self):
        conf = replace(self.conf, telegram_proxy_url="socks5h://user:password@127.0.0.1:10808")
        with patch.dict(os.environ, {"HTTPS_PROXY": "http://wrong:80"}), \
                patch("channel_sync.clients.httpx.AsyncClient") as constructor:
            JevClient(conf)
        options = constructor.call_args.kwargs
        self.assertEqual(options["proxy"], conf.telegram_proxy_url)
        self.assertEqual(options["timeout"].read, conf.request_timeout)
        self.assertFalse(options["trust_env"])
        self.assertFalse(options["follow_redirects"])

    async def test_unauthorized_is_permanent_and_does_not_leak_remote_body(self):
        self.api.jev_status = 401
        self.api.override_payload = {"error": TYPESAFE_KEY}
        with self.assertRaises(ApiError) as error:
            await self.jev.classify("Comment")
        self.assertEqual(error.exception.code, 401)
        self.assertEqual(len(self.api.jev_requests), 1)
        self.assertEqual(self.delays, [])
        self.assertNotIn(TYPESAFE_KEY, json.dumps(error.exception.as_dict()))

    async def test_transient_http_errors_retry_and_succeed(self):
        for status in (429, 503):
            requests = []
            async def handler(request):
                requests.append(request)
                if len(requests) < 3:
                    return httpx.Response(status, headers={"Retry-After": "0.01"})
                return await self.api.handle(request)
            evaluator = JevClient(self.conf, transport=httpx.MockTransport(handler), sleeper=self.jev._sleep)
            try:
                scores = await evaluator.classify("Comment")
            finally:
                await evaluator.aclose()
            self.assertEqual(scores["should_delete"], 0.55)
            self.assertEqual(len(requests), 3)

    async def test_malformed_missing_and_invalid_scores_are_rejected_after_retries(self):
        invalid_values = [None, True, "0.9", -0.1, 1.1, float("nan"), float("inf")]
        for value in invalid_values:
            with self.subTest(value=value):
                self.api.override_payload = {"answers": {
                    key: {"noul": value if key == "should_delete" else 0.1} for key in self.api.scores}}
                # httpx's JSON encoder rejects nonfinite values; use raw response JSON instead.
                def handler(request):
                    return httpx.Response(200, content=json.dumps(self.api.override_payload).encode())
                evaluator = JevClient(self.conf, transport=httpx.MockTransport(handler), sleeper=self.jev._sleep)
                try:
                    with self.assertRaises(ApiError):
                        await evaluator.classify("Comment")
                finally:
                    await evaluator.aclose()
        for payload in ({}, [], {"answers": {}}, {"answers": {"should_delete": {"noul": 0.9}}}):
            self.api.override_payload = payload
            with self.assertRaises(ApiError):
                await self.jev.classify("Comment")

    async def test_health_checks_key_reachability_and_discussion_delete_rights(self):
        report = await self.report()
        self.assertTrue(report["ok"])
        self.assertEqual(report["moderation"]["telegram"]["chat_id"], GROUP_ID)
        self.assertTrue(report["moderation"]["telegram"]["can_delete_messages"])
        self.assertTrue(report["moderation"]["jev"]["reachable"])
        self.assertTrue(report["moderation"]["jev"]["api_key_valid"])
        self.assertFalse(report["moderation"]["bale"]["supported"])
        self.assertEqual(len(self.api.jev_requests), 1)
        self.assertEqual(self.api.deletions, [])
        self.assertFalse(any(method == "getUpdates" or method.startswith("send") for _, method in self.api.events))

    async def test_health_rejects_invalid_key(self):
        self.api.jev_status = 401
        report = await self.report()
        self.assertFalse(report["ok"])
        self.assertTrue(report["moderation"]["jev"]["reachable"])
        self.assertFalse(report["moderation"]["jev"]["api_key_valid"])

    async def test_health_rejects_missing_delete_rights_and_non_admin(self):
        for status, right in (("member", True), ("administrator", False), ("administrator", None)):
            self.api.group_status, self.api.can_delete = status, right
            report = await self.report()
            self.assertFalse(report["ok"])
            self.assertIn("Delete messages", report["moderation"]["telegram"]["error"])

    async def test_health_accepts_owner_and_rejects_missing_or_non_group_chat(self):
        self.api.group_status, self.api.can_delete = "creator", None
        self.assertTrue((await self.report())["ok"])
        self.api.group_type = "channel"
        self.assertFalse((await self.report())["ok"])
        self.api.linked_group = None
        self.assertFalse((await self.report())["ok"])

    async def test_health_with_missing_key_is_actionable_and_no_jev_request(self):
        self.jev.config = replace(self.conf, typesafe_api_key="")
        report = await self.report()
        self.assertFalse(report["ok"])
        self.assertIn("TYPESAFE_API_KEY", report["moderation"]["jev"]["error"])
        self.assertEqual(self.api.jev_requests, [])


class ModerationConfigTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / ".env"

    def write(self, **values):
        values = {"TELEGRAM_BOT_TOKEN": TELEGRAM_TOKEN, "BALE_BOT_TOKEN": BALE_TOKEN, **values}
        template = (Path(__file__).parents[1] / ".env.example").read_text()
        self.path.write_text("\n".join(
            f"{line.split('=', 1)[0]}={values[line.split('=', 1)[0]]}"
            if "=" in line and line.split("=", 1)[0] in values else line
            for line in template.splitlines()
        ))

    def test_moderation_defaults_and_private_api_key(self):
        self.write(TYPESAFE_API_KEY=TYPESAFE_KEY)
        conf = load_config(self.path)
        self.assertTrue(conf.moderation_enabled)
        self.assertTrue(conf.moderation_skip_admins)
        self.assertEqual(conf.moderation_threshold, 0.55)
        self.assertEqual(conf.moderation_review_low, 0.45)
        self.assertEqual(conf.jev_model, "jev-latest")
        self.assertIsNone(conf.telegram_discussion_group_id)
        self.assertIsNone(conf.bale_discussion_group_id)
        self.assertIsNone(conf.moderation_alert_chat_id)
        self.assertNotIn(TYPESAFE_KEY, repr(conf))

    def test_typesafe_key_is_only_loaded_from_env_file_without_interpolation(self):
        self.write()
        with patch.dict(os.environ, {"TYPESAFE_API_KEY": TYPESAFE_KEY}):
            self.assertEqual(load_config(self.path).typesafe_api_key, "")
        self.write(TYPESAFE_API_KEY="${TYPESAFE_API_KEY}")
        with patch.dict(os.environ, {"TYPESAFE_API_KEY": TYPESAFE_KEY}):
            self.assertEqual(load_config(self.path).typesafe_api_key, "${TYPESAFE_API_KEY}")

    def test_probability_bounds_and_optional_ids(self):
        for key, value in (("MODERATION_THRESHOLD", "nan"), ("MODERATION_THRESHOLD", "1.1"),
                           ("MODERATION_REVIEW_LOW", "-0.1"), ("MODERATION_REVIEW_LOW", "0.9"),
                           ("MODERATION_ENABLED", "yes"), ("MODERATION_SKIP_ADMINS", "yes"),
                           ("TELEGRAM_DISCUSSION_GROUP_ID", "0"), ("TELEGRAM_DISCUSSION_GROUP_ID", "42"),
                           ("TELEGRAM_DISCUSSION_GROUP_ID", "-1004472119156"),
                           ("BALE_DISCUSSION_GROUP_ID", "invalid"), ("MODERATION_ALERT_CHAT_ID", "0")):
            with self.subTest(key=key, value=value):
                self.write(**{key: value})
                with self.assertRaises(ConfigError):
                    load_config(self.path)
        self.write(MODERATION_THRESHOLD="0", MODERATION_REVIEW_LOW="0",
                   TELEGRAM_DISCUSSION_GROUP_ID=str(GROUP_ID), MODERATION_ALERT_CHAT_ID="1234")
        conf = load_config(self.path)
        self.assertEqual(conf.moderation_threshold, 0)
        self.assertEqual(conf.telegram_discussion_group_id, GROUP_ID)
        self.assertEqual(conf.moderation_alert_chat_id, 1234)

    def test_moderate_test_prints_all_scores_and_constructs_no_bot_clients(self):
        self.write(TYPESAFE_API_KEY=TYPESAFE_KEY)
        with patch("channel_sync.cli.JevClient") as evaluator, \
                patch("channel_sync.cli.TelegramClient") as telegram, patch("channel_sync.cli.BaleClient") as bale:
            from unittest.mock import AsyncMock
            evaluator.return_value.classify = AsyncMock(return_value={key: 0.3 for key in load_questions()})
            evaluator.return_value.aclose = AsyncMock()
            with redirect_stdout(io.StringIO()) as output:
                code = main(["--env", str(self.path), "moderate-test", "A comment"])
        self.assertEqual(code, 0)
        self.assertEqual(set(json.loads(output.getvalue())), set(load_questions()))
        telegram.assert_not_called()
        bale.assert_not_called()
        evaluator.return_value.classify.assert_awaited_once_with("A comment")
        evaluator.return_value.aclose.assert_awaited_once()

    def test_api_key_is_redacted_in_logs_and_nested_reports(self):
        import logging
        redactor = Redactor(TYPESAFE_KEY)
        record = logging.LogRecord("test", logging.ERROR, "test", 1, "Authorization: Bearer %s", (TYPESAFE_KEY,), None)
        self.assertNotIn(TYPESAFE_KEY, RedactingFormatter(redactor).format(record))
        self.assertNotIn(TYPESAFE_KEY, json.dumps(redactor.data({"error": [TYPESAFE_KEY]})))
